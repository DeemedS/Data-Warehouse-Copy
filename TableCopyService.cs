using Microsoft.Data.SqlClient;
using Serilog;

public static class TableCopyService
{
    public static void CopyTable(
        string tableName,
        string sourceConnStr,
        string targetConnStr,
        string targetSchema,
        string sourceSchema,
        string start_date, string
        end_date, bool useTruncate,
        string? dateColumn)
    {   
        var startTime = DateTime.Now;
        long recordsCopied = 0;
        int logId = -1;
        string status = "In Progress";
        string? errorMessage = null;

        var log = Log.ForContext("Table", tableName);
        var exePath = System.Diagnostics.Process.GetCurrentProcess().MainModule?.FileName;
        var basedir = Path.GetDirectoryName(exePath)!;

        var tableTimestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var monthFolder = DateTime.Now.ToString("yyyy-MM");
        var logDir = Path.Combine(basedir, "logs", monthFolder, tableName);
        Directory.CreateDirectory(logDir);

        var tableLogPath = Path.Combine(logDir, $"{tableName}-{tableTimestamp}.log");

        var tableLogger = new LoggerConfiguration()
        .MinimumLevel.Information()
        // .WriteTo.Console()
        .WriteTo.File(
            path: tableLogPath,
            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {Message:lj}{NewLine}{Exception}"
        )
        .CreateLogger()
        .ForContext("Table", tableName);


        tableLogger.Information("Starting: {Table}", tableName);
        SqlTransaction? transaction = null;

        try
        {
            using var sourceConn = new SqlConnection(sourceConnStr);
            using var targetConn = new SqlConnection(targetConnStr);

            sourceConn.Open();
            targetConn.Open();

            // Insert initial log row
            using (var logCmd = new SqlCommand(@"
                INSERT INTO bronze.tbl_dw_copy_logs (TableName, StartTime, EndTime, StartDateParam, EndDateParam, Status)
                OUTPUT INSERTED.Id
                VALUES (@TableName, @StartTime, @EndTime, @StartDateParam, @EndDateParam, @Status)", targetConn))
            {
                logCmd.Parameters.AddWithValue("@TableName", tableName);
                logCmd.Parameters.AddWithValue("@StartTime", startTime);
                logCmd.Parameters.AddWithValue("@EndTime", DBNull.Value);

                if (useTruncate)
                {
                    logCmd.Parameters.AddWithValue("@StartDateParam", DBNull.Value);
                    logCmd.Parameters.AddWithValue("@EndDateParam", DBNull.Value);
                }
                else
                {
                    logCmd.Parameters.AddWithValue("@StartDateParam", start_date);
                    logCmd.Parameters.AddWithValue("@EndDateParam", end_date);
                }
                
                logCmd.Parameters.AddWithValue("@Status", status);
                logId = (int)logCmd.ExecuteScalar();
            }

            transaction = targetConn.BeginTransaction();

            tableLogger.Information("Deleting or truncating table: {Table}", tableName);

            // Delete or truncate
            if (useTruncate)
            {
                tableLogger.Information("Truncating table: {Table}", tableName);
                string truncateSql = $"TRUNCATE TABLE {targetSchema}.{tableName}";
                using var truncateCmd = new SqlCommand(truncateSql, targetConn, transaction);
                truncateCmd.ExecuteNonQuery();
                tableLogger.Information("Truncated table: {Table}", tableName);
            }
            else if (!string.IsNullOrEmpty(dateColumn))
            {
                tableLogger.Information("Deleting rows from {Table} where {DateColumn} is between {StartDate} and {EndDate}", tableName, dateColumn, start_date, end_date);

                string deleteSql = $@"
                    DELETE FROM {targetSchema}.{tableName}
                    WHERE {dateColumn} BETWEEN @start_date AND @end_date";

                using var deleteCmd = new SqlCommand(deleteSql, targetConn, transaction);
                deleteCmd.Parameters.AddWithValue("@start_date", DateTime.Parse(start_date).Date); // beginning of day
                deleteCmd.Parameters.AddWithValue("@end_date", DateTime.Parse(end_date).Date.AddDays(1).AddMilliseconds(-3)); // end of day 23:59:59.997

                deleteCmd.CommandTimeout = 0;
                int deleted = deleteCmd.ExecuteNonQuery();

                tableLogger.Information("Deleted {Count} rows from {Table}", deleted, tableName);
            }
            else
            {
                tableLogger.Warning("Skipped delete/truncate for {Table} — no date column.", tableName);
            }

            // Select from source
            string selectSql = useTruncate || string.IsNullOrEmpty(dateColumn)
                ? $"SELECT * FROM {sourceSchema}.{tableName}"
                : $"SELECT * FROM {sourceSchema}.{tableName} WHERE CAST({dateColumn} AS DATE) BETWEEN @start_date AND @end_date";

            using var selectCmd = new SqlCommand(selectSql, sourceConn);
            if (!useTruncate && !string.IsNullOrEmpty(dateColumn))
            {
                selectCmd.Parameters.AddWithValue("@start_date", start_date);
                selectCmd.Parameters.AddWithValue("@end_date", end_date);
            }

            using var reader = selectCmd.ExecuteReader();

            tableLogger.Information("Starting bulk copy for {Table}", tableName);

            using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = $"{targetSchema}.{tableName}",
                BulkCopyTimeout = 0,
                EnableStreaming = true,
                NotifyAfter = 1
            };

            int totalRows = 0;
            bulkCopy.SqlRowsCopied += (sender, e) =>
            {
                totalRows = (int)e.RowsCopied;
            };

            bulkCopy.WriteToServer(reader);
            transaction.Commit();
            
            status = "Completed";
            recordsCopied = totalRows;

            tableLogger.Information("Done with {Table}. Rows inserted: {Rows}", tableName, totalRows);
            Log.Information("Done with {Table}. Rows inserted: {Rows}", tableName, totalRows);
        }
        catch (Exception ex)
        {   
            status = "Failed";
            errorMessage = ex.Message;

            try
            {
                if (transaction != null && transaction.Connection != null)
                {
                    transaction.Rollback();
                    tableLogger.Warning("Rolled back transaction for {Table}", tableName);
                    Log.Warning("Rolled back transaction for {Table}", tableName);
                }
                else
                {
                    tableLogger.Warning("No active transaction to roll back for {Table}", tableName);
                    Log.Warning("No active transaction to roll back for {Table}", tableName);
                }
            }
            catch (Exception rollbackEx)
            {
                tableLogger.Error(rollbackEx, "Rollback failed for {Table}", tableName);
                Log.Error(rollbackEx, "Rollback failed for {Table}", tableName);
            }

            tableLogger.Error(ex, "Error processing {Table}", tableName);
            Log.Error(ex, "Error processing {Table}", tableName);
        }
        finally
        {
            // Final update to log table
            using var finalConn = new SqlConnection(targetConnStr);
            finalConn.Open();
            using var updateCmd = new SqlCommand(@"
                UPDATE bronze.tbl_dw_copy_logs
                SET EndTime = @EndTime,
                    Status = @Status,
                    RecordsCopied = @RecordsCopied,
                    ErrorMessage = @ErrorMessage
                WHERE Id = @Id", finalConn);

            updateCmd.Parameters.AddWithValue("@EndTime", DateTime.Now);
            updateCmd.Parameters.AddWithValue("@Status", status);
            updateCmd.Parameters.AddWithValue("@RecordsCopied", (object)recordsCopied ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("@ErrorMessage", (object?)errorMessage ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("@Id", logId);
            updateCmd.ExecuteNonQuery();

            tableLogger.Information("Finished processing {Table}", tableName);
            Log.Information("Finished processing {Table}", tableName);
        }
    }
}
