using Microsoft.Data.SqlClient;
using Serilog;

public static class TableUpdateService
{
    public static void UpdateTable(
        string tableName,
        string sourceConnStr,
        string targetConnStr,
        string targetSchema,
        string sourceSchema,
        string startDate,
        string endDate,
        string? dateColumn, 
        string? updateDateColumn,
        string primaryKey
    )
    {
        var startTime = DateTime.Now;
        long recordsCopied = 0;
        int logId = -1;
        string status = "In Progress";
        string? errorMessage = null;

        var exePath = System.Diagnostics.Process.GetCurrentProcess().MainModule?.FileName;
        var basedir = Path.GetDirectoryName(exePath)!;

        var tableTimestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var monthFolder = DateTime.Now.ToString("yyyy-MM");
        var logDir = Path.Combine(basedir, "logs", monthFolder, tableName);
        Directory.CreateDirectory(logDir);

        var tableLogPath = Path.Combine(logDir, $"{tableName}-{tableTimestamp}.log");

        var tableLogger = new LoggerConfiguration()
            .MinimumLevel.Information()
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
                INSERT INTO bronze.tbl_dw_copy_logs (TableName, StartTime, EndTime, StartDateParam, EndDateParam, DateColumn, UpdateDateColumn, PrimaryKey, Process, Status)
                OUTPUT INSERTED.Id
                VALUES (@TableName, @StartTime, @EndTime, @StartDateParam, @EndDateParam, @DateColumn, @UpdateDateColumn, @PrimaryKey, @Process, @Status)", targetConn))
            {
                logCmd.Parameters.AddWithValue("@TableName", tableName);
                logCmd.Parameters.AddWithValue("@StartTime", startTime);
                logCmd.Parameters.AddWithValue("@EndTime", DBNull.Value);
                logCmd.Parameters.AddWithValue("@StartDateParam", startDate);
                logCmd.Parameters.AddWithValue("@EndDateParam", endDate);
                logCmd.Parameters.AddWithValue("@DateColumn", dateColumn);
                logCmd.Parameters.AddWithValue("@UpdateDateColumn", updateDateColumn);
                logCmd.Parameters.AddWithValue("@PrimaryKey", primaryKey);
                logCmd.Parameters.AddWithValue("@Process", "Table Update");
                logCmd.Parameters.AddWithValue("@Status", status);
                logId = (int)logCmd.ExecuteScalar();
            }

            transaction = targetConn.BeginTransaction();

            var pkValuesToUpdate = new List<object>();
            if (!string.IsNullOrEmpty(updateDateColumn))
            {
                tableLogger.Information("Identifying primary keys to update based on {UpdateDateColumn}", updateDateColumn);

                string pkSql = $@"
                    SELECT {primaryKey}
                    FROM {sourceSchema}.{tableName}
                    WHERE CAST({updateDateColumn} AS DATE) BETWEEN @StartDate AND @EndDate
                    AND {primaryKey} NOT IN (
                    SELECT {primaryKey} FROM {sourceSchema}.{tableName}
                    WHERE CAST({dateColumn} AS DATE) BETWEEN @StartDate AND @EndDate
                    )";

                using var pkCmd = new SqlCommand(pkSql, sourceConn);

                pkCmd.CommandTimeout = 0;

                pkCmd.Parameters.AddWithValue("@StartDate",
                    !string.IsNullOrEmpty(startDate) ? DateTime.Parse(startDate).Date : throw new ArgumentNullException(nameof(startDate))); // beginning of day
                pkCmd.Parameters.AddWithValue("@EndDate",
                    !string.IsNullOrEmpty(endDate) ? DateTime.Parse(endDate).Date.AddDays(1).AddMilliseconds(-3) : throw new ArgumentNullException(nameof(endDate))); // end of day 23:59:59.997

                using var reader = pkCmd.ExecuteReader();
                while (reader.Read())
                {
                    pkValuesToUpdate.Add(reader[0]);
                }
                
                tableLogger.Information("Total primary keys to update: {Count}", pkValuesToUpdate.Count);
            }

            if (pkValuesToUpdate.Count > 0)
            {
                int batchSize = 2000;
                for (int i = 0; i < pkValuesToUpdate.Count; i += batchSize)
                {
                    var batch = pkValuesToUpdate.Skip(i).Take(batchSize).ToList();
                    string inClause = string.Join(", ", batch.Select((_, index) => $"@pk{index}"));

                    // Delete old rows
                    string deleteSql = $@"
                        DELETE FROM {targetSchema}.{tableName}
                        WHERE {primaryKey} IN ({inClause})";

                    using (var deleteCmd = new SqlCommand(deleteSql, targetConn, transaction))
                    {
                        for (int j = 0; j < batch.Count; j++)
                            deleteCmd.Parameters.AddWithValue($"@pk{j}", batch[j]);

                        int deleted = deleteCmd.ExecuteNonQuery();
                        tableLogger.Information("Deleted {Count} rows in {Table} (batch {Batch})", deleted, tableName, i / batchSize + 1);
                    }

                    // Insert fresh rows
                    string selectSql = $@"
                        SELECT *
                        FROM {sourceSchema}.{tableName}
                        WHERE {primaryKey} IN ({inClause})";

                    using var selectCmd = new SqlCommand(selectSql, sourceConn);
                    for (int j = 0; j < batch.Count; j++)
                        selectCmd.Parameters.AddWithValue($"@pk{j}", batch[j]);

                    using var srcReader = selectCmd.ExecuteReader();
                    using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.Default, transaction)
                    {
                        DestinationTableName = $"{targetSchema}.{tableName}",
                        BulkCopyTimeout = 0,
                        EnableStreaming = true,
                        NotifyAfter = 10000
                    };

                    for (int col = 0; col < srcReader.FieldCount; col++)
                    {
                        string colName = srcReader.GetName(col);
                        if (!colName.Equals("Id", StringComparison.OrdinalIgnoreCase) &&
                            !colName.Equals("insert_datetime", StringComparison.OrdinalIgnoreCase))
                        {
                            bulkCopy.ColumnMappings.Add(colName, colName);
                        }
                    }

                    bulkCopy.WriteToServer(srcReader);
                    tableLogger.Information("Inserted batch of {Count} rows into {Table}", batch.Count, tableName);
                }

                recordsCopied = pkValuesToUpdate.Count;
                transaction.Commit();
                status = "Completed";
            }
            else
            {
                tableLogger.Information("No rows to update for {Table}", tableName);
                transaction.Commit();
                status = "Completed";
            }
        }
        catch (Exception ex)
        {
            status = "Failed";
            errorMessage = ex.Message;

            try
            {
                if (transaction?.Connection != null)
                {
                    transaction.Rollback();
                    tableLogger.Warning("Rolled back transaction for {Table}", tableName);
                }
            }
            catch (Exception rollbackEx)
            {
                tableLogger.Error(rollbackEx, "Rollback failed for {Table}", tableName);
            }

            tableLogger.Error(ex, "Error processing {Table}", tableName);
        }
        finally
        {
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
            updateCmd.Parameters.AddWithValue("@RecordsCopied", recordsCopied);
            updateCmd.Parameters.AddWithValue("@ErrorMessage", (object?)errorMessage ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("@Id", logId);
            updateCmd.ExecuteNonQuery();

            tableLogger.Information("Finished processing {Table}", tableName);
        }
    }
}
