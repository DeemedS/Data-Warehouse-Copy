using Microsoft.Data.SqlClient;
using Serilog;

public static class TableCopyService
{
    public static void CopyTable(string tableName, string sourceConnStr, string targetConnStr, string start_date, string end_date, bool useTruncate, string? dateColumn)
    {
        Log.Information("Starting: {Table}", tableName);
        SqlTransaction? transaction = null;

        try
        {
            using var sourceConn = new SqlConnection(sourceConnStr);
            using var targetConn = new SqlConnection(targetConnStr);

            sourceConn.Open();
            targetConn.Open();
            transaction = targetConn.BeginTransaction();

            // Delete or truncate
            if (useTruncate)
            {
                string truncateSql = $"TRUNCATE TABLE {tableName}";
                using var truncateCmd = new SqlCommand(truncateSql, targetConn, transaction);
                truncateCmd.ExecuteNonQuery();
                Log.Information("Truncated table: {Table}", tableName);
            }
            else if (!string.IsNullOrEmpty(dateColumn))
            {
                string deleteSql = $"DELETE FROM {tableName} WHERE CAST({dateColumn} AS DATE) BETWEEN @start_date AND @end_date";
                using var deleteCmd = new SqlCommand(deleteSql, targetConn, transaction);
                deleteCmd.Parameters.AddWithValue("@start_date", start_date);
                deleteCmd.Parameters.AddWithValue("@end_date", end_date);
                deleteCmd.CommandTimeout = 3000;
                int deleted = deleteCmd.ExecuteNonQuery();
                Log.Information("Deleted {Count} rows from {Table}", deleted, tableName);
            }
            else
            {
                Log.Warning("Skipped delete/truncate for {Table} — no date column.", tableName);
            }

            // Select from source
            string selectSql = useTruncate || string.IsNullOrEmpty(dateColumn)
                ? $"SELECT * FROM {tableName}"
                : $"SELECT * FROM {tableName} WHERE CAST({dateColumn} AS DATE) BETWEEN @start_date AND @end_date";

            using var selectCmd = new SqlCommand(selectSql, sourceConn);
            if (!useTruncate && !string.IsNullOrEmpty(dateColumn))
            {
                selectCmd.Parameters.AddWithValue("@start_date", start_date);
                selectCmd.Parameters.AddWithValue("@end_date", end_date);
            }

            using var reader = selectCmd.ExecuteReader();

            using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = tableName,
                BulkCopyTimeout = 0,
                EnableStreaming = true,
                NotifyAfter = 1000
            };

            int totalRows = 0;
            bulkCopy.SqlRowsCopied += (sender, e) =>
            {
                totalRows = (int)e.RowsCopied;
                Log.Information("Copied {Rows} rows for {Table}", totalRows, tableName);
            };

            bulkCopy.WriteToServer(reader);
            transaction.Commit();
            Log.Information("Done with {Table}. Rows inserted: {Rows}", tableName, totalRows);
        }
        catch (Exception ex)
        {
            try
            {
                transaction?.Rollback();
                Log.Warning("Rolled back transaction for {Table}", tableName);
            }
            catch (Exception rollbackEx)
            {
                Log.Error(rollbackEx, "Rollback failed for {Table}", tableName);
            }

            Log.Error(ex, "Error processing {Table}", tableName);
        }
    }
}
