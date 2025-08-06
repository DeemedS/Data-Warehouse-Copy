using Microsoft.Data.SqlClient;
using Serilog;
using Microsoft.Extensions.Configuration;

class Program
{
    static void Main()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        var sourceConnStr = config.GetConnectionString("Source");
        var targetConnStr = config.GetConnectionString("Target");

        var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var logFilePath = $"bulk_copy_log_{timestamp}.txt";

        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.File(logFilePath)
            .CreateLogger();

        SqlTransaction? transaction = null;

        try
        {
            using var sourceConn = new SqlConnection(sourceConnStr);
            using var targetConn = new SqlConnection(targetConnStr);

            sourceConn.Open();
            targetConn.Open();
            Log.Information("Connections opened successfully.");

            transaction = targetConn.BeginTransaction();

            // Delete from target table
            using (var deleteCmd = new SqlCommand("DELETE FROM tbl_transaction_master WHERE CAST(transaction_date AS DATE) BETWEEN '2025-07-07' AND '2025-07-13'", targetConn, transaction))
            {
                deleteCmd.CommandTimeout = 3000;
                int deleted = deleteCmd.ExecuteNonQuery();
                Log.Information("Deleted {Count} rows from target table.", deleted);
            }

            string query = @"SELECT *
                             FROM tbl_transaction_master 
                             WHERE CAST(transaction_date AS DATE) BETWEEN '2025-07-07' AND '2025-07-13'";

            using var cmd = new SqlCommand(query, sourceConn);
            using var reader = cmd.ExecuteReader();

            using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.Default, transaction)
            {
                // DestinationTableName = "tbl_transaction_master",
                DestinationTableName = "tbl_transaction_master",
                BulkCopyTimeout = 0,
                EnableStreaming = true,
                NotifyAfter = 100000
            };

            int totalRowsInserted = 0;
            bulkCopy.SqlRowsCopied += (sender, e) =>
            {
                totalRowsInserted = (int)e.RowsCopied;
                Log.Information("Processed {RowCount} rows.", totalRowsInserted);
            };

            bulkCopy.WriteToServer(reader);
            Log.Information("Bulk copy completed.");

            try
            {
                transaction.Commit();
                Log.Information("Transaction committed.");
                Log.Information("Total rows inserted: {RowCount} +", totalRowsInserted);
            }
            catch (Exception commitEx)
            {
                Log.Error(commitEx, "Commit failed.");
                try
                {
                    transaction.Rollback();
                    Log.Warning("Transaction rolled back after commit failure.");
                }
                catch (Exception rollbackEx)
                {
                    Log.Error(rollbackEx, "Rollback failed.");
                }
            }
        }
        catch (Exception ex)
        {
            try
            {
                transaction?.Rollback();
                Log.Warning("Transaction rolled back due to error.");
            }
            catch (Exception rollbackEx)
            {
                Log.Error(rollbackEx, "Rollback failed.");
            }

            Log.Error(ex, "Unexpected error occurred.");
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}
