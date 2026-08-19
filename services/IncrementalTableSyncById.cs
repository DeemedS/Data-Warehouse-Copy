using Microsoft.Data.SqlClient;
using Serilog;
using System.Data;

public static class IncrementalTableSyncById
{
	public static void SyncIncrementalTableById(
		string tableName,
		string sourceConnStr,
		string targetConnStr,
		string targetSchema,
		string sourceSchema,
		string idColumn
	)
	{
		var startTime = DateTime.Now;
		int logId = -1;
		long recordsInserted = 0;
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

		tableLogger.Information("Starting incremental (by id) copy for {Table}", tableName);

		SqlTransaction? transaction = null;

		try
		{
			using var sourceConn = new SqlConnection(sourceConnStr);
			using var targetConn = new SqlConnection(targetConnStr);
			sourceConn.Open();
			targetConn.Open();

			// Insert initial log row
			using (var logCmd = new SqlCommand(@"
				INSERT INTO bronze.tbl_dw_copy_logs 
					(TableName, StartTime, EndTime, StartDateParam, EndDateParam, DateColumn, UpdateDateColumn, PrimaryKey, Process, Status, ErrorMessage)
				OUTPUT INSERTED.Id
				VALUES 
					(@TableName, @StartTime, @EndTime, @StartDateParam, @EndDateParam, @DateColumn, @UpdateDateColumn, @PrimaryKey, @Process, @Status, @ErrorMessage)", targetConn))
			{
				logCmd.CommandTimeout = 0;
				logCmd.Parameters.AddWithValue("@TableName", tableName);
				logCmd.Parameters.AddWithValue("@StartTime", startTime);
				logCmd.Parameters.AddWithValue("@EndTime", DBNull.Value);
				logCmd.Parameters.AddWithValue("@StartDateParam", DBNull.Value);
				logCmd.Parameters.AddWithValue("@EndDateParam", DBNull.Value);
				logCmd.Parameters.AddWithValue("@DateColumn", DBNull.Value);
				logCmd.Parameters.AddWithValue("@UpdateDateColumn", DBNull.Value);
				logCmd.Parameters.AddWithValue("@PrimaryKey", idColumn);
				logCmd.Parameters.AddWithValue("@Process", "Incremental - ById");
				logCmd.Parameters.AddWithValue("@Status", status);
				logCmd.Parameters.AddWithValue("@ErrorMessage", DBNull.Value);

				logId = (int)logCmd.ExecuteScalar();
			}

			// --- Step 1: Find max id in target ---
			long maxId = 0;
			using (var maxCmd = new SqlCommand($"SELECT ISNULL(MAX([{idColumn}]), 0) FROM [{targetSchema}].[{tableName}]", targetConn))
			{
				maxCmd.CommandTimeout = 0;
				var result = maxCmd.ExecuteScalar();
				if (result != null && result != DBNull.Value)
				{
					maxId = Convert.ToInt64(result);
				}
			}

			tableLogger.Information("Current max {IdColumn} in target {Table} = {MaxId}", idColumn, tableName, maxId);

			// --- Step 2: Load new rows from source where id > maxId ---
			var srcCmd = new SqlCommand($@"
				SELECT *
				FROM [{sourceSchema}].[{tableName}]
				WHERE [{idColumn}] > @MaxId
				ORDER BY [{idColumn}] ASC
			", sourceConn);

			srcCmd.Parameters.AddWithValue("@MaxId", maxId);
			srcCmd.CommandTimeout = 0;
			var srcAdapter = new SqlDataAdapter(srcCmd);
			var srcTable = new DataTable();
			srcAdapter.Fill(srcTable);
			tableLogger.Information("Loaded {Count} new rows from source {Table}", srcTable.Rows.Count, tableName);

			if (srcTable.Rows.Count > 0)
			{
				transaction = targetConn.BeginTransaction();

				using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.KeepIdentity, transaction);
				bulkCopy.DestinationTableName = $"[{targetSchema}].[{tableName}]";
				bulkCopy.BatchSize = 1000;
				bulkCopy.BulkCopyTimeout = 0;

				foreach (DataColumn col in srcTable.Columns)
				{
					bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
				}

				bulkCopy.WriteToServer(srcTable);
				recordsInserted += srcTable.Rows.Count;

				transaction.Commit();
				tableLogger.Information("Inserted {Count} incremental rows into target {Table}", srcTable.Rows.Count, tableName);
			}

			status = "Completed";
		}
		catch (Exception ex)
		{
			status = "Failed";
			errorMessage = ex.Message;
			tableLogger.Error(ex, "Error in incremental copy by id for {Table}", tableName);
			try { transaction?.Rollback(); } catch { }
		}
		finally
		{
			try
			{
				using var targetConn = new SqlConnection(targetConnStr);
				targetConn.Open();

				using var updateCmd = new SqlCommand(@"
					UPDATE bronze.tbl_dw_copy_logs
					SET EndTime = @EndTime,
						Status = @Status,
						RecordsCopied = @RecordsInserted,
						ErrorMessage = @ErrorMessage
					WHERE Id = @Id", targetConn);

				updateCmd.Parameters.AddWithValue("@EndTime", DateTime.Now);
				updateCmd.Parameters.AddWithValue("@Status", status);
				updateCmd.Parameters.AddWithValue("@RecordsInserted", recordsInserted);
				updateCmd.Parameters.AddWithValue("@ErrorMessage", (object?)errorMessage ?? DBNull.Value);
				updateCmd.Parameters.AddWithValue("@Id", logId);
				updateCmd.ExecuteNonQuery();

				tableLogger.Information("Log table updated for {Table} with status {Status}", tableName, status);
			}
			catch (Exception logEx)
			{
				tableLogger.Error(logEx, "Failed to update process log for {Table}", tableName);
			}
		}
	}
}

