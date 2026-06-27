using Microsoft.Data.SqlClient;
using Serilog;
using System.Data;

public static class TableCopyServiceWithVersioning_v2
{
    public static void CopyTableWithVersioningFullIncremental_v2(
        string tableName,
        string sourceConnStr,
        string targetConnStr,
        string targetSchema,
        string sourceSchema,
        string startDate,
        string endDate,
        string? dateColumn, 
        string? updateDateColumn,
        string primaryKeyColumn
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

        tableLogger.Information("Starting full incremental versioned copy for {Table}", tableName);

        SqlTransaction? transaction = null;

        try
        {
            using var sourceConn = new SqlConnection(sourceConnStr);
            using var targetConn = new SqlConnection(targetConnStr);
            sourceConn.Open();
            targetConn.Open();

            // --- Insert initial log row ---
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
                logCmd.Parameters.AddWithValue("@StartDateParam", startDate);
                logCmd.Parameters.AddWithValue("@EndDateParam", endDate);
                logCmd.Parameters.AddWithValue("@DateColumn", dateColumn);
                logCmd.Parameters.AddWithValue("@UpdateDateColumn", updateDateColumn);
                logCmd.Parameters.AddWithValue("@PrimaryKey", primaryKeyColumn);
                logCmd.Parameters.AddWithValue("@Process", "Fact - SCD Type 2");
                logCmd.Parameters.AddWithValue("@Status", status);
                logCmd.Parameters.AddWithValue("@ErrorMessage", DBNull.Value);

                logId = (int)logCmd.ExecuteScalar();
            }

            // --- Step 1: Load source data ---
            var srcCmd = new SqlCommand($@"
                SELECT *
                FROM {sourceSchema}.{tableName}
                WHERE {updateDateColumn} BETWEEN @StartDate AND @EndDate
            ", sourceConn);

            srcCmd.Parameters.AddWithValue("@StartDate",
                !string.IsNullOrEmpty(startDate) ? DateTime.Parse(startDate).Date : throw new ArgumentNullException(nameof(startDate))); // beginning of day
            srcCmd.Parameters.AddWithValue("@EndDate",
                !string.IsNullOrEmpty(endDate) ? DateTime.Parse(endDate).Date.AddDays(1).AddMilliseconds(-3) : throw new ArgumentNullException(nameof(endDate))); // end of day 23:59:59.997

            srcCmd.CommandTimeout = 0;
            var srcAdapter = new SqlDataAdapter(srcCmd);
            var srcTable = new DataTable();
            srcAdapter.Fill(srcTable);
            tableLogger.Information("Loaded {Count} rows from source {Table}", srcTable.Rows.Count, tableName);

            // --- Step 2: Load active target data (valid_end IS NULL) ---
            var tgtCmd = new SqlCommand($@"
                SELECT *
                FROM {targetSchema}.{tableName}
                WHERE {updateDateColumn} BETWEEN @StartDate AND @EndDate
            ", targetConn);

            tgtCmd.CommandTimeout = 0;
            tgtCmd.Parameters.AddWithValue("@PK", primaryKeyColumn);
            tgtCmd.Parameters.AddWithValue("@StartDate",
                !string.IsNullOrEmpty(startDate) ? DateTime.Parse(startDate).Date : throw new ArgumentNullException(nameof(startDate))); // beginning of day
            tgtCmd.Parameters.AddWithValue("@EndDate",
                !string.IsNullOrEmpty(endDate) ? DateTime.Parse(endDate).Date.AddDays(1).AddMilliseconds(-3) : throw new ArgumentNullException(nameof(endDate))); // end of day 23:59:59.997
            var tgtAdapter = new SqlDataAdapter(tgtCmd);
            var tgtTable = new DataTable();
            tgtAdapter.Fill(tgtTable);
            tableLogger.Information("Loaded {Count} active rows from target {Table}", tgtTable.Rows.Count, tableName);

            // Build target lookup by primary key
            var targetLookup = tgtTable.AsEnumerable()
            .Where(r => r[primaryKeyColumn] != DBNull.Value)
            .ToDictionary(
                r => r[primaryKeyColumn].ToString()!.Trim().ToLowerInvariant(),
                r => r
            );

            // --- Step 3: Detect Identity Column in target ---
            var identityColumns = new HashSet<string>();
            using (var schemaCmd = new SqlCommand(@"
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema 
                  AND TABLE_NAME = @table
                  AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1", targetConn))
            {
                schemaCmd.Parameters.AddWithValue("@schema", targetSchema);
                schemaCmd.Parameters.AddWithValue("@table", tableName);

                using var reader = schemaCmd.ExecuteReader();
                while (reader.Read())
                {
                    identityColumns.Add(reader.GetString(0));
                }
            }

            if (identityColumns.Count > 0)
                tableLogger.Information("Skipping identity columns: {Cols}", string.Join(", ", identityColumns));

            transaction = targetConn.BeginTransaction();

            var changedRows = tgtTable.Clone();
            var newRows = tgtTable.Clone();
            var newChangedRows = tgtTable.Clone();

            foreach (DataRow srcRow in srcTable.Rows)
            {
                string pk = srcRow[primaryKeyColumn]?.ToString()?.Trim().ToLowerInvariant() ?? "";
                bool isNewRecord = !targetLookup.ContainsKey(pk);
                bool updateDateChanged = false;
                DataRow? tgtRow = null;

                tableLogger.Information("Processing record PK={PK}: IsNew={IsNew}", pk, isNewRecord);

                if (!isNewRecord)
                {
                    tgtRow = targetLookup[pk];

                    DateTime srcUpdateDate = Convert.ToDateTime(srcRow[updateDateColumn ?? throw new ArgumentNullException(nameof(updateDateColumn))]);
                    DateTime tgtUpdateDate = Convert.ToDateTime(tgtRow[updateDateColumn ?? throw new ArgumentNullException(nameof(updateDateColumn))]);

                    tableLogger.Information("Comparing record PK={PK}: Source {SrcDate}, Target {TgtDate}", pk, srcUpdateDate, tgtUpdateDate);

                    updateDateChanged = srcUpdateDate != tgtUpdateDate;
                }

                if (updateDateChanged && tgtRow != null)
                {
                    changedRows.ImportRow(tgtRow);     // archive old version
                    newChangedRows.ImportRow(srcRow);  // insert new version later
                }

                if (isNewRecord)
                {
                    newRows.ImportRow(srcRow);
                }
    
            }

            // --- Step 4: Insert new rows ---
            if (newRows.Rows.Count > 0)
            {
                using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.KeepIdentity, transaction);
                bulkCopy.DestinationTableName = $"{targetSchema}.{tableName}";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 0;

                foreach (DataColumn col in newRows.Columns)
                {
                    if (!identityColumns.Contains(col.ColumnName))
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                }

                bulkCopy.WriteToServer(newRows);
                recordsInserted += newRows.Rows.Count;

                tableLogger.Information("Inserted {Count} new rows into target {Table}", newRows.Rows.Count, tableName);
            }

            // --- Step 5: Update changed rows ---
            if (changedRows.Rows.Count > 0)
            {
                //Archive old rows to _history table
                var historyTableName = $"{tableName}_history";
                
                using var bulkCopyHistory = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.KeepIdentity, transaction);
                bulkCopyHistory.DestinationTableName = $"{targetSchema}.{historyTableName}";
                bulkCopyHistory.BatchSize = 1000;
                bulkCopyHistory.BulkCopyTimeout = 0;
                
                foreach (DataColumn col in changedRows.Columns)
                {
                    bulkCopyHistory.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                bulkCopyHistory.WriteToServer(changedRows);
                recordsInserted += changedRows.Rows.Count;

                tableLogger.Information("Archived {Count} changed rows to history table {HistoryTable}", changedRows.Rows.Count, historyTableName);

                // Bulk Delete old rows from target table where primary key in changedRows
                var pkValues = changedRows.AsEnumerable()
                    .Select(r => r[primaryKeyColumn]?.ToString()?.Trim().ToLowerInvariant())
                    .Where(pk => !string.IsNullOrEmpty(pk))
                    .ToList();

                if (pkValues.Count > 0)
                {
                    var deleteCmd = new SqlCommand($@"
                        DELETE FROM {targetSchema}.{tableName}
                        WHERE {primaryKeyColumn} IN ({string.Join(",", pkValues.Select((_, i) => $"@pk{i}"))})", targetConn, transaction);

                    deleteCmd.CommandTimeout = 0;

                    for (int i = 0; i < pkValues.Count; i++)
                    {
                        deleteCmd.Parameters.AddWithValue($"@pk{i}", pkValues[i]);
                    }

                    int deletedCount = deleteCmd.ExecuteNonQuery();
                    tableLogger.Information("Deleted {Count} old rows from target {Table}", deletedCount, tableName);
                }

                // Insert New changed rows into target table remove identity columns from mapping
                using var bulkCopyChanged = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.KeepIdentity, transaction);
                bulkCopyChanged.DestinationTableName = $"{targetSchema}.{tableName}";

                bulkCopyChanged.BatchSize = 1000;
                bulkCopyChanged.BulkCopyTimeout = 0;

                foreach (DataColumn col in newChangedRows.Columns)
                {
                    if (!identityColumns.Contains(col.ColumnName))
                    {
                        bulkCopyChanged.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                }
                bulkCopyChanged.WriteToServer(newChangedRows);
                recordsInserted += newChangedRows.Rows.Count;
                tableLogger.Information("Inserted {Count} changed rows into target {Table}", newChangedRows.Rows.Count, tableName);

            }

            transaction.Commit();
            status = "Completed";
            tableLogger.Information("Inserted {Count} versioned rows for {Table}", recordsInserted, tableName);
        }
        catch (Exception ex)
        {
            status = "Failed";
            errorMessage = ex.Message;
            tableLogger.Error(ex, "Error in versioned copy for {Table}", tableName);
            transaction?.Rollback();
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
