using Microsoft.Data.SqlClient;
using Serilog;
using System.Data;

public static class TableCopyServiceWithVersioning
{
    public static void CopyTableWithVersioningFullIncremental(
        string tableName,
        string sourceConnStr,
        string targetConnStr,
        string targetSchema,
        string sourceSchema,
        string updateDateColumn,
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
                logCmd.Parameters.AddWithValue("@StartDateParam", DBNull.Value);
                logCmd.Parameters.AddWithValue("@EndDateParam", DBNull.Value);
                logCmd.Parameters.AddWithValue("@DateColumn", updateDateColumn);
                logCmd.Parameters.AddWithValue("@UpdateDateColumn", updateDateColumn);
                logCmd.Parameters.AddWithValue("@PrimaryKey", primaryKeyColumn);
                logCmd.Parameters.AddWithValue("@Process", "SCD Type 2");
                logCmd.Parameters.AddWithValue("@Status", status);
                logCmd.Parameters.AddWithValue("@ErrorMessage", DBNull.Value);

                logId = (int)logCmd.ExecuteScalar();
            }

            // --- Step 1: Load source data ---
            var srcCmd = new SqlCommand($"SELECT * FROM {sourceSchema}.{tableName}", sourceConn);
            srcCmd.CommandTimeout = 0;
            var srcAdapter = new SqlDataAdapter(srcCmd);
            var srcTable = new DataTable();
            srcAdapter.Fill(srcTable);
            tableLogger.Information("Loaded {Count} rows from source {Table}", srcTable.Rows.Count, tableName);

            // --- Step 2: Load active target data (valid_end IS 9999-12-31) ---
            var tgtCmd = new SqlCommand($@"
                SELECT * 
                FROM {targetSchema}.{tableName} 
                WHERE valid_end = '9999-12-31 00:00:00.000'", targetConn);
            tgtCmd.CommandTimeout = 0;
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

            using var transaction = targetConn.BeginTransaction();

            foreach (DataRow srcRow in srcTable.Rows)
            {
                string pk = srcRow[primaryKeyColumn]?.ToString()?.Trim().ToLowerInvariant() ?? "";
                bool isNewRecord = !targetLookup.ContainsKey(pk);
                bool updateDateChanged = false;
                bool rowChanged = false;
                tableLogger.Information("Processing record PK={PK}: IsNew={IsNew}", pk, isNewRecord);

                if (!isNewRecord)
                {
                    var tgtRow = targetLookup[pk];

                    DateTime srcUpdateDate = Convert.ToDateTime(srcRow[updateDateColumn]);
                    DateTime tgtUpdateDate = Convert.ToDateTime(tgtRow[updateDateColumn]);

                    tableLogger.Information("Comparing record PK={PK}: Source {SrcDate}, Target {TgtDate}", pk, srcUpdateDate, tgtUpdateDate);

                    if (srcUpdateDate > tgtUpdateDate)
                    {
                        updateDateChanged = true;
                    }

                    // Check if any other columns have changed (excluding identity and versioning columns)
                    foreach (DataColumn col in srcTable.Columns)
                    {
                        if (identityColumns.Contains(col.ColumnName) || 
                            string.Equals(col.ColumnName, "valid_start", StringComparison.OrdinalIgnoreCase) || 
                            string.Equals(col.ColumnName, "valid_end", StringComparison.OrdinalIgnoreCase))
                        {
                            continue; // Skip identity and versioning columns
                        }

                        var srcValue = srcRow[col];
                        var tgtValue = tgtRow[col];

                        if (!object.Equals(srcValue, tgtValue))
                        {
                            rowChanged = true;
                            tableLogger.Information("Column {Col} changed for PK={PK}: Source {SrcVal}, Target {TgtVal}", col.ColumnName, pk, srcValue, tgtValue);
                            break; // No need to check further columns
                        }
                    }
                }

                if (isNewRecord || updateDateChanged || rowChanged)
                {
                    DateTime newValidStart = TrimMs(Convert.ToDateTime(srcRow[updateDateColumn]));
                    DateTime newValidEnd = DateTime.Parse("9999-12-31 00:00:00.000"); // For SQL DATE type
                    
                    if (updateDateChanged)
                    {
                        // Close previous record (set valid_end to previous day)
                        var closeCmd = new SqlCommand($@"
                            UPDATE {targetSchema}.{tableName}
                            SET valid_end = DATEADD(MILLISECOND, -3, @new_valid_start)
                            WHERE {primaryKeyColumn} = @pk AND valid_end = @valid_end", targetConn, transaction);

                        closeCmd.CommandTimeout = 0;
                        closeCmd.Parameters.Add("@new_valid_start", SqlDbType.DateTime).Value = newValidStart;
                        closeCmd.Parameters.AddWithValue("@pk", pk);
                        closeCmd.Parameters.Add("@valid_end", SqlDbType.DateTime).Value = newValidEnd;
                        closeCmd.ExecuteNonQuery();
                    }

                    if (rowChanged && !updateDateChanged)
                    {
                        //new valid start is the current timestamp for the new version
                        DateTime newValidStartForRowhange = TrimMs(DateTime.Now);

                        // Close previous record (set valid_end to previous day)
                        var closeCmd = new SqlCommand($@"
                            UPDATE {targetSchema}.{tableName}
                            SET valid_end = DATEADD(MILLISECOND, -3, @new_valid_start)
                            WHERE {primaryKeyColumn} = @pk AND valid_end = @valid_end", targetConn, transaction);

                        closeCmd.CommandTimeout = 0;
                        closeCmd.Parameters.Add("@new_valid_start", SqlDbType.DateTime).Value = newValidStartForRowhange;
                        closeCmd.Parameters.AddWithValue("@pk", pk);
                        closeCmd.Parameters.Add("@valid_end", SqlDbType.DateTime).Value = newValidEnd;
                        closeCmd.ExecuteNonQuery();
                    }

                    // Build insert excluding identity columns
                    var columnNames = srcTable.Columns.Cast<DataColumn>()
                        .Where(c => !string.Equals(c.ColumnName, "id", StringComparison.OrdinalIgnoreCase))
                        .Select(c => c.ColumnName)
                        .ToList();

                    columnNames.Add("valid_start");
                    columnNames.Add("valid_end");

                    var insertCols = string.Join(",", columnNames);
                    var insertParams = string.Join(",", columnNames.Select(c => "@" + c));

                    var insertCmd = new SqlCommand($@"
                        INSERT INTO {targetSchema}.{tableName} ({insertCols})
                        VALUES ({insertParams})", targetConn, transaction);
                    insertCmd.CommandTimeout = 0;

                    foreach (var col in srcTable.Columns.Cast<DataColumn>())
                    {
                        if (string.Equals(col.ColumnName, "id", StringComparison.OrdinalIgnoreCase))
                            continue;

                        insertCmd.Parameters.AddWithValue("@" + col.ColumnName, srcRow[col] ?? DBNull.Value);
                    }

                    //insertCmd.Parameters.Add("@valid_start", SqlDbType.DateTime).Value = newValidStart;
                    insertCmd.Parameters.Add("@valid_start", SqlDbType.DateTime).Value = rowChanged && !updateDateChanged ? TrimMs(DateTime.Now) : newValidStart;
                    insertCmd.Parameters.Add("@valid_end", SqlDbType.DateTime).Value = newValidEnd;

                    recordsInserted += insertCmd.ExecuteNonQuery();
                }
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

    private static DateTime TrimMs(DateTime dateTime)
    {
        return dateTime.AddTicks(-(dateTime.Ticks % TimeSpan.TicksPerSecond));
    }
}
