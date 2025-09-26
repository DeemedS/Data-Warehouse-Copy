using System.Data;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Serilog;

public class BulkCopyManager
{
    private readonly IConfiguration _config;

    public BulkCopyManager(IConfiguration config)
    {
        _config = config;
    }

    public void Run()
    {
        var sourceConnStr = _config.GetConnectionString("Source")
            ?? throw new InvalidOperationException("Missing 'Source' connection string.");

        var targetConnStr = _config.GetConnectionString("Target")
            ?? throw new InvalidOperationException("Missing 'Target' connection string.");

        var bulkCopySettings = _config.GetSection("BulkCopyConfig");

        var sourceSchema = bulkCopySettings["SourceSchema"];
        if (string.IsNullOrWhiteSpace(sourceSchema))
        {
            throw new InvalidOperationException("Missing 'SourceSchema' in configuration.");
        }

        var targetSchema = bulkCopySettings["TargetSchema"];
        if (string.IsNullOrWhiteSpace(targetSchema))
        {
            throw new InvalidOperationException("Missing 'TargetSchema' in configuration.");
        }

        var scheduleMode = bulkCopySettings["ScheduleMode"]?.ToLowerInvariant();

        string? dateFrom;
        string? dateTo;
        
        Log.Information("ScheduleMode: {ScheduleMode}", scheduleMode ?? "null");

        if (string.IsNullOrWhiteSpace(scheduleMode))
        {
            dateFrom = bulkCopySettings["DateFrom"]
                ?? throw new InvalidOperationException("DateFrom missing in config when ScheduleMode is null.");
            dateTo = bulkCopySettings["DateTo"]
                ?? throw new InvalidOperationException("DateTo missing in config when ScheduleMode is null.");
        }
        else
        {
            switch (scheduleMode.ToLower())
            {
                case "hourly":
                    var now = DateTime.Now;
                    if (now.Hour == 0)
                    {
                        dateFrom = now.AddDays(-1).ToString("yyyy-MM-dd");
                        dateTo = now.AddDays(-1).ToString("yyyy-MM-dd");
                    }
                    else
                    {
                        dateFrom = now.ToString("yyyy-MM-dd");
                        dateTo = now.ToString("yyyy-MM-dd");
                    }
                    break;

                case "daily":
                    dateFrom = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");
                    dateTo = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");
                    break;

                case "weekly":
                    dateFrom = DateTime.Now.AddDays(-8).ToString("yyyy-MM-dd");
                    dateTo = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");
                    break;

                default:
                    Log.Warning("Unknown ScheduleMode '{ScheduleMode}', defaulting to 'daily'.", scheduleMode);
                    dateFrom = bulkCopySettings["DateFrom"];
                    dateTo = bulkCopySettings["DateTo"];
                    break;
            }
        }

        var tables = bulkCopySettings.GetSection("Tables")
            .GetChildren()
            .Select(x => new
            {
                TableName = x["table_name"],
                TableType = x["table_type"],
                DateColumn = x["date_column"],
                UpdateDateColumn = x["update_date_column"],
                PrimaryKey = x["primary_key"]
            })
            .Where(t => !string.IsNullOrEmpty(t.TableName))
            .ToList();

        var normalList = tables.Where(t => t.TableType == "fact" && t.UpdateDateColumn == null && t.PrimaryKey == null).ToList();
        var copyUpdateList = tables.Where(t => t.TableType == "fact" && t.UpdateDateColumn != null && t.PrimaryKey != null).ToList();
        var truncateList = tables.Where(t => t.TableType == "dim").ToList();
        var storedProcList = tables.Where(t => t.TableType == "sproc").ToList();
        var historicalList = tables.Where(t => t.TableType == "historical" && t.UpdateDateColumn != null).ToList();


        Log.Information("Starting bulk copy process...");
        Log.Information("Source Schema: {SourceSchema}", sourceSchema);
        Log.Information("Target Schema: {TargetSchema}", targetSchema);
        Log.Information("Date Range: {DateFrom} to {DateTo}", dateFrom, dateTo);
        Log.Information("Tables to copy: {Tables}", string.Join(", ", tables.Select(t => t.TableName)));

        Log.Information("Starting process for stored procedures: {Tables}", string.Join(", ", storedProcList.Select(t => t.TableName)));
        foreach (var sproc in storedProcList)
        {
            var now = DateTime.Now;
            var effectiveDateFrom = dateFrom;
            var effectiveDateTo = dateTo;

            if (now.Hour == 0)
            {
                effectiveDateFrom = now.AddDays(-1).ToString("yyyy-MM-dd");
                effectiveDateTo = now.AddDays(-1).ToString("yyyy-MM-dd");

                Log.Information("Midnight run detected. Overriding date range to yesterday: {From} to {To}", effectiveDateFrom, effectiveDateTo);
            }

            StoredProcRunner.ExecuteStoredProc(
                sproc.TableName!,
                targetConnStr,
                effectiveDateFrom!,
                effectiveDateTo!
            );
        }

        Log.Information("Starting process for tables: {Tables}", string.Join(", ", normalList.Select(t => t.TableName)));
        foreach (var table in copyUpdateList)
        {
            TableCopyService.CopyTable(
                table.TableName!,
                sourceConnStr,
                targetConnStr,
                targetSchema,
                sourceSchema,
                dateFrom!,
                dateTo!,
                false,
                table.DateColumn
            );

            TableUpdateService.UpdateTable(
                table.TableName!,
                sourceConnStr,
                targetConnStr,
                targetSchema,
                sourceSchema,
                dateFrom!,
                dateTo!,
                table.DateColumn,
                table.UpdateDateColumn,
                table.PrimaryKey!
            );
        }

        Log.Information("Starting process for tables: {Tables}", string.Join(", ", normalList.Select(t => t.TableName)));
        foreach (var table in normalList)
        {
            TableCopyService.CopyTable(
                table.TableName!,
                sourceConnStr,
                targetConnStr,
                targetSchema,
                sourceSchema,
                dateFrom!,
                dateTo!,
                false,
                table.DateColumn
            );
        }

        Log.Information("Starting  process for tables: {Tables}", string.Join(", ", truncateList.Select(t => t.TableName)));
        Parallel.ForEach(truncateList, table =>
        {
            TableCopyService.CopyTable(
                table.TableName!,
                sourceConnStr,
                targetConnStr,
                targetSchema,
                sourceSchema,
                dateFrom!,
                dateTo!,
                true,
                table.DateColumn
            );
        });

        Log.Information("All tables processed.");
    }
}
