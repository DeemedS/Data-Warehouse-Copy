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

        var dateFrom = string.IsNullOrWhiteSpace(bulkCopySettings["DateFrom"])
            ? DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd")
            : bulkCopySettings["DateFrom"];

        var dateTo = string.IsNullOrWhiteSpace(bulkCopySettings["DateTo"])
            ? DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd")
            : bulkCopySettings["DateTo"];

        var tables = bulkCopySettings.GetSection("Tables")
            .GetChildren()
            .Select(x => new
            {
                TableName = x["table_name"],
                TableType = x["table_type"],
                DateColumn = x["date_column"]
            })
            .Where(t => !string.IsNullOrEmpty(t.TableName))
            .ToList();

        var normalList = tables.Where(t => t.TableType == "fact").ToList();
        var truncateList = tables.Where(t => t.TableType == "dim").ToList();

        Log.Information("Starting bulk copy process...");
        Log.Information("Source Schema: {SourceSchema}", sourceSchema);
        Log.Information("Target Schema: {TargetSchema}", targetSchema);
        Log.Information("Date Range: {DateFrom} to {DateTo}", dateFrom, dateTo);
        Log.Information("Tables to copy: {Tables}", string.Join(", ", tables.Select(t => t.TableName)));

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
