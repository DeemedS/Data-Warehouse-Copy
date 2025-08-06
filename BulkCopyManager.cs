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

        var tables = new List<string>
        {
            "tbl_transaction_master",
            "tbl_transaction_inserted_bill_logs",
            "tbl_utility_wallet",
        };

        var truncateTables = new HashSet<string>
        {
            "tbl_utility_wallet",
        };

        var dateColumns = new Dictionary<string, string>
        {
            { "tbl_transaction_master", "transaction_date" },
            { "tbl_transaction_inserted_bill_logs", "inserted_date" }
        };

        var dateFrom = "2025-07-07";
        var dateTo = "2025-07-13";

        foreach (var table in tables)
        {
            bool useTruncate = truncateTables.Contains(table);
            string? dateColumn = dateColumns.TryGetValue(table, out var col) ? col : null;

            TableCopyService.CopyTable(table, sourceConnStr, targetConnStr, dateFrom, dateTo, useTruncate, dateColumn);
        }

        Log.Information("All tables processed.");
    }
}
