using Microsoft.Extensions.Configuration;
using Serilog;

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

        var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var logFilePath = $"bulk_copy_log_{timestamp}.txt";

        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.File(logFilePath)
            .CreateLogger();

        try
        {
            var runner = new BulkCopyManager(config);
            runner.Run();
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}
