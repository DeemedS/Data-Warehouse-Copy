using Microsoft.Extensions.Configuration;
using Serilog;

class Program
{
    static void Main()
    {
        var exePath = System.Diagnostics.Process.GetCurrentProcess().MainModule?.FileName;
        var basedir = Path.GetDirectoryName(exePath)!;

        var config = new ConfigurationBuilder()
            .SetBasePath(basedir)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            // .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();
            
        var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var monthFolder = DateTime.Now.ToString("yyyy-MM");
        var logDir = Path.Combine(basedir, "logs", monthFolder, "Main");
        Directory.CreateDirectory(logDir);

        var logFilePath = Path.Combine(logDir, $"bulk_copy_log_{timestamp}.txt");

        Console.WriteLine($"[DEBUG] Log file will be written to: {logFilePath}");

        Log.Logger = new LoggerConfiguration()
            // .WriteTo.Console()
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
