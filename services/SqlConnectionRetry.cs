using Microsoft.Data.SqlClient;
using Serilog;

public static class SqlConnectionRetry
{
	public static void OpenWithRetry(SqlConnection connection, ILogger logger, string connectionName)
	{
		const int maxAttempts = 3;

		for (var attempt = 1; attempt <= maxAttempts; attempt++)
		{
			try
			{
				connection.Open();
				return;
			}
			catch (Exception ex) when (attempt < maxAttempts)
			{
				var delay = TimeSpan.FromMinutes(1);
				logger.Warning(
					ex,
					"Failed to open {ConnectionName} connection (attempt {Attempt}/{MaxAttempts}); retrying in {Delay}",
					connectionName,
					attempt,
					maxAttempts,
					delay);
				Thread.Sleep(delay);
			}
		}
	}
}
