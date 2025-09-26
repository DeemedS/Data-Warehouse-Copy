using System.Data;
using Microsoft.Data.SqlClient;
using Serilog;

public static class StoredProcRunner
{
    public static void ExecuteStoredProc(
        string storedProcName,
        string connectionString,
        string startDate,
        string endDate
    )
    {
        try
        {
            using var conn = new SqlConnection(connectionString);
            using var cmd = new SqlCommand(storedProcName, conn)
            {
                CommandType = CommandType.StoredProcedure,
                CommandTimeout = 0
            };

            cmd.Parameters.AddWithValue("@StartDateParam", startDate);
            cmd.Parameters.AddWithValue("@EndDateParam", endDate);

            conn.Open();
            cmd.ExecuteNonQuery();

            Log.Information("Stored procedure {StoredProc} executed successfully for range {StartDate} to {EndDate}.",
                storedProcName, startDate, endDate);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error executing stored procedure {StoredProc}.", storedProcName);
            throw;
        }
    }
}
