using Xunit;

namespace Pulse.Mqtt.Storage.SqlServer.Tests;

internal sealed class SqlServerFactAttribute : FactAttribute
{
    public SqlServerFactAttribute()
    {
        if (!SqlServerTestDatabase.HasConnectionString)
        {
            Skip = $"Set {SqlServerTestDatabase.ConnectionStringVariable} to run SQL Server storage tests.";
        }
    }
}
