namespace Pulse.Mqtt.Storage.SqlServer;

internal sealed class SqlServerTables
{
    private SqlServerTables(string schemaName, string tablePrefix)
    {
        SchemaName = schemaName;
        TablePrefix = tablePrefix;
        Subscriptions = CreateName("Subscriptions");
        InFlightOutbound = CreateName("InFlightOutbound");
        InFlightInbound = CreateName("InFlightInbound");
        Queue = CreateName("Queue");
    }

    public string SchemaName { get; }

    public string TablePrefix { get; }

    public string Subscriptions { get; }

    public string InFlightOutbound { get; }

    public string InFlightInbound { get; }

    public string Queue { get; }

    public string QualifiedSubscriptions => Qualified(Subscriptions);

    public string QualifiedInFlightOutbound => Qualified(InFlightOutbound);

    public string QualifiedInFlightInbound => Qualified(InFlightInbound);

    public string QualifiedQueue => Qualified(Queue);

    public static SqlServerTables Create(SqlServerStorageOptions? options)
    {
        options ??= new SqlServerStorageOptions();
        return new SqlServerTables(
            ValidateIdentifier(options.SchemaName, nameof(options.SchemaName)),
            ValidateIdentifier(options.TablePrefix, nameof(options.TablePrefix)));
    }

    public string Qualified(string tableName) => $"{QuoteIdentifier(SchemaName)}.{QuoteIdentifier(tableName)}";

    public static string QuoteIdentifier(string identifier) => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";

    public static string EscapeLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private string CreateName(string suffix) => ValidateIdentifier(TablePrefix + suffix, nameof(TablePrefix));

    private static string ValidateIdentifier(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("SQL Server identifiers cannot be empty.", parameterName);
        }

        if (value.Length > 128)
        {
            throw new ArgumentException("SQL Server identifiers cannot exceed 128 characters.", parameterName);
        }

        return value;
    }
}
