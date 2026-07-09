namespace Pulse.Mqtt.Storage.SqlServer;

/// <summary>Table naming options for the SQL Server storage package.</summary>
public sealed record SqlServerStorageOptions
{
    /// <summary>The database schema that owns the storage tables. Defaults to <c>dbo</c>.</summary>
    public string SchemaName { get; init; } = "dbo";

    /// <summary>
    /// The prefix applied to every table this package owns. Defaults to <c>PulseMqtt</c>, producing
    /// tables such as <c>PulseMqttSubscriptions</c> and <c>PulseMqttQueue</c>.
    /// </summary>
    public string TablePrefix { get; init; } = "PulseMqtt";
}
