using System.Text.Json.Serialization;

namespace NCA.LogParser;

public class LogEvent
{
    [JsonPropertyName("event_id")]
    public string EventId { get; set; } = Guid.NewGuid().ToString();

    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }

    [JsonPropertyName("timestamp_iso")]
    public DateTime TimestampIso { get; set; }

    [JsonPropertyName("log_level")]
    public string LogLevel { get; set; } = string.Empty;

    [JsonPropertyName("event_type")]
    public string EventType { get; set; } = string.Empty;

    [JsonPropertyName("package_name")]
    public string? PackageName { get; set; }

    [JsonPropertyName("package_normalized")]
    public string? PackageNormalized { get; set; }

    [JsonPropertyName("instance_id")]
    public string? InstanceId { get; set; }

    [JsonPropertyName("execution_id")]
    public string? ExecutionId { get; set; }

    [JsonPropertyName("message")]
    public string Message { get; set; } = string.Empty;

    [JsonPropertyName("message_hash")]
    public string? MessageHash { get; set; }

    [JsonPropertyName("structured")]
    public bool Structured { get; set; }

    [JsonPropertyName("details")]
    public Dictionary<string, object>? Details { get; set; }

    [JsonPropertyName("derived")]
    public DerivedFields Derived { get; set; } = new();

    [JsonPropertyName("metadata")]
    public MetadataFields Metadata { get; set; } = new();
}

public class DerivedFields
{
    [JsonPropertyName("has_data_changes")]
    public bool HasDataChanges { get; set; }

    [JsonPropertyName("is_zero_change")]
    public bool IsZeroChange { get; set; }

    [JsonPropertyName("is_error")]
    public bool IsError { get; set; }
}

public class MetadataFields
{
    [JsonPropertyName("source_file")]
    public string? SourceFile { get; set; }

    [JsonPropertyName("line_number")]
    public int LineNumber { get; set; }

    [JsonPropertyName("ingested_at")]
    public long IngestedAt { get; set; }

    [JsonPropertyName("nca_version")]
    public string NcaVersion { get; set; } = "R25.3";
}
