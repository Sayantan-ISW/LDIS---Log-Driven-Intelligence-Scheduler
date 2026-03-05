using System.Text.RegularExpressions;
using System.Text.Json;
using System.Security.Cryptography;
using System.Text;

namespace NCA.LogParser;

public class LogParserService
{
    private readonly Regex _timestampPattern;
    private readonly Regex _packagePattern;
    private readonly Regex _tablePattern;
    private readonly Regex _instancePattern;
    private readonly Regex _metricsPattern;
    private readonly Regex _batchIdPattern;
    private readonly Regex _manifestIdPattern;

    public LogParserService()
    {
        // Compile regex patterns for performance
        _timestampPattern = new Regex(
            @"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+-]\d{2}:\d{2}) \[(\w{3})\] (.+)$",
            RegexOptions.Compiled);

        _packagePattern = new Regex(
            @"(?:for package|package:|of package) ([A-Za-z0-9_\- /]+?)(?:\.|,|$)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        _tablePattern = new Regex(
            @"Refreshed (\w+) \[",
            RegexOptions.Compiled);

        _instancePattern = new Regex(
            @"instance ([a-zA-Z0-9]+) (?:for|status)",
            RegexOptions.Compiled);

        _metricsPattern = new Regex(
            @"Source Records: (\d+) Records Downloaded, Destination Records: Inserted : (\d+) rows Inserted, Updated: (\d+) rows Updated\]\. Load duration (\d{2}):(\d{2}):(\d{2})",
            RegexOptions.Compiled);

        _batchIdPattern = new Regex(
            @"Batch Id: (\d+)",
            RegexOptions.Compiled);

        _manifestIdPattern = new Regex(
            @"Manifest Id: (\d+)",
            RegexOptions.Compiled);
    }

    public async Task<IEnumerable<LogEvent>> ParseFileAsync(string filePath)
    {
        var events = new List<LogEvent>();
        var lineNumber = 0;
        LogEvent? currentEvent = null;

        await foreach (var line in File.ReadLinesAsync(filePath))
        {
            lineNumber++;

            var match = _timestampPattern.Match(line);

            if (match.Success)
            {
                // Flush previous event if exists
                if (currentEvent != null)
                {
                    events.Add(currentEvent);
                }

                // Start new event
                currentEvent = ParseLine(line, match, filePath, lineNumber);
            }
            else if (currentEvent != null)
            {
                // Multi-line continuation (stack trace)
                currentEvent.Message += "\n" + line;
                
                // Re-derive fields in case multi-line adds important info
                DeriveFields(currentEvent);
            }
        }

        // Add last event
        if (currentEvent != null)
        {
            events.Add(currentEvent);
        }

        // Generate execution IDs
        AssignExecutionIds(events);

        return events;
    }

    private LogEvent ParseLine(string line, Match match, string sourceFile, int lineNumber)
    {
        var timestampStr = match.Groups[1].Value;
        var level = match.Groups[2].Value;
        var message = match.Groups[3].Value;

        var timestamp = DateTime.ParseExact(
            timestampStr,
            "yyyy-MM-dd HH:mm:ss.fff zzz",
            System.Globalization.CultureInfo.InvariantCulture);

        var logEvent = new LogEvent
        {
            TimestampIso = timestamp,
            Timestamp = new DateTimeOffset(timestamp).ToUnixTimeMilliseconds(),
            LogLevel = MapLogLevel(level),
            Message = message,
            MessageHash = ComputeHash(message),
            Metadata = new MetadataFields
            {
                SourceFile = Path.GetFileName(sourceFile),
                LineNumber = lineNumber,
                IngestedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            }
        };

        // Extract JSON if present
        if (message.Contains("[JSONINFORMATION]") || message.Contains("[JSONERROR]"))
        {
            ExtractJson(logEvent, message);
        }

        // Extract entities
        ExtractPackageName(logEvent, message);
        ExtractTableName(logEvent, message);
        ExtractInstanceId(logEvent, message);
        ExtractMetrics(logEvent, message);
        ExtractBatchId(logEvent, message);
        ExtractManifestId(logEvent, message);

        // Classify event type
        ClassifyEvent(logEvent);

        // Derive fields
        DeriveFields(logEvent);

        return logEvent;
    }

    private void ExtractJson(LogEvent logEvent, string message)
    {
        try
        {
            var jsonStart = message.IndexOf('{');
            if (jsonStart >= 0)
            {
                var jsonStr = message[jsonStart..];
                using var jsonDoc = JsonDocument.Parse(jsonStr);

                logEvent.Structured = true;
                
                // Create a simplified dictionary for storage
                logEvent.Details = new Dictionary<string, object>();

                // Try to extract package name from JSON
                if (jsonDoc.RootElement.TryGetProperty("information", out var infoElement))
                {
                    // Check if information is an object or string
                    if (infoElement.ValueKind == JsonValueKind.Object)
                    {
                        if (infoElement.TryGetProperty("PackageName", out var pkgName))
                        {
                            logEvent.PackageName = pkgName.GetString();
                            logEvent.PackageNormalized = NormalizePackageName(logEvent.PackageName ?? "");
                        }
                        
                        if (infoElement.TryGetProperty("BatchId", out var batchId))
                        {
                            logEvent.Details["batch_id"] = batchId.GetString() ?? "";
                        }

                        if (infoElement.TryGetProperty("ManifestId", out var manifestId))
                        {
                            logEvent.Details["manifest_id"] = manifestId.GetString() ?? "";
                        }
                    }
                    else if (infoElement.ValueKind == JsonValueKind.String)
                    {
                        // Information is a string, just store it
                        logEvent.Details["information"] = infoElement.GetString() ?? "";
                    }
                }

                // Try to extract error information
                if (jsonDoc.RootElement.TryGetProperty("error", out var errorElement))
                {
                    if (errorElement.ValueKind == JsonValueKind.Object)
                    {
                        if (errorElement.TryGetProperty("Message", out var errorMsg))
                        {
                            logEvent.Details["error_message"] = errorMsg.GetString() ?? "";
                        }
                        if (errorElement.TryGetProperty("StackTrace", out var stackTrace))
                        {
                            logEvent.Details["stack_trace"] = stackTrace.GetString() ?? "";
                        }
                    }
                    else if (errorElement.ValueKind == JsonValueKind.String)
                    {
                        logEvent.Details["error"] = errorElement.GetString() ?? "";
                    }
                }

                // Store the raw JSON for reference
                logEvent.Details["raw_json"] = jsonStr;
            }
        }
        catch (JsonException ex)
        {
            // If JSON parsing fails, just mark as unstructured and continue
            logEvent.Structured = false;
            logEvent.Details ??= new Dictionary<string, object>();
            logEvent.Details["json_parse_error"] = ex.Message;
        }
    }

    private void ExtractPackageName(LogEvent logEvent, string message)
    {
        if (logEvent.PackageName != null) return; // Already extracted from JSON

        var match = _packagePattern.Match(message);
        if (match.Success)
        {
            logEvent.PackageName = match.Groups[1].Value.Trim();
            logEvent.PackageNormalized = NormalizePackageName(logEvent.PackageName);
        }
    }

    private void ExtractTableName(LogEvent logEvent, string message)
    {
        var match = _tablePattern.Match(message);
        if (match.Success)
        {
            logEvent.Details ??= new Dictionary<string, object>();
            logEvent.Details["table_name"] = match.Groups[1].Value;
        }
    }

    private void ExtractInstanceId(LogEvent logEvent, string message)
    {
        var match = _instancePattern.Match(message);
        if (match.Success)
        {
            logEvent.InstanceId = match.Groups[1].Value;
        }
    }

    private void ExtractMetrics(LogEvent logEvent, string message)
    {
        var match = _metricsPattern.Match(message);
        if (match.Success)
        {
            logEvent.Details ??= new Dictionary<string, object>();
            logEvent.Details["records_downloaded"] = int.Parse(match.Groups[1].Value);
            logEvent.Details["rows_inserted"] = int.Parse(match.Groups[2].Value);
            logEvent.Details["rows_updated"] = int.Parse(match.Groups[3].Value);

            var hours = int.Parse(match.Groups[4].Value);
            var minutes = int.Parse(match.Groups[5].Value);
            var seconds = int.Parse(match.Groups[6].Value);
            logEvent.Details["load_duration_seconds"] = hours * 3600 + minutes * 60 + seconds;
        }
    }

    private void ExtractBatchId(LogEvent logEvent, string message)
    {
        var match = _batchIdPattern.Match(message);
        if (match.Success)
        {
            logEvent.Details ??= new Dictionary<string, object>();
            logEvent.Details["batch_id"] = match.Groups[1].Value;
        }
    }

    private void ExtractManifestId(LogEvent logEvent, string message)
    {
        var match = _manifestIdPattern.Match(message);
        if (match.Success)
        {
            logEvent.Details ??= new Dictionary<string, object>();
            logEvent.Details["manifest_id"] = match.Groups[1].Value;
        }
    }

    private void ClassifyEvent(LogEvent logEvent)
    {
        var message = logEvent.Message.ToLower();

        logEvent.EventType = message switch
        {
            var m when m.Contains("initiating incremental loads") => "execution_start",
            var m when m.Contains("incremental load completed") => "execution_complete",
            var m when m.Contains("refreshed") && m.Contains("source records:") => "table_load",
            var m when m.Contains("all tables") && m.Contains("synchronized") => "package_sync",
            var m when m.Contains("starting instance") => "instance_start",
            var m when m.Contains("status after stopinstance") => "instance_stop",
            var m when m.Contains("successfully completed scheduling") => "schedule_complete",
            var m when m.Contains("transient data movement") && m.Contains("initiated") => "download_start",
            var m when m.Contains("transient data movement") && m.Contains("completed") => "download_complete",
            var m when m.Contains("adding table") => "table_added",
            var m when m.Contains("deleting table") => "table_deleted",
            var m when m.Contains("resetting table") => "table_reset",
            _ => logEvent.LogLevel == "ERROR" || logEvent.LogLevel == "FATAL" ? "error" : "info"
        };
    }

    private void DeriveFields(LogEvent logEvent)
    {
        logEvent.Derived.IsError =
            logEvent.LogLevel == "ERROR" || logEvent.LogLevel == "FATAL";

        if (logEvent.Details != null)
        {
            if (logEvent.Details.TryGetValue("records_downloaded", out var records))
            {
                var recordCount = Convert.ToInt32(records);
                logEvent.Derived.HasDataChanges = recordCount > 0;
                logEvent.Derived.IsZeroChange = recordCount == 0;
            }
        }
    }

    private void AssignExecutionIds(List<LogEvent> events)
    {
        var executionState = new Dictionary<string, string>();

        foreach (var evt in events)
        {
            if (evt.PackageNormalized == null) continue;

            if (evt.EventType == "execution_start")
            {
                // Generate new execution ID
                var date = evt.TimestampIso.ToString("yyyyMMdd");
                var time = evt.TimestampIso.ToString("HHmmss");
                var execId = $"exec_{date}_{time}_{evt.PackageNormalized}";
                
                executionState[evt.PackageNormalized] = execId;
                evt.ExecutionId = execId;
            }
            else if (executionState.ContainsKey(evt.PackageNormalized))
            {
                // Assign current execution ID for this package
                evt.ExecutionId = executionState[evt.PackageNormalized];
            }
        }
    }

    private string MapLogLevel(string level) => level switch
    {
        "INF" => "INFO",
        "ERR" => "ERROR",
        "FTL" => "FATAL",
        "WRN" => "WARNING",
        _ => level
    };

    private string NormalizePackageName(string packageName)
    {
        return packageName
            .Replace("XXC_ISW - ", "")
            .Replace("XXC_ISW-", "")
            .Replace("/", "_")
            .Replace(" ", "_")
            .ToLower();
    }

    private string ComputeHash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(bytes).ToLower();
    }
}
