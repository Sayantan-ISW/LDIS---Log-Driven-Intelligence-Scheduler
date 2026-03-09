using System.Text.RegularExpressions;
using System.Text.Json;
using System.Security.Cryptography;
using System.Text;

namespace NCA.LogParser;

public class LogParserService
{
    // --- NCA regex patterns ---
    private readonly Regex _timestampPattern;
    private readonly Regex _packagePattern;
    private readonly Regex _tablePattern;
    private readonly Regex _instancePattern;
    private readonly Regex _metricsPattern;
    private readonly Regex _batchIdPattern;
    private readonly Regex _manifestIdPattern;

    // --- Sirus regex patterns ---
    private readonly Regex _sirusTimestampPattern;
    private readonly Regex _sirusVersionPattern;
    private readonly Regex _sirusExtractPattern;
    private readonly Regex _sirusExtractBatchPattern;
    private readonly Regex _sirusLoadingPattern;
    private readonly Regex _sirusStatsPattern;
    private readonly Regex _sirusLoadedTablePattern;
    private readonly Regex _sirusInfoCodePattern;
    private readonly Regex _sirusErrorPattern;

    // --- Classification regex (first-line signature) ---
    private static readonly Regex NcaSignature = new(
        @"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+-]\d{2}:\d{2} \[",
        RegexOptions.Compiled);

    private static readonly Regex SirusSignature = new(
        @"^\w{3} \w{3} \d{1,2} \d{2}:\d{2}:\d{2} UTC \d{4}:",
        RegexOptions.Compiled);

    public LogParserService()
    {
        // Compile NCA regex patterns
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

        // Compile Sirus regex patterns
        _sirusTimestampPattern = new Regex(
            @"^(\w{3} \w{3} \d{1,2} \d{2}:\d{2}:\d{2} UTC \d{4}):\s*(.*)$",
            RegexOptions.Compiled);

        _sirusVersionPattern = new Regex(
            @"Running Bryteflow Ingest-AZ ([\d.]+[a-z]?) \(Build (\d+)\)",
            RegexOptions.Compiled);

        _sirusExtractPattern = new Regex(
            @"(Full|Delta) Extract FILES\.(\w+?)(?:\s+complete\s+\((\d+) records\))?$",
            RegexOptions.Compiled);

        _sirusExtractBatchPattern = new Regex(
            @"Extract(?:ing|ed) (\d+)",
            RegexOptions.Compiled);

        _sirusLoadingPattern = new Regex(
            @"Loading table (\w+\.\w+) with ([\d,]+) records\(([\d,]+) bytes\)\s*-\s*(\w+)",
            RegexOptions.Compiled);

        _sirusStatsPattern = new Regex(
            @"Info\(RL872\): Table (\w+) Stats\(Src=(\d+)/(\d+)/(\d+) Dst=(\d+)/(\d+)/(\d+)\)",
            RegexOptions.Compiled);

        _sirusLoadedTablePattern = new Regex(
            @"Loaded table (\w+\.\w+)\((\d+) of (\d+) left\)",
            RegexOptions.Compiled);

        _sirusInfoCodePattern = new Regex(
            @"(Info|Error)\((\w+)\):\s*(.+)",
            RegexOptions.Compiled);

        _sirusErrorPattern = new Regex(
            @"Error\((\w+)\):\s*(.+)",
            RegexOptions.Compiled);
    }

    // ========================================================================
    // Log Classification
    // ========================================================================

    /// <summary>
    /// Classifies a log file by reading its first few lines and matching signatures.
    /// Falls back to filename pattern if content is inconclusive.
    /// </summary>
    public static async Task<LogSource> ClassifyFileAsync(string filePath)
    {
        var fileName = Path.GetFileName(filePath);

        // Fast path: filename-based classification
        if (fileName.StartsWith("NCAReplicationLog", StringComparison.OrdinalIgnoreCase) &&
            fileName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
            return LogSource.NCA;

        if (fileName.StartsWith("sirus-", StringComparison.OrdinalIgnoreCase) &&
            fileName.EndsWith(".log", StringComparison.OrdinalIgnoreCase))
            return LogSource.Sirus;

        // Content-based classification: read first 5 non-empty lines
        var linesRead = 0;
        await foreach (var line in File.ReadLinesAsync(filePath))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            if (NcaSignature.IsMatch(line)) return LogSource.NCA;
            if (SirusSignature.IsMatch(line)) return LogSource.Sirus;
            if (++linesRead >= 5) break;
        }

        return LogSource.Unknown;
    }

    /// <summary>
    /// Discovers all parseable log files recursively under a root directory.
    /// Returns files grouped by their classified LogSource.
    /// </summary>
    public static async Task<Dictionary<LogSource, List<string>>> DiscoverAndClassifyAsync(string rootDirectory)
    {
        var result = new Dictionary<LogSource, List<string>>
        {
            [LogSource.NCA] = new(),
            [LogSource.Sirus] = new(),
            [LogSource.Unknown] = new()
        };

        var extensions = new[] { ".txt", ".log" };
        var files = Directory.EnumerateFiles(rootDirectory, "*.*", SearchOption.AllDirectories)
            .Where(f => extensions.Contains(Path.GetExtension(f).ToLowerInvariant()));

        foreach (var file in files)
        {
            var source = await ClassifyFileAsync(file);
            result[source].Add(file);
        }

        return result;
    }

    // ========================================================================
    // Batch Folder Processing
    // ========================================================================

    /// <summary>
    /// Processes an entire folder tree: discovers logs, classifies them,
    /// parses each file, and writes structured JSON to separate output folders.
    /// </summary>
    public async Task<BatchResult> ProcessFolderAsync(
        string inputDirectory,
        string outputDirectory,
        Action<string>? onProgress = null)
    {
        var result = new BatchResult();

        onProgress?.Invoke($"Discovering log files in: {inputDirectory}");
        var classified = await DiscoverAndClassifyAsync(inputDirectory);

        result.NcaFilesFound = classified[LogSource.NCA].Count;
        result.SirusFilesFound = classified[LogSource.Sirus].Count;
        result.UnknownFilesFound = classified[LogSource.Unknown].Count;

        var ncaOutputDir = Path.Combine(outputDirectory, "NCA");
        var sirusOutputDir = Path.Combine(outputDirectory, "Sirus");
        Directory.CreateDirectory(ncaOutputDir);
        Directory.CreateDirectory(sirusOutputDir);

        // Process NCA logs
        foreach (var file in classified[LogSource.NCA])
        {
            try
            {
                onProgress?.Invoke($"[NCA] Parsing: {Path.GetFileName(file)}");
                var events = await ParseNcaFileAsync(file);
                var eventsList = events.ToList();

                var outputFile = Path.Combine(ncaOutputDir,
                    Path.GetFileNameWithoutExtension(file) + "_Structured.json");
                await WriteStructuredOutputAsync(eventsList, file, outputFile, "NCA");

                result.NcaFilesProcessed++;
                result.TotalEventsProcessed += eventsList.Count;
            }
            catch (Exception ex)
            {
                result.Errors.Add($"[NCA] {Path.GetFileName(file)}: {ex.Message}");
            }
        }

        // Process Sirus logs
        foreach (var file in classified[LogSource.Sirus])
        {
            try
            {
                onProgress?.Invoke($"[Sirus] Parsing: {Path.GetFileName(file)}");
                var events = await ParseSirusFileAsync(file);
                var eventsList = events.ToList();

                var outputFile = Path.Combine(sirusOutputDir,
                    Path.GetFileNameWithoutExtension(file) + "_Structured.json");
                await WriteStructuredOutputAsync(eventsList, file, outputFile, "Sirus");

                result.SirusFilesProcessed++;
                result.TotalEventsProcessed += eventsList.Count;
            }
            catch (Exception ex)
            {
                result.Errors.Add($"[Sirus] {Path.GetFileName(file)}: {ex.Message}");
            }
        }

        // Log unknown files
        foreach (var file in classified[LogSource.Unknown])
        {
            result.Errors.Add($"[Unknown] Could not classify: {file}");
        }

        return result;
    }

    private static async Task WriteStructuredOutputAsync(
        List<LogEvent> events, string sourceFile, string outputPath, string source)
    {
        var summary = new
        {
            TotalEvents = events.Count,
            ParsedAt = DateTime.UtcNow,
            LogSource = source,
            SourceFile = Path.GetFileName(sourceFile),
            EventsByType = events.GroupBy(e => e.EventType)
                .ToDictionary(g => g.Key, g => g.Count()),
            EventsByLevel = events.GroupBy(e => e.LogLevel)
                .ToDictionary(g => g.Key, g => g.Count()),
            PackagesFound = events.Where(e => e.PackageName != null)
                .Select(e => e.PackageNormalized)
                .Distinct().ToList(),
            TablesFound = events.Where(e => e.Details != null && e.Details.ContainsKey("table_name"))
                .Select(e => e.Details!["table_name"].ToString())
                .Distinct().ToList(),
            ErrorCount = events.Count(e => e.Derived.IsError),
            ZeroChangeCount = events.Count(e => e.Derived.IsZeroChange)
        };

        var output = new { Summary = summary, Events = events };
        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        // Ensure output directory exists
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);

        await using var stream = File.Create(outputPath);
        await JsonSerializer.SerializeAsync(stream, output, options);
    }

    // ========================================================================
    // NCA Log Parsing (existing)
    // ========================================================================

    /// <summary>
    /// Parses an NCA Replication log file (NCAReplicationLog*.txt).
    /// Kept as the original ParseFileAsync signature for backward compatibility.
    /// </summary>
    public Task<IEnumerable<LogEvent>> ParseFileAsync(string filePath)
        => ParseNcaFileAsync(filePath);

    public async Task<IEnumerable<LogEvent>> ParseNcaFileAsync(string filePath)
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
            LogSourceType = "NCA",
            TimestampIso = timestamp,
            Timestamp = new DateTimeOffset(timestamp).ToUnixTimeMilliseconds(),
            LogLevel = MapLogLevel(level),
            Message = message,
            MessageHash = ComputeHash(message),
            Metadata = new MetadataFields
            {
                SourceFile = Path.GetFileName(sourceFile),
                SourcePath = sourceFile,
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

    // ========================================================================
    // Sirus / BryteFlow Log Parsing
    // ========================================================================

    /// <summary>
    /// Parses a Sirus BryteFlow Ingest log file (sirus-*.log).
    /// </summary>
    public async Task<IEnumerable<LogEvent>> ParseSirusFileAsync(string filePath)
    {
        var events = new List<LogEvent>();
        var lineNumber = 0;
        string? currentBryteflowVersion = null;
        string? currentBryteflowBuild = null;
        string? currentSessionId = null;
        int? currentBatchNumber = null;

        // Try to extract ingest instance ID from the file's parent directory name
        var parentDir = Path.GetFileName(Path.GetDirectoryName(filePath));
        string? ingestInstance = parentDir != null && parentDir.StartsWith("ingest_")
            ? parentDir.Replace("ingest_", "")
            : null;

        await foreach (var line in File.ReadLinesAsync(filePath))
        {
            lineNumber++;
            var tsMatch = _sirusTimestampPattern.Match(line);
            if (!tsMatch.Success) continue;

            var timestampStr = tsMatch.Groups[1].Value;
            var messageRaw = tsMatch.Groups[2].Value.Trim();

            // Skip separator lines and empty messages
            if (string.IsNullOrWhiteSpace(messageRaw) || messageRaw.StartsWith("---"))
                continue;

            // Parse timestamp (e.g., "Thu Jul 24 10:02:33 UTC 2025")
            if (!TryParseSirusTimestamp(timestampStr, out var timestamp))
                continue;

            // Detect session/version info
            var versionMatch = _sirusVersionPattern.Match(messageRaw);
            if (versionMatch.Success)
            {
                currentBryteflowVersion = versionMatch.Groups[1].Value;
                currentBryteflowBuild = versionMatch.Groups[2].Value;
                currentSessionId = $"session_{timestamp:yyyyMMdd_HHmmss}";
            }

            // Track batch numbers
            var batchMatch = _sirusExtractBatchPattern.Match(messageRaw);
            if (batchMatch.Success)
                currentBatchNumber = int.Parse(batchMatch.Groups[1].Value);

            var logEvent = new LogEvent
            {
                LogSourceType = "Sirus",
                TimestampIso = timestamp,
                Timestamp = new DateTimeOffset(timestamp, TimeSpan.Zero).ToUnixTimeMilliseconds(),
                LogLevel = ClassifySirusLevel(messageRaw),
                Message = messageRaw,
                MessageHash = ComputeHash(messageRaw),
                Metadata = new MetadataFields
                {
                    SourceFile = Path.GetFileName(filePath),
                    SourcePath = filePath,
                    LineNumber = lineNumber,
                    IngestedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    BryteflowVersion = currentBryteflowVersion,
                    BryteflowBuild = currentBryteflowBuild,
                    IngestInstance = ingestInstance
                }
            };

            if (currentSessionId != null)
                logEvent.ExecutionId = currentSessionId;

            // Extract Sirus-specific fields
            ExtractSirusFields(logEvent, messageRaw, currentBatchNumber);

            // Classify Sirus event type
            ClassifySirusEvent(logEvent);

            // Derive fields
            DeriveSirusFields(logEvent);

            events.Add(logEvent);
        }

        // Assign execution IDs based on session boundaries
        AssignSirusExecutionIds(events);

        return events;
    }

    private static bool TryParseSirusTimestamp(string timestampStr, out DateTime result)
    {
        // Format: "Thu Jul 24 10:02:33 UTC 2025"
        // There can be single-digit day (e.g. "Thu Jul  4 10:02:33 UTC 2025")
        var normalized = Regex.Replace(timestampStr, @"\s+", " ").Trim();
        return DateTime.TryParseExact(
            normalized,
            new[] { "ddd MMM d HH:mm:ss UTC yyyy", "ddd MMM dd HH:mm:ss UTC yyyy" },
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
            out result);
    }

    private void ExtractSirusFields(LogEvent logEvent, string message, int? batchNumber)
    {
        logEvent.Details ??= new Dictionary<string, object>();

        if (batchNumber.HasValue)
            logEvent.Details["batch_number"] = batchNumber.Value;

        // Extract/Delta complete pattern: "Full Extract FILES.BALANCEPVO complete (595093 records)"
        var extractMatch = _sirusExtractPattern.Match(message);
        if (extractMatch.Success)
        {
            logEvent.Details["extract_type"] = extractMatch.Groups[1].Value; // Full or Delta
            logEvent.Details["table_name"] = extractMatch.Groups[2].Value;
            logEvent.PackageNormalized = extractMatch.Groups[2].Value.ToLower();

            if (extractMatch.Groups[3].Success)
            {
                var records = int.Parse(extractMatch.Groups[3].Value);
                logEvent.Details["records_extracted"] = records;
                logEvent.Derived.HasDataChanges = records > 0;
                logEvent.Derived.IsZeroChange = records == 0;
            }
            return;
        }

        // Loading table pattern: "Loading table xxc_isw.BALANCEPVO with 595,093 records(345,934,917 bytes)"
        var loadMatch = _sirusLoadingPattern.Match(message);
        if (loadMatch.Success)
        {
            var fullTableName = loadMatch.Groups[1].Value;
            var tableName = fullTableName.Contains('.')
                ? fullTableName.Split('.')[1]
                : fullTableName;
            logEvent.Details["table_name"] = tableName;
            logEvent.Details["records_loading"] = int.Parse(loadMatch.Groups[2].Value.Replace(",", ""));
            logEvent.Details["bytes_loading"] = long.Parse(loadMatch.Groups[3].Value.Replace(",", ""));
            logEvent.Details["load_method"] = loadMatch.Groups[4].Value;
            logEvent.PackageNormalized = tableName.ToLower();
            return;
        }

        // Stats pattern: "Info(RL872): Table BALANCEPVO Stats(Src=595093/0/0 Dst=595093/0/0)"
        var statsMatch = _sirusStatsPattern.Match(message);
        if (statsMatch.Success)
        {
            var tableName = statsMatch.Groups[1].Value;
            logEvent.Details["table_name"] = tableName;
            logEvent.Details["src_records"] = int.Parse(statsMatch.Groups[2].Value);
            logEvent.Details["src_inserts"] = int.Parse(statsMatch.Groups[3].Value);
            logEvent.Details["src_updates"] = int.Parse(statsMatch.Groups[4].Value);
            logEvent.Details["dst_records"] = int.Parse(statsMatch.Groups[5].Value);
            logEvent.Details["dst_inserts"] = int.Parse(statsMatch.Groups[6].Value);
            logEvent.Details["dst_updates"] = int.Parse(statsMatch.Groups[7].Value);
            logEvent.PackageNormalized = tableName.ToLower();

            var srcRecords = int.Parse(statsMatch.Groups[2].Value);
            logEvent.Derived.HasDataChanges = srcRecords > 0;
            logEvent.Derived.IsZeroChange = srcRecords == 0;
            return;
        }

        // Loaded table pattern: "Loaded table xxc_isw.BALANCEPVO(0 of 1 left)"
        var loadedMatch = _sirusLoadedTablePattern.Match(message);
        if (loadedMatch.Success)
        {
            var fullTableName = loadedMatch.Groups[1].Value;
            var tableName = fullTableName.Contains('.')
                ? fullTableName.Split('.')[1]
                : fullTableName;
            logEvent.Details["table_name"] = tableName;
            logEvent.Details["tables_remaining"] = int.Parse(loadedMatch.Groups[2].Value);
            logEvent.Details["tables_total"] = int.Parse(loadedMatch.Groups[3].Value);
            logEvent.PackageNormalized = tableName.ToLower();
            return;
        }

        // Creating/Created table
        if (message.Contains("Creating table") || message.Contains("Created table"))
        {
            var tableMatch = Regex.Match(message, @"(?:Creating|Created) table (\w+\.\w+)");
            if (tableMatch.Success)
            {
                var tableName = tableMatch.Groups[1].Value.Split('.')[1];
                logEvent.Details["table_name"] = tableName;
                logEvent.PackageNormalized = tableName.ToLower();
            }
        }
    }

    private void ClassifySirusEvent(LogEvent logEvent)
    {
        var message = logEvent.Message.ToLower();

        logEvent.EventType = message switch
        {
            var m when m.Contains("running bryteflow") => "session_start",
            var m when m.Contains("tomcat started") => "session_ready",
            var m when Regex.IsMatch(m, @"^\s*extracting \d+") => "batch_extract_start",
            var m when Regex.IsMatch(m, @"^\s*extracted \d+") => "batch_extract_end",
            var m when m.Contains("full extract") && m.Contains("complete") => "table_full_extract",
            var m when m.Contains("delta extract") && m.Contains("complete") => "table_delta_extract",
            var m when m.Contains("full extract") && !m.Contains("complete") => "table_full_extract_start",
            var m when m.Contains("delta extract") && !m.Contains("complete") => "table_delta_extract_start",
            var m when Regex.IsMatch(m, @"^\s*load file \d+") => "batch_load_start",
            var m when Regex.IsMatch(m, @"^\s*loaded file \d+") => "batch_load_end",
            var m when m.Contains("loading table") => "table_load_start",
            var m when m.Contains("info(rl872)") => "table_load_stats",
            var m when m.Contains("loaded table") => "table_load_complete",
            var m when m.Contains("submitted process") => "table_load_submit",
            var m when m.Contains("creating table") => "table_create_start",
            var m when m.Contains("created table") => "table_create_complete",
            var m when m.Contains("checking table") => "table_staging_check",
            var m when m.Contains("created new connection") => "connection_create",
            var m when m.Contains("reusing connection") => "connection_reuse",
            var m when m.Contains("exit command") => "session_exit",
            var m when m.Contains("exiting program") => "session_shutdown",
            var m when m.Contains("licence expired") => "license_error",
            var m when m.Contains("error(") => "error",
            _ => logEvent.LogLevel == "ERROR" ? "error" : "info"
        };
    }

    private static string ClassifySirusLevel(string message)
    {
        if (message.Contains("Error(", StringComparison.OrdinalIgnoreCase)) return "ERROR";
        if (message.Contains("Licence expired", StringComparison.OrdinalIgnoreCase)) return "WARNING";
        if (message.Contains("No driver for", StringComparison.OrdinalIgnoreCase)) return "WARNING";
        return "INFO";
    }

    private static void DeriveSirusFields(LogEvent logEvent)
    {
        logEvent.Derived.IsError = logEvent.LogLevel == "ERROR";

        // Zero-change detection already handled in ExtractSirusFields
        // for extract and stats events
    }

    private static void AssignSirusExecutionIds(List<LogEvent> events)
    {
        string? currentSessionId = null;

        foreach (var evt in events)
        {
            if (evt.EventType == "session_start")
            {
                var date = evt.TimestampIso.ToString("yyyyMMdd");
                var time = evt.TimestampIso.ToString("HHmmss");
                currentSessionId = $"sirus_session_{date}_{time}";
            }

            if (currentSessionId != null)
                evt.ExecutionId = currentSessionId;
        }
    }
}

// ========================================================================
// Batch Processing Result
// ========================================================================

public class BatchResult
{
    public int NcaFilesFound { get; set; }
    public int SirusFilesFound { get; set; }
    public int UnknownFilesFound { get; set; }
    public int NcaFilesProcessed { get; set; }
    public int SirusFilesProcessed { get; set; }
    public int TotalEventsProcessed { get; set; }
    public List<string> Errors { get; set; } = new();
}
