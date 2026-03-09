using System.Text.Json;
using NCA.LogParser;

Console.WriteLine("=== NCA / Sirus Log Parser ===");
Console.WriteLine();

// Determine mode: folder (batch) or single file
string inputPath;
if (args.Length > 0)
{
    inputPath = args[0];
}
else
{
    Console.Write("Enter the path to a log file or Logs folder: ");
    inputPath = Console.ReadLine()?.Trim('"') ?? string.Empty;
}

if (string.IsNullOrWhiteSpace(inputPath))
{
    Console.WriteLine("Error: No path provided.");
    return;
}

var parser = new LogParserService();

// ── Batch folder mode ──
if (Directory.Exists(inputPath))
{
    var outputDirectory = Path.Combine(
        Path.GetDirectoryName(inputPath) ?? Directory.GetCurrentDirectory(),
        "Logs_Structured");

    Console.WriteLine($"Input Folder:  {inputPath}");
    Console.WriteLine($"Output Folder: {outputDirectory}");
    Console.WriteLine();

    var result = await parser.ProcessFolderAsync(
        inputPath,
        outputDirectory,
        msg => Console.WriteLine($"  {msg}"));

    Console.WriteLine();
    Console.WriteLine("=== Batch Summary ===");
    Console.WriteLine($"  NCA files found:       {result.NcaFilesFound}");
    Console.WriteLine($"  NCA files processed:   {result.NcaFilesProcessed}");
    Console.WriteLine($"  Sirus files found:     {result.SirusFilesFound}");
    Console.WriteLine($"  Sirus files processed: {result.SirusFilesProcessed}");
    Console.WriteLine($"  Unknown files:         {result.UnknownFilesFound}");
    Console.WriteLine($"  Total events:          {result.TotalEventsProcessed}");

    if (result.Errors.Any())
    {
        Console.WriteLine();
        Console.WriteLine($"  Errors ({result.Errors.Count}):");
        foreach (var err in result.Errors)
            Console.WriteLine($"    - {err}");
    }

    Console.WriteLine();
    Console.WriteLine($"Structured output written to: {outputDirectory}");
    return;
}

// ── Single file mode (backward compatible) ──
if (!File.Exists(inputPath))
{
    Console.WriteLine("Error: File not found or invalid path.");
    return;
}

var inputFilePath = inputPath;

// Auto-detect log type
var logSource = await LogParserService.ClassifyFileAsync(inputFilePath);
Console.WriteLine($"Detected log type: {logSource}");

// Generate output file path
var inputFileName = Path.GetFileNameWithoutExtension(inputFilePath);
var outputDir = Path.GetDirectoryName(inputFilePath) ?? Directory.GetCurrentDirectory();
var outputFilePath = Path.Combine(outputDir, $"{inputFileName}_Structured.json");

Console.WriteLine($"Input File:  {inputFilePath}");
Console.WriteLine($"Output File: {outputFilePath}");
Console.WriteLine();
Console.WriteLine("Processing...");

try
{
    List<LogEvent> eventsList;

    if (logSource == LogSource.Sirus)
    {
        var events = await parser.ParseSirusFileAsync(inputFilePath);
        eventsList = events.ToList();
    }
    else
    {
        // Default to NCA for NCA and Unknown types
        var events = await parser.ParseFileAsync(inputFilePath);
        eventsList = events.ToList();
    }

    Console.WriteLine($"Parsed {eventsList.Count} events");

    // Generate summary statistics
    var summary = new
    {
        TotalEvents = eventsList.Count,
        ParsedAt = DateTime.UtcNow,
        LogSource = logSource.ToString(),
        SourceFile = Path.GetFileName(inputFilePath),
        EventsByType = eventsList.GroupBy(e => e.EventType)
            .ToDictionary(g => g.Key, g => g.Count()),
        EventsByLevel = eventsList.GroupBy(e => e.LogLevel)
            .ToDictionary(g => g.Key, g => g.Count()),
        PackagesFound = eventsList.Where(e => e.PackageName != null)
            .Select(e => e.PackageNormalized)
            .Distinct()
            .ToList(),
        ErrorCount = eventsList.Count(e => e.Derived.IsError),
        ZeroChangeCount = eventsList.Count(e => e.Derived.IsZeroChange)
    };

    // Create output structure
    var output = new
    {
        Summary = summary,
        Events = eventsList
    };

    // Write to JSON file
    var options = new JsonSerializerOptions
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    await using var outputStream = File.Create(outputFilePath);
    await JsonSerializer.SerializeAsync(outputStream, output, options);

    Console.WriteLine();
    Console.WriteLine("Processing complete!");
    Console.WriteLine();
    Console.WriteLine("=== Summary ===");
    Console.WriteLine($"Log Source:       {logSource}");
    Console.WriteLine($"Total Events:     {summary.TotalEvents}");
    Console.WriteLine($"Error Events:     {summary.ErrorCount}");
    Console.WriteLine($"Zero-Change:      {summary.ZeroChangeCount}");
    Console.WriteLine();
    Console.WriteLine("Events by Type:");
    foreach (var (type, count) in summary.EventsByType.OrderByDescending(x => x.Value))
    {
        Console.WriteLine($"  {type,-25} {count,6}");
    }
    Console.WriteLine();
    Console.WriteLine("Events by Level:");
    foreach (var (level, count) in summary.EventsByLevel.OrderByDescending(x => x.Value))
    {
        Console.WriteLine($"  {level,-25} {count,6}");
    }
    
    if (summary.PackagesFound.Any())
    {
        Console.WriteLine();
        Console.WriteLine("Packages Found:");
        foreach (var package in summary.PackagesFound)
        {
            Console.WriteLine($"  - {package}");
        }
    }

    Console.WriteLine();
    Console.WriteLine($"Structured output written to: {outputFilePath}");
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
    Console.WriteLine(ex.StackTrace);
}
