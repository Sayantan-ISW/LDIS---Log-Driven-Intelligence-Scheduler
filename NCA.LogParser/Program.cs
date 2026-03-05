using System.Text.Json;
using NCA.LogParser;

Console.WriteLine("=== NCA Log Parser ===");
Console.WriteLine();

// Get input file path
string inputFilePath;
if (args.Length > 0)
{
    inputFilePath = args[0];
}
else
{
    Console.Write("Enter the path to the NCA log file: ");
    inputFilePath = Console.ReadLine()?.Trim('"') ?? string.Empty;
}

if (string.IsNullOrWhiteSpace(inputFilePath) || !File.Exists(inputFilePath))
{
    Console.WriteLine("Error: File not found or invalid path.");
    return;
}

// Generate output file path
var inputFileName = Path.GetFileNameWithoutExtension(inputFilePath);
var outputDirectory = Path.GetDirectoryName(inputFilePath) ?? Directory.GetCurrentDirectory();
var outputFilePath = Path.Combine(outputDirectory, $"{inputFileName}_Structured.json");

Console.WriteLine($"Input File:  {inputFilePath}");
Console.WriteLine($"Output File: {outputFilePath}");
Console.WriteLine();
Console.WriteLine("Processing...");

try
{
    // Parse the log file
    var parser = new LogParserService();
    var events = await parser.ParseFileAsync(inputFilePath);
    
    var eventsList = events.ToList();
    Console.WriteLine($"Parsed {eventsList.Count} events");

    // Generate summary statistics
    var summary = new
    {
        TotalEvents = eventsList.Count,
        ParsedAt = DateTime.UtcNow,
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
    Console.WriteLine("✓ Processing complete!");
    Console.WriteLine();
    Console.WriteLine("=== Summary ===");
    Console.WriteLine($"Total Events:     {summary.TotalEvents}");
    Console.WriteLine($"Error Events:     {summary.ErrorCount}");
    Console.WriteLine($"Zero-Change:      {summary.ZeroChangeCount}");
    Console.WriteLine();
    Console.WriteLine("Events by Type:");
    foreach (var (type, count) in summary.EventsByType.OrderByDescending(x => x.Value))
    {
        Console.WriteLine($"  {type,-20} {count,6}");
    }
    Console.WriteLine();
    Console.WriteLine("Events by Level:");
    foreach (var (level, count) in summary.EventsByLevel.OrderByDescending(x => x.Value))
    {
        Console.WriteLine($"  {level,-20} {count,6}");
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
