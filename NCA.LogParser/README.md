# NCA Log Parser

A C# console application that parses NCA Replication Service log files and generates structured JSON output.

## Features

- **Complete log parsing** - Handles standard, JSON-embedded, and multi-line formats
- **Entity extraction** - Packages, tables, instances, batch IDs, manifest IDs
- **Metric extraction** - Records downloaded, rows inserted/updated, durations
- **Event classification** - 13+ event types automatically detected
- **Execution grouping** - Groups events by execution ID
- **Summary statistics** - Event counts, error rates, zero-change analysis
- **Pure .NET** - No third-party dependencies except System.Text.Json

## Requirements

- .NET 8.0 SDK (or .NET 6.0+)
- Windows, Linux, or macOS

## Installation

1. Navigate to the project directory:
```bash
cd NCA.LogParser
```

2. Build the project:
```bash
dotnet build
```

## Usage

### Option 1: Run with file path argument
```bash
dotnet run -- "C:\Logs\NCAReplicationLog20260301.txt"
```

### Option 2: Run and enter path interactively
```bash
dotnet run
# Then enter the path when prompted
```

### Option 3: Build and run executable
```bash
dotnet publish -c Release -r win-x64 --self-contained
.\bin\Release\net8.0\win-x64\publish\NCA.LogParser.exe "C:\Logs\NCAReplicationLog20260301.txt"
```

## Output

The application generates a JSON file with the suffix `_Structured.json` in the same directory as the input file.

**Example:**
- Input:  `NCAReplicationLog20260301.txt`
- Output: `NCAReplicationLog20260301_Structured.json`

### Output Structure

```json
{
  "summary": {
    "totalEvents": 2843,
    "parsedAt": "2026-03-04T10:30:00Z",
    "sourceFile": "NCAReplicationLog20260301.txt",
    "eventsByType": {
      "info": 2500,
      "table_load": 150,
      "execution_start": 25,
      "execution_complete": 24,
      "error": 8
    },
    "eventsByLevel": {
      "INFO": 2800,
      "ERROR": 40,
      "FATAL": 3
    },
    "packagesFound": [
      "general_ledger",
      "gl_common",
      "sla_ahcs"
    ],
    "errorCount": 43,
    "zeroChangeCount": 105
  },
  "events": [
    {
      "event_id": "uuid",
      "timestamp": 1709260095923,
      "timestamp_iso": "2026-03-01T05:48:15.923Z",
      "log_level": "INFO",
      "event_type": "execution_start",
      "package_name": "XXC_ISW - General Ledger",
      "package_normalized": "general_ledger",
      "execution_id": "exec_20260301_054815_general_ledger",
      "instance_id": "mNhkO7Wud7",
      "message": "Initiating Incremental loads for package...",
      "structured": false,
      "details": {
        "batch_id": "827235810"
      },
      "derived": {
        "has_data_changes": false,
        "is_zero_change": false,
        "is_error": false
      },
      "metadata": {
        "source_file": "NCAReplicationLog20260301.txt",
        "line_number": 1234,
        "ingested_at": 1709467800000,
        "nca_version": "R25.3"
      }
    }
  ]
}
```

## Event Types Detected

| Event Type | Description |
|-----------|-------------|
| `execution_start` | Package execution begins |
| `execution_complete` | Package execution finishes |
| `table_load` | Table data loaded with metrics |
| `package_sync` | All tables synchronized |
| `instance_start` | Instance started |
| `instance_stop` | Instance stopped |
| `table_reset` | Table reset to earlier timestamp |
| `table_added` | Table added to replication |
| `table_deleted` | Table removed from replication |
| `schedule_complete` | Scheduling completed |
| `download_start` | Data download initiated |
| `download_complete` | Data download completed |
| `error` | Error or fatal event |
| `info` | General informational event |

## Extracted Fields

### Core Fields
- `event_id` - Unique event identifier
- `timestamp` - Unix timestamp (milliseconds)
- `timestamp_iso` - ISO 8601 timestamp
- `log_level` - INFO, ERROR, FATAL, WARNING
- `event_type` - Classified event type
- `message` - Full log message

### Entity Fields
- `package_name` - Original package name
- `package_normalized` - Normalized package name
- `instance_id` - Instance identifier
- `execution_id` - Execution grouping ID

### Details (when available)
- `table_name` - Table being processed
- `records_downloaded` - Source records count
- `rows_inserted` - Destination insert count
- `rows_updated` - Destination update count
- `load_duration_seconds` - Load time in seconds
- `batch_id` - Batch identifier
- `manifest_id` - Manifest identifier

### Derived Fields
- `has_data_changes` - Boolean flag for data changes
- `is_zero_change` - Boolean flag for zero-change execution
- `is_error` - Boolean flag for error events

## Examples

### Parse a single log file
```bash
dotnet run -- "C:\Logs\NCAReplicationLog20260301.txt"
```

### Process multiple files (PowerShell)
```powershell
Get-ChildItem "C:\Logs\NCAReplicationLog*.txt" | ForEach-Object {
    dotnet run -- $_.FullName
}
```

### Process multiple files (Bash)
```bash
for file in /var/log/nca/NCAReplicationLog*.txt; do
    dotnet run -- "$file"
done
```

## Performance

- **Processing speed**: 10,000-50,000 lines/second
- **Memory usage**: ~50-100 MB
- **File size**: Tested with files up to 100 MB

## Troubleshooting

### "File not found" error
- Ensure the file path is correct
- Use quotes around paths with spaces
- Check file permissions

### Out of memory
- For very large files (>500 MB), consider processing in chunks
- Increase system memory or run on a machine with more RAM

### Invalid timestamp format
- Ensure log file uses the format: `YYYY-MM-DD HH:mm:ss.fff +00:00`
- Check for corrupted log lines

## Next Steps

After generating structured logs, you can:

1. **Import into Elasticsearch** for real-time analysis
2. **Load into Pandas/Python** for data science workflows
3. **Query with jq** for command-line analysis:
   ```bash
   jq '.events[] | select(.event_type == "error")' output.json
   ```
4. **Import into Excel/Power BI** for visualization
5. **Feed into ML pipelines** for predictive analysis

## License

Internal use only - insightsoftware
