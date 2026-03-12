# Log Analyzer v1

A distributed log analysis system using Apache Spark and Hadoop to analyze server logs, identify errors, detect memory leaks

## Project Structure

```
log_analyzer/
├── log_generator/        # Log file generator
│   └── log_generator.py  # Generate sample server logs
├── spark/                # Spark analysis module
│   └── analysis.py       # Analyze logs using Spark
├── dashboard/            # Web dashboard for visualization
├── data/                 # Data storage
│   └── raw_logs/         # Raw log files
├── hadoop-project/       # Hadoop distribution
└── scripts/              # Utility scripts
```

## Features

- **Log Generation**: Generate realistic server logs with configurable parameters
- **Distributed Analysis**: Process logs at scale using Apache Spark and Hadoop
- **Error Tracking**: Identify and count ERROR level events by server
- **Memory Leak Detection**: Detect servers with memory usage exceeding average

## Requirements

- Python 3.7+
- Apache Spark 2.4+
- Apache Hadoop (included in hadoop-project/)
- YARN cluster (for distributed execution)

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Generate Sample Logs

```bash
python log_generator/log_generator.py
```

This generates 5 sample log files with 250,000 entries each in `data/raw_logs/generated_logs/`.

Log format:
```
timestamp | server | level | Response Time = Xms | CPU=X% | Memory=X.XGB | Requests=X
```

### Run Log Analysis

Navigate to the spark folder and configure your environment:

```bash
cd spark/
python analysis.py
```

The analysis script will:
1. Read logs from HDFS
2. Parse and structure log data
3. Perform error analysis by server
4. Detect potential memory leaks

### View the Dashboard

```bash
# From the dashboard folder
python app.py
```

## Configuration

See individual module READMEs for configuration options:
- [Spark Analysis Configuration](spark/README.md)

## License

MIT