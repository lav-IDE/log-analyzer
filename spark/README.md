# Spark Log Analysis

This module performs analysis on distributed logs using Apache Spark.

## Setup

### Prerequisites
- Apache Spark 2.4+
- Apache Hadoop installed and configured
- Python 3.7+
- PySpark library

### Configuration

1. Copy the example configuration:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and set the appropriate values for your environment:
   - `HADOOP_CONF_DIR`: Path to your Hadoop configuration directory
   - `HDFS_NAMENODE`: Your HDFS namenode address (e.g., `hdfs://namenode-host:9000`)
   - `LOG_INPUT_PATH`: HDFS path where your logs are stored
   - `SPARK_LOCAL_IP`: Local IP address (default: 127.0.0.1)
   - `SPARK_JARS_PATH`: HDFS path to Spark JARs archive

3. Load the environment variables before running:
   ```bash
   export $(cat .env | xargs)
   python analysis.py
   ```

## Running the Analysis

```bash
python analysis.py
```

The script will:
1. Read logs from the specified HDFS path
2. Parse and structure the log data
3. Generate error analysis by server
4. Identify potential memory leaks

## Output

The script outputs:
- **Error count by Server**: Count of ERROR level logs per server
- **Memory Leak Analysis**: Servers with memory usage exceeding the average

## Notes

- Ensure HADOOP_CONF_DIR is correctly set for Hadoop to function properly
- The HDFS paths should be accessible from your Spark cluster
- This script requires YARN to be configured and running
