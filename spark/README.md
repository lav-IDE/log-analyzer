# Spark Log Analysis

This module performs analysis on distributed logs using Apache Spark.

## Setup

### Prerequisites
- Dockerized Hadoop cluster running from `hadoop-project/docker-compose.yml`
- Python 3.7+
- PySpark library

### Configuration

1. Copy the example configuration:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and set the appropriate values for your environment:
   - `HADOOP_CONF_DIR`: Path to Hadoop configuration directory (default in container image: `/opt/hadoop/etc/hadoop`)
   - `HDFS_NAMENODE`: HDFS namenode address (for this project: `hdfs://master:9000`)
   - `LOG_INPUT_PATH`: HDFS path where your logs are stored
   - `SPARK_LOCAL_IP`: Local IP address (default: 127.0.0.1)
   - `SPARK_JARS_PATH`: Optional HDFS path to Spark JARs archive (can be empty)

3. Load the environment variables before running:
   ```bash
   export $(cat .env | xargs)
   python analysis.py
   ```

4. Ensure the cluster and HDFS inputs are ready (run from repository root):
   ```bash
   bash scripts/start_cluster.sh
   bash scripts/upload_logs.sh
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


## Analysis v2

```bash
python spark/windows_2k_analysis.py
streamlit run dashboard/app.py
```

The scripts will:
1. Read logs from the specified HDFS path
2. Run Spark SQL queries on the data
3. Convert the output into .parquet files
4. Show a streamlit dashboard of the analysis of 2000 Windows Log Records


## Output

The script outputs:
- **The Component Summary**: Count of CBS/CSI Components
- **The Level Summary**: Count of the levels (INFO/WARN/ERROR)
- **Server Actions**: Top 10 most frequent actions the server takes


## Notes

- Ensure HADOOP_CONF_DIR is correctly set for Hadoop to function properly
- The HDFS paths should be accessible from your Spark cluster
- This script requires YARN to be configured and running
