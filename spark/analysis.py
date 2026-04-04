"""

Configuration:
    - SPARK_LOCAL_IP: IP address for Spark (default: 127.0.0.1)
    - SPARK_HOME: Path to Spark installation (auto-detected)
    - HADOOP_CONF_DIR: Path to Hadoop configuration directory (default: /opt/hadoop/etc/hadoop)
    - HDFS_NAMENODE: HDFS namenode address (default: hdfs://master:9000)
    - LOG_INPUT_PATH: HDFS path to input logs (default: /raw_logs/generated_logs)
    - SPARK_JARS_PATH: Optional HDFS archive path used for spark.yarn.archive

Set these environment variables before running:
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    export HDFS_NAMENODE=hdfs://master:9000
    export LOG_INPUT_PATH=/raw_logs/generated_logs
"""

from pyspark.sql import SparkSession
import pyspark
import os
import pyspark.sql.functions as F


def get_config(key, default):
    return os.environ.get(key, default)


# Configuration from environment variables
SPARK_LOCAL_IP = get_config("SPARK_LOCAL_IP", "127.0.0.1")
HADOOP_CONF_DIR = get_config("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
HDFS_NAMENODE = get_config("HDFS_NAMENODE", "hdfs://master:9000")
LOG_INPUT_PATH = get_config("LOG_INPUT_PATH", "/raw_logs/generated_logs")
SPARK_JARS_PATH = get_config("SPARK_JARS_PATH", "").strip()

# Set required environment variables
os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ["HADOOP_CONF_DIR"] = HADOOP_CONF_DIR

# Build the full HDFS paths
hdfs_logs = f"{HDFS_NAMENODE}{LOG_INPUT_PATH}"

# Initialize Spark Session
spark_builder = SparkSession \
    .builder \
    .appName("Log Analysis") \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "client")

if SPARK_JARS_PATH:
    spark_builder = spark_builder.config("spark.yarn.archive", f"{HDFS_NAMENODE}{SPARK_JARS_PATH}")

spark = spark_builder.getOrCreate()


logs = spark.read.text(hdfs_logs)

split_col = F.split(logs["value"], r" \| ")

structured_logs = logs \
    .withColumn("Time stamp", split_col.getItem(0)) \
    .withColumn("Server", split_col.getItem(1)) \
    .withColumn("Level", split_col.getItem(2)) \
    .withColumn("Response Time (ms)", F.regexp_extract(split_col.getItem(3), r"\d+\.?\d*", 0).cast("integer")) \
    .withColumn("CPU (%)", F.regexp_extract(split_col.getItem(4), r"\d+\.?\d*", 0).cast("integer")) \
    .withColumn("Memory (GB)", F.regexp_extract(split_col.getItem(5), r"\d+\.?\d*", 0).cast("double")) \
    .withColumn("Requests", F.regexp_extract(split_col.getItem(6), r"\d+\.?\d*", 0).cast("integer")) \
    .drop("value")
    

structured_logs.createOrReplaceTempView("server_logs")

print("\n ------ Error count by Server ------")
error_analysis = spark.sql("""
    SELECT Server, COUNT(*) as Total_errors
    FROM server_logs
    WHERE Level = "ERROR"
    GROUP BY Server
    ORDER BY Total_errors DESC
                           """)

error_analysis.show()

print("\n ----- Memory Leak -----")
memory_leak = spark.sql("""
    SELECT Server, Count(*) AS times_over_avg, MAX(`Memory (GB)`)
    FROM server_logs
    WHERE `Memory (GB)` > (SELECT AVG(`Memory (GB)`) FROM server_logs)
    GROUP BY Server
    ORDER BY times_over_avg DESC
    """)

memory_leak.show()