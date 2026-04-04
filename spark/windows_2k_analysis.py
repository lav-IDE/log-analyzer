from pyspark.sql import SparkSession
import pyspark
import os
import shutil
import subprocess
import pyspark.sql.functions as F


def get_config(key, default):
    return os.environ.get(key, default)


# Configuration from environment variables
SPARK_LOCAL_IP = get_config("SPARK_LOCAL_IP", "127.0.0.1")
HADOOP_CONF_DIR = get_config("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
HDFS_NAMENODE = get_config("HDFS_NAMENODE", "hdfs://master:9000")
LOG_INPUT_PATH = get_config("LOG_INPUT_PATH", "/logs/windows/Windows.log")
MAX_INPUT_ROWS = int(get_config("MAX_INPUT_ROWS", "0"))
HDFS_OUTPUT_BASE = get_config("HDFS_OUTPUT_BASE", "/processed_data")
LOCAL_OUTPUT_BASE = get_config("LOCAL_OUTPUT_BASE", "/output" if os.path.isdir("/output") else os.path.join(os.getcwd(), "data", "processed_data"))
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

# Read as raw text and parse with regex
raw_df = spark.read.text(hdfs_logs)
if MAX_INPUT_ROWS > 0:
    raw_df = raw_df.limit(MAX_INPUT_ROWS)

# Pattern: Date, Time, Level, Component, Content
# Example: "2016-09-28 04:30:31, Info                  CBS    SQM: ..."
parsed_df = raw_df.select(
    F.regexp_extract('value', r'^(\d{4}-\d{2}-\d{2})', 1).alias('Date'),
    F.regexp_extract('value', r'^\d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2})', 1).alias('Time'),
    F.regexp_extract('value', r',\s*(Info|Warning|Error|Debug|Critical|Verbose)', 1).alias('Level'),
    F.regexp_extract('value', r'(CBS|CSI)', 1).alias('Component'),
    F.regexp_extract('value', r'(?:CBS|CSI)\s+(.*)', 1).alias('Content')
).filter(F.col('Date') != '')

# Build a lightweight template column by normalizing IDs, numbers and hex-like values.
parsed_df = parsed_df.withColumn(
    "EventTemplate",
    F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("Content"), r"0x[0-9A-Fa-f]+", "<HEX>"),
                r"\b\d+\b",
                "<NUM>"
            ),
            r"@[0-9A-Fa-f]{6,}",
            "@<ID>"
        )
    )
)

parsed_df.createOrReplaceTempView("windows_logs")


import time

def export_query(query_sql_or_df, export_name):
    """Runs a Spark SQL query or accepts a DataFrame and saves it to the local data folder."""
    print(f"running query: {export_name}...")
    start = time.time()

    if isinstance(query_sql_or_df, str):
        result_df = spark.sql(query_sql_or_df)
    else:
        result_df = query_sql_or_df

    if not result_df.columns:
        raise ValueError(f"{export_name} produced no columns; check parser and query logic")

    hdfs_folder = f"{HDFS_NAMENODE}{HDFS_OUTPUT_BASE}/{export_name}.parquet"
    result_df.write.mode("overwrite").parquet(hdfs_folder)

    os.makedirs(LOCAL_OUTPUT_BASE, exist_ok=True)
    local_folder = os.path.join(LOCAL_OUTPUT_BASE, f"{export_name}.parquet")
    if os.path.exists(local_folder):
        shutil.rmtree(local_folder)

    subprocess.run(
        ["hdfs", "dfs", "-get", "-f", f"{HDFS_OUTPUT_BASE}/{export_name}.parquet", LOCAL_OUTPUT_BASE],
        check=True,
    )

    elapsed = time.time() - start
    print(f"Successfully bridged: {export_name}.parquet — ⏱ {elapsed:.2f}s\n")


# query 1: The Component Summary
export_query("""
    SELECT Component, COUNT(*) as Total_Logs
    FROM windows_logs
    GROUP BY Component
    ORDER BY Total_Logs DESC
""", "1_component_volume")

# query 2: The Level Summary (Info vs Warning vs Error)
export_query("""
    SELECT Level, COUNT(*) as Total_Logs
    FROM windows_logs
    GROUP BY Level
    ORDER BY Total_Logs DESC
""", "2_severity_levels")

# query 3: Top 10 most frequent actions the server takes
export_query("""
    SELECT EventTemplate, COUNT(*) as Actions
    FROM windows_logs
    GROUP BY EventTemplate
    ORDER BY Actions DESC
    LIMIT 10
""", "3_most_frequent_actions")


# query 4: Hourly time window features for anomaly detection
# query 4: Hourly time window features for anomaly detection
anomaly_df = parsed_df \
    .withColumn("Timestamp", F.to_timestamp(F.concat_ws(" ", F.col("Date"), F.col("Time")), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Hour_Window", F.date_trunc("hour", F.col("Timestamp"))) \
    .filter(F.col("Timestamp").isNotNull()) \
    .groupBy("Hour_Window") \
    .agg(
        F.count("*").alias("Total_Logs"),
        F.count(F.when(F.col("Component") == "CBS", 1)).alias("CBS_Count"),
        F.count(F.when(F.col("Component") == "CSI", 1)).alias("CSI_Count"),
        F.countDistinct("EventTemplate").alias("Unique_Templates"),
        F.countDistinct("Component").alias("Active_Components"),
        F.round(
            F.count(F.when(F.col("Component") == "CSI", 1)).cast("double") / F.count("*"), 4
        ).alias("CSI_Ratio")
    ) \
    .orderBy("Hour_Window")

export_query(anomaly_df, "4_anomaly_features")



print(f"\n{'='*50}")
print(f"Executors: {get_config('SPARK_NUM_EXECUTORS', '2')}")
print(f"{'='*50}")