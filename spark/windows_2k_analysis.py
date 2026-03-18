from pyspark.sql import SparkSession
import pyspark
import os
import pyspark.sql.functions as F


def get_config(key, default):
    return os.environ.get(key, default)


# Configuration from environment variables
SPARK_LOCAL_IP = get_config("SPARK_LOCAL_IP", "127.0.0.1")
HADOOP_CONF_DIR = get_config("HADOOP_CONF_DIR", "hadoop")
HDFS_NAMENODE = get_config("HDFS_NAMENODE", "hdfs://127.0.0.1:9000")
LOG_INPUT_PATH = get_config("LOG_INPUT_PATH", "/loghub_windows2k_data/Windows_2k.log_structured.csv")
SPARK_JARS_PATH = get_config("SPARK_JARS_PATH", "/spark-jars/spark-jars.zip")

# Set required environment variables
os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ["HADOOP_CONF_DIR"] = HADOOP_CONF_DIR

# Build the full HDFS paths
hdfs_spark_jars = f"{HDFS_NAMENODE}{SPARK_JARS_PATH}"
hdfs_logs = f"{HDFS_NAMENODE}{LOG_INPUT_PATH}"

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Log Analysis") \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.archive", hdfs_spark_jars) \
    .getOrCreate()
    

windows_df = spark.read \
        .option("header", "True") \
        .option("inferSchema", "True") \
        .csv(hdfs_logs)

windows_df.createOrReplaceTempView("windows_logs")

work_dir = os.getcwd()

def export_query(query_sql, export_name):
    """runs a Spark SQL query and saves it directly to the local data folder."""
    print(f"running query: {export_name}...")
    result_df = spark.sql(query_sql)
    
    folder_path = os.path.join(work_dir, "data", "processed_data", f"{export_name}.parquet")
    output_path = f"file://{folder_path}"
    result_df.write.mode("overwrite").parquet(output_path)
    print(f"Successfully bridged: {export_name}.parquet\n")

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