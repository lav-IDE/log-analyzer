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

summary_df = spark.sql('''
    SELECT Component, COUNT(*) as Total_Logs
    FROM windows_logs
    GROUP BY Component
    ORDER BY Total_Logs DESC              
                       ''')

    

output_path = "file:///mnt/d/programs/projects/log_analyzer/data/processed_data/component_summary.parquet"

summary_df.write \
    .mode("overwrite") \
    .parquet(output_path)
    
print(f"data summarized and bridged to {output_path}")