import os
import findspark
from pyspark.sql import SparkSession

def init_spark():
    try:
        # Explicit environment setup
        os.environ["JAVA_HOME"] = r"C:\\Program Files\\Eclipse Adoptium\\jdk-21.0.7.6-hotspot"
        os.environ["HADOOP_HOME"] = r"C:\\hadoop"
        os.environ["SPARK_HOME"] = r"C:\\Users\\shiva\\Documents\\noaa-ais-pipeline\\.venv\\Lib\\site-packages\\pyspark"
        os.environ["PATH"] += os.pathsep + os.path.join(os.environ["SPARK_HOME"], "bin")
        os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

        # Initialize Spark (findspark ensures PySpark finds its jars)
        findspark.init(os.environ["SPARK_HOME"])

        spark = SparkSession.builder \
            .appName("TestPySpark") \
            .master("local[*]") \
            .config("spark.driver.host", "127.0.0.1") \
            .getOrCreate()

        print("Spark initialized successfully")
        print(f"Spark version: {spark.version}")
        return spark

    except Exception as e:
        print(f"Error initializing Spark: {e}")
        return None

if __name__ == '__main__':
    spark = init_spark()
    if spark:
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        df.show()
        spark.stop()