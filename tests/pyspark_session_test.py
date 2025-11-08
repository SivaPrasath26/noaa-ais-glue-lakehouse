import os
import findspark
from pyspark.sql import SparkSession

def init_spark():
    try:
        # Ensure required environment variables are set
        required_env_vars = ["JAVA_HOME", "HADOOP_HOME", "SPARK_HOME"]
        missing = [var for var in required_env_vars if var not in os.environ]
        if missing:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

        # Extend PATH for Spark and Hadoop
        os.environ["PATH"] += os.pathsep + os.path.join(os.environ["SPARK_HOME"], "bin")
        os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

        # Initialize findspark
        findspark.init(os.environ["SPARK_HOME"])

        # Initialize Spark session
        spark = (
            SparkSession.builder
            .appName("TestPySpark")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )

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
