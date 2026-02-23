"""
spark-sql-driver.py
===================
Generic SQL executor for SparkSqlPipeline sql-type steps.

Uploaded to: s3a://spark-jars/spark-sql-driver.py

Called by SparkApplication as:
  mainApplicationFile: s3a://spark-jars/spark-sql-driver.py
  arguments:
    - s3a://job-data/dev/data.csv          argv[1] input path
    - s3a://job-data/dev-output/processed/ argv[2] output path
    - s3a://spark-jars/transform.sql       argv[3] sql file path in MinIO
    - csv|parquet|json                     argv[4] optional input format (overrides inferring from path)

The SQL file uses 'input' as the table name — it is registered
as a temp view from the input path before execution:

  SELECT trim(column1), trim(column2) FROM input

Output is written as parquet to the output path.
"""
import sys
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import lit, col, to_date

def read_sql_from_s3a(spark: SparkSession, sql_path: str) -> str:
    """Read a SQL file from s3a and return its contents as a string."""
    sc = spark.sparkContext
    # Use Hadoop FileSystem API to read the file as text
    hadoop_conf = sc._jsc.hadoopConfiguration()
    path = sc._jvm.org.apache.hadoop.fs.Path(sql_path)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI(sql_path), hadoop_conf
    )
    stream = fs.open(path)
    reader = sc._jvm.java.io.BufferedReader(
        sc._jvm.java.io.InputStreamReader(stream)
    )
    lines = []
    line = reader.readLine()
    while line is not None:
        lines.append(line)
        line = reader.readLine()
    reader.close()
    return "\n".join(lines)

def main():
    if len(sys.argv) < 4:
        raise ValueError("Expected at least 3 arguments: input, output, sql_path")

    input_path  = sys.argv[1]
    output_path = sys.argv[2]
    sql_path    = sys.argv[3]

    # Optional partition column (from extra arg --partitionColumn=xxx)
    partition_col = None
    input_format = None
    output_format = "parquet"  # Default to parquet output

    # Parse additional arguments
    for arg in sys.argv[4:]:
        if arg.startswith("--partitionColumn="):
            partition_col = arg.split("=", 1)[1]
        elif arg.startswith("--inputFormat="):
            input_format = arg.split("=", 1)[1].lower()
        elif arg.startswith("--outputFormat="):
            output_format = arg.split("=", 1)[1].lower()

    # Initialize Spark session
    spark = SparkSession.builder.appName("spark-sql-driver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Validate input format
    if input_format is None:
        if input_path.endswith(".csv"):
            input_format = "csv"
        elif input_path.endswith(".json"):
            input_format = "json"
        else:
            input_format = "parquet"

    print(f"[sql-driver] Using input format: {input_format}")

    # Read input data based on the input format
    if input_format == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    elif input_format == "json":
        df = spark.read.json(input_path)
    else:  # Default to parquet
        df = spark.read.parquet(input_path)

    # Register the input data as a temp view named 'input' — SQL files reference this name
    df.createOrReplaceTempView("input")
    print(f"[sql-driver] Registered temp view 'input' with {df.count()} rows")

    # Read SQL query from MinIO (S3)
    print(f"[sql-driver] Reading SQL from: {sql_path}")
    sql_text = read_sql_from_s3a(spark, sql_path)
    print(f"[sql-driver] SQL:\n{sql_text}")

    # Execute SQL query on the registered DataFrame
    result = spark.sql(sql_text)
    print(f"[sql-driver] Result schema:")
    result.printSchema()
    print(f"[sql-driver] Result row count: {result.count()}")

    # Write output with optional partitioning
    if partition_col:
        # If the partition column exists in the DataFrame, use it
        if partition_col in df.columns:
            # Ensure it's in 'yyyy-MM-dd' format
            result = result.withColumn(partition_col, to_date(col(partition_col)))
            print(f"[sql-driver] Using existing column '{partition_col}' for partitioning")
        else:
            # Otherwise, use the current date
            partition_value = datetime.now().strftime("%Y-%m-%d")
            print(f"[sql-driver] Column '{partition_col}' not found, using current date: {partition_value}")
            result = result.withColumn(partition_col, lit(partition_value))

        if output_format == "csv":
            result.write.mode("overwrite").partitionBy(partition_col).option("header", "true").csv(output_path)
        elif output_format == "json":
            result.write.mode("overwrite").partitionBy(partition_col).json(output_path)
        else:
            result.write.mode("overwrite").partitionBy(partition_col).parquet(output_path)
    else:
        print(f"[sql-driver] Writing output to: {output_path}")
        if output_format == "csv":
            result.write.mode("overwrite").option("header", "true").csv(output_path)
        elif output_format == "json":
            result.write.mode("overwrite").json(output_path)
        else:
            result.write.mode("overwrite").parquet(output_path)

    print(f"[sql-driver] Done.")
    
    spark.stop()

if __name__ == "__main__":
    main()