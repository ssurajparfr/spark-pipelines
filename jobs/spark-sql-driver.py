"""
spark-sql-driver.py
===================
Generic SQL executor for SparkSqlPipeline sql-type steps.

Upload to: s3a://<scriptsBucket>/spark-sql-driver.py

Called by SparkApplication as:
  mainApplicationFile: s3a://<scriptsBucket>/spark-sql-driver.py
  arguments:
    argv[1]  input path       (e.g. s3a://team-a/job-data/raw/securities/)
    argv[2]  output path      (e.g. s3a://team-a/job-data/lake/securities/)
    argv[3]  sql file path    (e.g. s3a://team-a/job/securities_transform.sql)
    argv[4+] optional flags:
               --inputFormat=csv|json|parquet   (default: inferred from path, else parquet)
               --outputFormat=csv|json|parquet  (default: parquet)
               --partitionColumn=<col>          (partition output by this column)

SQL file convention:
  Reference the input dataset as the temp view named 'input':
    SELECT isin, CAST(amount AS DOUBLE) AS amount, currency, type, date
    FROM input
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def read_sql_from_s3a(spark: SparkSession, sql_path: str) -> str:
    """Read a .sql file from s3a using Hadoop FileSystem API."""
    sc = spark.sparkContext
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
        raise ValueError(
            "Expected at least 3 arguments: <input_path> <output_path> <sql_path>"
        )

    input_path  = sys.argv[1]
    output_path = sys.argv[2]
    sql_path    = sys.argv[3]

    # Defaults
    input_format   = None
    output_format  = "parquet"
    partition_col  = None

    # Parse optional flags from argv[4:]
    for arg in sys.argv[4:]:
        if arg.startswith("--inputFormat="):
            input_format = arg.split("=", 1)[1].lower()
        elif arg.startswith("--outputFormat="):
            output_format = arg.split("=", 1)[1].lower()
        elif arg.startswith("--partitionColumn="):
            partition_col = arg.split("=", 1)[1]

    # Infer input format from file extension if not provided
    if input_format is None:
        if input_path.endswith(".csv"):
            input_format = "csv"
        elif input_path.endswith(".json"):
            input_format = "json"
        else:
            input_format = "parquet"

    # Initialize Spark
    spark = SparkSession.builder.appName("spark-sql-driver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[sql-driver] input:         {input_path}")
    print(f"[sql-driver] output:        {output_path}")
    print(f"[sql-driver] sql:           {sql_path}")
    print(f"[sql-driver] inputFormat:   {input_format}")
    print(f"[sql-driver] outputFormat:  {output_format}")
    print(f"[sql-driver] partitionCol:  {partition_col}")

    # Read input
    if input_format == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    elif input_format == "json":
        df = spark.read.json(input_path)
    else:
        df = spark.read.parquet(input_path)

    df.createOrReplaceTempView("input")
    print(f"[sql-driver] Registered temp view 'input' with {df.count()} rows")

    # Read and execute SQL
    print(f"[sql-driver] Reading SQL from: {sql_path}")
    sql_text = read_sql_from_s3a(spark, sql_path)
    print(f"[sql-driver] SQL:\n{sql_text}")

    result = spark.sql(sql_text)
    result.printSchema()
    print(f"[sql-driver] Result row count: {result.count()}")

    # Write output
    if partition_col:
        # Add partition column with today's date if not already in result
        if partition_col not in result.columns:
            partition_value = datetime.now().strftime("%Y-%m-%d")
            print(f"[sql-driver] Adding partition column '{partition_col}' = {partition_value}")
            result = result.withColumn(partition_col, lit(partition_value))

        print(f"[sql-driver] Writing output partitioned by '{partition_col}' to: {output_path}")
        writer = result.write.mode("overwrite").partitionBy(partition_col)
        if output_format == "csv":
            writer.option("header", "true").csv(output_path)
        elif output_format == "json":
            writer.json(output_path)
        else:
            writer.parquet(output_path)
    else:
        print(f"[sql-driver] Writing output to: {output_path}")
        writer = result.write.mode("overwrite")
        if output_format == "csv":
            writer.option("header", "true").csv(output_path)
        elif output_format == "json":
            writer.json(output_path)
        else:
            writer.parquet(output_path)

    print("[sql-driver] Done.")
    spark.stop()


if __name__ == "__main__":
    main()