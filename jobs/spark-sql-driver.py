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

The SQL file uses 'input' as the table name — it is registered
as a temp view from the input path before execution:

  SELECT trim(column1), trim(column2) FROM input

Output is written as parquet to the output path.
"""

import sys
from pyspark.sql import SparkSession


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
    if len(sys.argv) != 4:
        print(
            "Usage: spark-sql-driver.py <input_path> <output_path> <sql_s3a_path>",
            file=sys.stderr,
        )
        sys.exit(1)

    input_path  = sys.argv[1]
    output_path = sys.argv[2]
    sql_path    = sys.argv[3]

    spark = SparkSession.builder.appName("spark-sql-driver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read input — supports csv, parquet, json (inferred from path extension)
    print(f"[sql-driver] Reading input from: {input_path}")
    if input_path.endswith(".csv"):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    elif input_path.endswith(".json"):
        df = spark.read.json(input_path)
    else:
        # Default to parquet
        df = spark.read.parquet(input_path)

    # Register as temp view named 'input' — SQL files reference this name
    df.createOrReplaceTempView("input")
    print(f"[sql-driver] Registered temp view 'input' with {df.count()} rows")

    # Read SQL from MinIO
    print(f"[sql-driver] Reading SQL from: {sql_path}")
    sql_text = read_sql_from_s3a(spark, sql_path)
    print(f"[sql-driver] SQL:\n{sql_text}")

    # Execute SQL
    result = spark.sql(sql_text)
    print(f"[sql-driver] Result schema:")
    result.printSchema()
    print(f"[sql-driver] Result row count: {result.count()}")

    # Write output as parquet
    print(f"[sql-driver] Writing output to: {output_path}")
    result.write.mode("overwrite").parquet(output_path)
    print(f"[sql-driver] Done.")

    spark.stop()


if __name__ == "__main__":
    main()