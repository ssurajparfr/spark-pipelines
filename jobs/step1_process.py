"""
step1_process.py
================
Reads data.csv from MinIO, trims string columns, writes parquet back to MinIO.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


def main():
    if len(sys.argv) != 3:
        print("Usage: step1_process.py <input_csv_s3a> <output_parquet_s3a>", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("step1-process").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[step1] Reading CSV from: {input_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    df.printSchema()

    row_count = df.count()
    print(f"[step1] Input row count: {row_count}")

    # Trim all string columns
    cleaned = df.select([trim(col(c)).alias(c) if t == "string" else col(c) for c, t in df.dtypes])

    print(f"[step1] Writing parquet to: {output_path}")
    cleaned.write.mode("overwrite").parquet(output_path)

    print("[step1] Done.")
    spark.stop()


if __name__ == "__main__":
    main()