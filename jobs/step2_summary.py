"""
step2_summary.py
================
Reads Parquet produced by step1, computes per-column summary, writes CSV back to MinIO.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min, max


def main():
    if len(sys.argv) != 3:
        print("Usage: step2_summary.py <input_parquet_s3a> <output_csv_s3a>", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("step2-summary").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[step2] Reading parquet from: {input_path}")
    df = spark.read.parquet(input_path)
    df.printSchema()

    total = df.count()
    print(f"[step2] Total rows: {total}")
    df.show(10, truncate=False)

    # Build summary per column
    summary_rows = []
    for c, t in df.dtypes:
        agg_row = df.agg(
            count(col(c)).alias("count"),
            countDistinct(col(c)).alias("distinct"),
            min(col(c)).cast("string").alias("min"),
            max(col(c)).cast("string").alias("max"),
        ).collect()[0]
        summary_rows.append((c, t, agg_row["count"], agg_row["distinct"], agg_row["min"], agg_row["max"]))

    summary_df = spark.createDataFrame(
        summary_rows,
        ["column", "type", "count", "distinct", "min", "max"]
    )

    print("[step2] Column summary:")
    summary_df.show(truncate=False)

    print(f"[step2] Writing summary CSV to: {output_path}")
    summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    print("[step2] Done.")
    spark.stop()


if __name__ == "__main__":
    main()