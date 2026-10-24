"""Real Spark DataFrame cleaning shared by batch and Structured Streaming."""
import json
import os
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

SCHEMA = T.StructType([T.StructField(k, T.StringType()) for k in ("hash", "count", "source", "_corrupt_record")])


def session(name="Breach batch"):
    return (SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]"))
            .appName(name).config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false").getOrCreate())


def normalize(df):
    # Each parsed row receives exactly one rejection reason, in this precedence.
    df = df.withColumn("original_hash", F.col("hash")).withColumn("hash", F.upper(F.regexp_replace("hash", r"^\s+|\s+$", "")))
    df = df.withColumn("count_text", F.col("count").cast("string"))
    reason = (F.when(F.col("_corrupt_record").isNotNull(), "malformed_row")
              .when(F.col("hash").isNull() | (F.length("hash") == 0), "missing_hash")
              .when(~F.col("hash").rlike("^[0-9A-F]{40}$"), "invalid_hash")
              .when(F.col("count_text").isNull() | ~F.col("count_text").rlike("^[0-9]+$") |
                    F.col("count_text").cast("long").isNull(), "invalid_count"))
    return df.withColumn("reason", reason).withColumn("normalized", ~F.col("original_hash").eqNullSafe(F.col("hash")))


def read_input(spark, path):
    name = str(path).lower().removesuffix(".gz")
    if name.endswith(".csv"):
        return spark.read.schema(SCHEMA).option("header", True).option("mode", "PERMISSIVE").csv(str(path))
    if name.endswith((".json", ".jsonl")):
        return spark.read.schema(SCHEMA).option("multiLine", name.endswith(".json")).json(str(path))
    lines = spark.read.text(str(path))
    parts = F.split("value", ":", -1)
    return lines.select(parts[0].alias("hash"), parts[1].alias("count"),
                        F.lit("imported-unverified").alias("source"),
                        F.when(F.size(parts) != 2, F.col("value")).alias("_corrupt_record"))


def run(paths, output):
    start = time.perf_counter()
    spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    parsed = None
    for path in paths:
        current = read_input(spark, path)
        parsed = current if parsed is None else parsed.unionByName(current)
    if parsed is None:
        raise ValueError("No input files selected")
    df = normalize(parsed).persist()
    try:
        total = df.count()
        reasons = {r.reason: r['count'] for r in df.filter("reason IS NOT NULL").groupBy("reason").count().collect()}
        invalid = sum(reasons.values())
        changed = df.filter("normalized").count()
        valid = df.filter("reason IS NULL")
        clean = valid.groupBy("hash").agg(F.max(F.col("count_text").cast("long")).alias("count"),
                                        F.concat_ws("|", F.sort_array(F.collect_set(F.coalesce("source", F.lit("imported-unverified"))))).alias("source")).persist()
        try:
            unique = clean.count()
            clean.write.mode("overwrite").parquet(str(output))
        finally:
            clean.unpersist()
        seconds = time.perf_counter() - start
        report = dict(input_records=total, valid_records=total-invalid, invalid_records=invalid,
                      duplicates_removed=total-invalid-unique, unique_hashes=unique,
                      normalized_rows=changed, rejection_reasons=reasons, read_failures=0,
                      duration_seconds=seconds, records_per_second=total / seconds,
                      spark_mode=spark.sparkContext.master, spark_version=spark.version,
                      policy="Maximum count per normalized hash; sources unioned; normalization counts all parsed rows")
        Path(str(output) + '.report.json').write_text(json.dumps(report, indent=2))
        return report
    finally:
        df.unpersist()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("paths", nargs="+")
    p.add_argument("--output", default="data/processed/clean")
    a = p.parse_args()
    print(json.dumps(run(a.paths, a.output), indent=2))
