
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    sha2,
    from_json,
    lit,
    try_to_timestamp,
    when,
    trim,
    window,
    isnan,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

ROOT = Path(__file__).resolve().parents[2]

BASE_OUTPUT = ROOT / "data" / "processed" / "transactions_v4"
NORMAL_OUTPUT = BASE_OUTPUT / "normal"
FLAGGED_OUTPUT = BASE_OUTPUT / "flagged"

QUARANTINE_OUTPUT = (
    ROOT / "data" / "quarantine" / "transactions_v4"
)

VALID_CHECKPOINT = (
    ROOT / "data" / "checkpoints" / "transactions_v4"
)
INVALID_CHECKPOINT = (
    ROOT / "data" / "checkpoints" / "transactions_v4_quarantine"
)

VELOCITY_V2_OUTPUT = (
    ROOT / "data" / "processed" / "transactions_v4" / "velocity_flags_v2"
)

VELOCITY_V2_CHECKPOINT = (
    ROOT / "data" / "checkpoints" / "transactions_v4_velocity_v2"
)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "upi-transactions"

WATERMARK_DELAY = "2 days"
HIGH_VALUE_THRESHOLD = float(
    os.getenv("UPI_HIGH_VALUE_THRESHOLD", "50000")
)

# --------------------------------------------------
# Incoming Kafka JSON schema
# --------------------------------------------------

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("is_synthetic", StringType(), True),
])

# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("UPITransactionMonitoringV4")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
    )
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Read Kafka events
# --------------------------------------------------

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    kafka_stream
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_payload"),
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data"),
    )
    .select(
        "kafka_key",
        "partition",
        "offset",
        "kafka_timestamp",
        "raw_payload",
        "data.*",
    )
    .withColumn(
        "event_timestamp",
        try_to_timestamp(col("event_time"))
    )
    .withColumn("processed_at", __import__(
        "pyspark.sql.functions", fromlist=["current_timestamp"]
    ).current_timestamp())
)

# --------------------------------------------------
# Validate incoming events
# --------------------------------------------------

parsed = parsed.withColumn(
    "validation_error",
    when(
        col("transaction_id").isNull()
        | (trim(col("transaction_id")) == ""),
        lit("missing_transaction_id"),
    )
    .when(
        col("amount").isNull() | isnan(col("amount")),
        lit("missing_or_invalid_amount"),
    )
    .when(
        (col("amount") == float("inf"))
        | (col("amount") == float("-inf")),
        lit("invalid_amount"),
    )
    .when(
        col("amount") <= 0,
        lit("amount_must_be_positive"),
    )
    .when(
        col("currency").isNull()
        | (col("currency") != "INR"),
        lit("unsupported_currency"),
    )
    .when(
        col("status").isNull()
        | (~col("status").isin("SUCCESS", "FAILED", "PENDING")),
        lit("invalid_status"),
    )
    .when(
        col("event_time").isNull()
        | (trim(col("event_time")) == ""),
        lit("missing_event_time"),
    )
    .when(
        col("event_timestamp").isNull(),
        lit("invalid_event_timestamp"),
    )
    .otherwise(lit(None).cast("string")),
)

# --------------------------------------------------
# Write invalid events to quarantine
# --------------------------------------------------

def write_quarantine_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"V4 quarantine batch {batch_id}: 0 records")
        return

    output = (
        batch_df
        .withColumn("batch_id", lit(batch_id))
    )

    output_path = QUARANTINE_OUTPUT / f"batch_id={batch_id}"

    (
        output.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(
        f"V4 quarantine batch {batch_id}: "
        f"{output.count()} records"
    )


invalid_stream = (
    parsed
    .filter(col("validation_error").isNotNull())
    .writeStream
    .foreachBatch(write_quarantine_batch)
    .option("checkpointLocation", str(INVALID_CHECKPOINT))
    .trigger(processingTime="5 seconds")
    .start()
)

# --------------------------------------------------
# Deduplicate valid transactions across micro-batches
# --------------------------------------------------

valid_stream_df = (
    parsed
    .filter(col("validation_error").isNull())
    .withWatermark("event_timestamp", WATERMARK_DELAY)
    .dropDuplicatesWithinWatermark(["transaction_id"])
)

# --------------------------------------------------
# Classify and write normal / suspicious transactions
# --------------------------------------------------

def write_valid_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"V4 valid batch {batch_id}: 0 records")
        return

    batch_df = batch_df.withColumn(
        "batch_id",
        lit(batch_id)
    )

    flagged = (
        batch_df
        .filter(col("amount") >= HIGH_VALUE_THRESHOLD)
        .withColumn(
            "risk_reason",
            lit("high_value_transaction")
        )
        .withColumn("risk_level", lit("HIGH"))
        .withColumn(
            "risk_threshold",
            lit(HIGH_VALUE_THRESHOLD)
        )
    )

    normal = batch_df.filter(
        col("amount") < HIGH_VALUE_THRESHOLD
    )

    flagged_count = flagged.count()
    normal_count = normal.count()

    if normal_count > 0:
        (
            normal.write
            .mode("overwrite")
            .parquet(str(NORMAL_OUTPUT / f"batch_id={batch_id}"))
        )

    if flagged_count > 0:
        (
            flagged.write
            .mode("overwrite")
            .parquet(str(FLAGGED_OUTPUT / f"batch_id={batch_id}"))
        )

    print(
        f"V4 batch {batch_id}: "
        f"normal={normal_count}, "
        f"flagged={flagged_count}, "
        f"threshold=INR {HIGH_VALUE_THRESHOLD:.2f}"
    )


valid_query = (
    valid_stream_df
    .writeStream
    .foreachBatch(write_valid_batch)
    .option("checkpointLocation", str(VALID_CHECKPOINT))
    .trigger(processingTime="5 seconds")
    .start()
)


# --------------------------------------------------
# Detect rapid repeated transactions per user
# --------------------------------------------------

VELOCITY_THRESHOLD = 5
VELOCITY_WINDOW = "1 minute"
VELOCITY_SLIDE = "10 seconds"
VELOCITY_WATERMARK = "2 minutes"


def write_velocity_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    output = (
        batch_df
        .withColumn(
            "alert_id",
            sha2(
                concat_ws(
                    "||",
                    col("user_id"),
                    col("window.start").cast("string"),
                    col("window.end").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("batch_id", lit(batch_id))
    )

    output_path = VELOCITY_V2_OUTPUT / f"batch_id={batch_id}"
    output.write.mode("overwrite").parquet(str(output_path))
    print(f"V4 velocity V2 batch {batch_id}: {output.count()} finalized alert windows")


velocity_events = (
    parsed
    .filter(
        col("validation_error").isNull()
        & col("user_id").isNotNull()
        & (trim(col("user_id")) != "")
        & col("transaction_id").isNotNull()
        & (trim(col("transaction_id")) != "")
    )
    .withWatermark("event_timestamp", VELOCITY_WATERMARK)
    .dropDuplicatesWithinWatermark(["transaction_id"])
)

velocity_counts = (
    velocity_events
    .groupBy(
        window(
            col("event_timestamp"),
            VELOCITY_WINDOW,
            VELOCITY_SLIDE,
        ),
        col("user_id"),
    )
    .count()
    .filter(col("count") >= VELOCITY_THRESHOLD)
)

velocity_query = (
    velocity_counts
    .writeStream
    .outputMode("append")
    .foreachBatch(write_velocity_batch)
    .option("checkpointLocation", str(VELOCITY_V2_CHECKPOINT))
    .trigger(processingTime="5 seconds")
    .start()
)

print("Velocity detection V2 enabled.")
print(f"Threshold: {VELOCITY_THRESHOLD} transactions per user")
print(f"Window: {VELOCITY_WINDOW}, sliding every {VELOCITY_SLIDE}")
print(f"Velocity V2 output: {VELOCITY_V2_OUTPUT}")
print(f"Velocity V2 checkpoint: {VELOCITY_V2_CHECKPOINT}")


print("V4 UPI monitoring pipeline started.")
print(f"Kafka topic: {TOPIC}")
print(f"Normal output: {NORMAL_OUTPUT}")
print(f"Flagged output: {FLAGGED_OUTPUT}")
print(f"Quarantine output: {QUARANTINE_OUTPUT}")
print(f"High-value threshold: INR {HIGH_VALUE_THRESHOLD:.2f}")
print(f"Watermark delay: {WATERMARK_DELAY}")
print("Press Ctrl+C to stop.")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping V4 pipeline...")
finally:
    for query in spark.streams.active:
        query.stop()
    spark.stop()
