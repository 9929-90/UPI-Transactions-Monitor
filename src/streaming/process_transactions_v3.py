import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    try_to_timestamp,
    when,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

ROOT = Path(__file__).resolve().parents[2]

OUTPUT = ROOT / "data" / "processed" / "transactions_v3"
QUARANTINE = ROOT / "data" / "quarantine" / "transactions_v3"
VALID_CHECKPOINT = ROOT / "data" / "checkpoints" / "transactions_v3"
INVALID_CHECKPOINT = ROOT / "data" / "checkpoints" / "transactions_v3_quarantine"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "upi-transactions"
WATERMARK_DELAY = "2 days"

SCHEMA = StructType([
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


def main():
    for path in (
        OUTPUT,
        QUARANTINE,
        VALID_CHECKPOINT,
        INVALID_CHECKPOINT,
    ):
        path.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("UPITransactionStreamingV3")
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

    try:
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
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset"),
                col("value").cast("string").alias("raw_payload"),
                from_json(
                    col("value").cast("string"),
                    SCHEMA,
                ).alias("event"),
            )
            .select("kafka_timestamp", "partition", "offset", "raw_payload", "event.*")
            .withColumn(
                "event_timestamp",
                try_to_timestamp(col("event_time")),
            )
            .withColumn(
                "validation_error",
                when(
                    col("transaction_id").isNull()
                    | (col("transaction_id") == ""),
                    lit("missing_transaction_id"),
                )
                .when(
                    col("amount").isNull(),
                    lit("missing_or_invalid_amount"),
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
                    | (col("event_time") == ""),
                    lit("missing_event_time"),
                )
                .when(
                    col("event_timestamp").isNull(),
                    lit("invalid_event_time_format"),
                ),
            )
            .withColumn("processed_at", current_timestamp())
        )

        # Query 1: valid events with cross-micro-batch deduplication.
        valid_stream = (
            parsed
            .filter(col("validation_error").isNull())
            .withWatermark("event_timestamp", WATERMARK_DELAY)
            .dropDuplicatesWithinWatermark(["transaction_id"])
        )

        def write_valid_batch(batch_df, batch_id):
            row_count = batch_df.count()

            if row_count == 0:
                print(f"Valid batch {batch_id}: 0 records")
                return

            batch_output = OUTPUT / f"batch_id={batch_id}"

            (
                batch_df
                .drop("validation_error", "raw_payload")
                .write
                .mode("overwrite")
                .parquet(str(batch_output))
            )

            print(
                f"V3 valid batch {batch_id}: "
                f"written={row_count}"
            )

        valid_query = (
            valid_stream.writeStream
            .foreachBatch(write_valid_batch)
            .option("checkpointLocation", str(VALID_CHECKPOINT))
            .trigger(processingTime="5 seconds")
            .start()
        )

        # Query 2: invalid events go to quarantine.
        invalid_stream = parsed.filter(
            col("validation_error").isNotNull()
        )

        def write_invalid_batch(batch_df, batch_id):
            row_count = batch_df.count()

            if row_count == 0:
                return

            batch_output = QUARANTINE / f"batch_id={batch_id}"

            (
                batch_df
                .select(
                    "raw_payload",
                    "validation_error",
                    "kafka_timestamp",
                    "partition",
                    "offset",
                    "transaction_id",
                    "event_time",
                    "processed_at",
                )
                .write
                .mode("overwrite")
                .parquet(str(batch_output))
            )

            print(
                f"V3 quarantine batch {batch_id}: "
                f"quarantined={row_count}"
            )

        invalid_query = (
            invalid_stream.writeStream
            .foreachBatch(write_invalid_batch)
            .option("checkpointLocation", str(INVALID_CHECKPOINT))
            .trigger(processingTime="5 seconds")
            .start()
        )

        print("V3 streaming pipeline started.")
        print(f"Kafka topic: {TOPIC}")
        print(f"Valid output: {OUTPUT}")
        print(f"Quarantine output: {QUARANTINE}")
        print(f"Watermark delay: {WATERMARK_DELAY}")
        print("Press Ctrl+C to stop.")

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("Stopping V3 streaming pipeline...")

    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
