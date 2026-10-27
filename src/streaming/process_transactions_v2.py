import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    when,
    lit,
    row_number,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

ROOT = Path(__file__).resolve().parents[2]

# Version 2 paths: preserve all existing data and checkpoints.
OUTPUT = ROOT / "data" / "processed" / "transactions_v2"
QUARANTINE = ROOT / "data" / "quarantine" / "transactions_v2"
CHECKPOINT = ROOT / "data" / "checkpoints" / "transactions_v2"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "upi-transactions"

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


def main():
    spark = (
        SparkSession.builder
        .appName("UPITransactionStreamingV2")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
        )
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for path in (OUTPUT, QUARANTINE, CHECKPOINT):
        path.mkdir(parents=True, exist_ok=True)

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
                col("value").cast("string"), schema
            ).alias("event"),
        )
        .select("kafka_timestamp", "partition", "offset",
                "raw_payload", "event.*")
        .withColumn(
            "validation_error",
            when(
                col("transaction_id").isNull(),
                "missing_transaction_id",
            )
            .when(
                col("amount").isNull(),
                "missing_or_invalid_amount",
            )
            .when(
                col("amount") <= 0,
                "amount_must_be_positive",
            )
            .when(
                col("currency").isNull()
                | (col("currency") != "INR"),
                "unsupported_currency",
            )
            .when(
                col("status").isNull()
                | (~col("status").isin(
                    "SUCCESS", "FAILED", "PENDING"
                )),
                "invalid_status",
            )
            .when(
                col("event_time").isNull(),
                "missing_event_time",
            ),
        )
        .withColumn("processed_at", current_timestamp())
    )

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"Batch {batch_id}: no events")
            return

        valid = batch_df.filter(
            col("validation_error").isNull()
        )

        invalid = batch_df.filter(
            col("validation_error").isNotNull()
        )

        # Deterministically keep the first occurrence within this batch.
        duplicate_window = (
            Window.partitionBy("transaction_id")
            .orderBy(col("partition"), col("offset"))
        )

        ranked = valid.withColumn(
            "_duplicate_rank",
            row_number().over(duplicate_window),
        )

        unique_valid = ranked.filter(
            col("_duplicate_rank") == 1
        ).drop("_duplicate_rank")

        duplicate_records = (
            ranked.filter(col("_duplicate_rank") > 1)
            .drop("_duplicate_rank", "validation_error")
            .withColumn(
                "validation_error",
                lit("duplicate_transaction_id_in_batch"),
            )
        )

        quarantined = invalid.unionByName(
            duplicate_records,
            allowMissingColumns=True,
        )

        valid_count = unique_valid.count()
        invalid_count = quarantined.count()
        total_count = batch_df.count()

        # Every batch has its own deterministic output location.
        # Reprocessing a batch replaces that batch's files.
        if valid_count:
            batch_output = OUTPUT / f"batch_id={batch_id}"

            (
                unique_valid
                .drop("validation_error", "raw_payload")
                .write
                .mode("overwrite")
                .parquet(str(batch_output))
            )

        if invalid_count:
            batch_quarantine = (
                QUARANTINE / f"batch_id={batch_id}"
            )

            (
                quarantined
                .select(
                    "raw_payload",
                    "validation_error",
                    "kafka_timestamp",
                    "partition",
                    "offset",
                    "transaction_id",
                    "processed_at",
                )
                .write
                .mode("overwrite")
                .parquet(str(batch_quarantine))
            )

        print(
            f"Batch {batch_id}: total={total_count}, "
            f"processed={valid_count}, "
            f"quarantined={invalid_count}"
        )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(CHECKPOINT))
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Reading Kafka topic: {TOPIC}")
    print(f"Processed output: {OUTPUT}")
    print(f"Quarantine output: {QUARANTINE}")
    print(f"Checkpoint: {CHECKPOINT}")
    print("Press Ctrl+C to stop.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming query...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
