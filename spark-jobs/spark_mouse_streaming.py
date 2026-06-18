import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    expr,
    floor,
    from_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mouse-events")
POSTGRES_JDBC_URL = os.getenv(
    "POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/mousetracking"
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION", "/opt/spark/checkpoints/mouse-streaming"
)


spark = (
    SparkSession.builder.appName("MouseTrackingStreaming")
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

event_schema = StructType(
    [
        StructField("session_id", StringType()),
        StructField("user_id", StringType()),
        StructField("page_url", StringType()),
        StructField("event_type", StringType()),
        StructField("x", IntegerType()),
        StructField("y", IntegerType()),
        StructField("scroll_y", IntegerType()),
        StructField("viewport_width", IntegerType()),
        StructField("viewport_height", IntegerType()),
        StructField("timestamp", StringType()),
    ]
)

jdbc_props = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

raw_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_events = (
    raw_kafka.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_value"),
    )
    .select("*", from_json(col("raw_value"), event_schema).alias("event"))
    .select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
        col("event.session_id").alias("session_id"),
        col("event.user_id").alias("user_id"),
        col("event.page_url").alias("page_url"),
        col("event.event_type").alias("event_type"),
        col("event.x").alias("x"),
        col("event.y").alias("y"),
        col("event.scroll_y").alias("scroll_y"),
        col("event.viewport_width").alias("viewport_width"),
        col("event.viewport_height").alias("viewport_height"),
        col("event.timestamp").alias("event_timestamp_raw"),
    )
    .withColumn(
        "event_timestamp",
        when(
            col("event_timestamp_raw").rlike(r"^\d+(\.\d+)?$"),
            to_timestamp(expr("timestamp_seconds(CAST(event_timestamp_raw AS DOUBLE))")),
        ).otherwise(to_timestamp(col("event_timestamp_raw"))),
    )
    .withColumn("ingested_at", current_timestamp())
    .filter(col("session_id").isNotNull())
    .filter(col("page_url").isNotNull())
    .filter(col("event_type").isNotNull())
)


def pg_execute(sql):
    """Execute raw SQL on PostgreSQL via py4j JDBC (driver-side only)."""
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_USER)
    props.setProperty("password", POSTGRES_PASSWORD)
    conn = jvm.java.sql.DriverManager.getConnection(POSTGRES_JDBC_URL, props)
    conn.setAutoCommit(True)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def write_event_raw(batch_df, batch_id):
    rows = (
        batch_df.select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "session_id",
            "user_id",
            "page_url",
            "event_type",
            "x",
            "y",
            "scroll_y",
            "viewport_width",
            "viewport_height",
            "event_timestamp",
            "ingested_at",
            "raw_value",
        )
        .dropDuplicates(["kafka_topic", "kafka_partition", "kafka_offset"])
    )

    if rows.rdd.isEmpty():
        return

    rows.write.jdbc(
        url=POSTGRES_JDBC_URL,
        table="event_raw_stage",
        mode="overwrite",
        properties=jdbc_props,
    )

    pg_execute("""
        INSERT INTO event_raw (
            kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
            session_id, user_id, page_url, event_type, x, y, scroll_y,
            viewport_width, viewport_height, event_timestamp, ingested_at, raw_value
        )
        SELECT
            kafka_topic,
            CAST(kafka_partition AS INTEGER),
            kafka_offset,
            kafka_timestamp,
            session_id, user_id, page_url, event_type, x, y, scroll_y,
            viewport_width, viewport_height, event_timestamp, ingested_at, raw_value
        FROM event_raw_stage
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
    """)


def write_mouse_heatmap(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    heatmap_df = (
        batch_df
        .filter(col("x").isNotNull() & col("y").isNotNull())
        .withColumn("grid_x", (floor(col("x") / 100) * 100).cast("integer"))
        .withColumn("grid_y", (floor(col("y") / 100) * 100).cast("integer"))
        .groupBy("page_url", "grid_x", "grid_y")
        .agg(count("*").alias("event_count"))
    )

    if heatmap_df.rdd.isEmpty():
        return

    heatmap_df.write.jdbc(
        url=POSTGRES_JDBC_URL,
        table="mouse_heatmap_stage",
        mode="overwrite",
        properties=jdbc_props,
    )

    pg_execute("""
        INSERT INTO mouse_heatmap (page_url, grid_x, grid_y, event_count, updated_at)
        SELECT page_url, grid_x, grid_y, event_count, NOW()
        FROM mouse_heatmap_stage
        ON CONFLICT (page_url, grid_x, grid_y)
        DO UPDATE SET
            event_count = mouse_heatmap.event_count + EXCLUDED.event_count,
            updated_at = NOW()
    """)


raw_query = (
    parsed_events.writeStream.outputMode("append")
    .foreachBatch(write_event_raw)
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/event_raw")
    .trigger(processingTime="5 seconds")
    .queryName("event_raw_writer")
    .start()
)

heatmap_query = (
    parsed_events.writeStream.outputMode("append")
    .foreachBatch(write_mouse_heatmap)
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/mouse_heatmap")
    .trigger(processingTime="5 seconds")
    .queryName("mouse_heatmap_writer")
    .start()
)

spark.streams.awaitAnyTermination()
