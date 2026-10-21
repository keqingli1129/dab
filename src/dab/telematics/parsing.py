"""Turn raw Kafka records (bytes) into telematics columns."""

# DataFrame: type hint for the input/output of parse_telematics().
from pyspark.sql import DataFrame

# F: Spark SQL column functions (col, from_json, struct, ...).
from pyspark.sql import functions as F

# Types used to describe the JSON payload's shape.
from pyspark.sql.types import MapType, StringType

# Like the transcript: parse every field as a string for now; typing comes in silver.
# Read the JSON object as a map of string -> string. This never fails on a wrong type
# (e.g. speed sent as "fast"), so bronze keeps every record; casting to double/timestamp happens later.
PAYLOAD_SCHEMA = MapType(StringType(), StringType())
# The keys to pull out of the JSON payload, one output column each. Must match events.generate_event().
EVENT_FIELDS = ("chassis_number", "speed", "latitude", "longitude", "event_timestamp")


def parse_telematics(raw_df: DataFrame) -> DataFrame:
    """Decode the Kafka value to JSON text and split it into one column per event field."""
    # `raw_df` is what spark.readStream.format("kafka") returns, with columns:
    # key, value (binary), topic, partition, offset, timestamp, timestampType.
    # Step 1: cast the binary `value` to a string -> `raw_json` holds the JSON text.
    # Step 2: parse that text with PAYLOAD_SCHEMA -> `payload` is a map like {"speed": "87.3", ...}.
    decoded = raw_df.withColumn("raw_json", F.col("value").cast("string")).withColumn(
        "payload", F.from_json("raw_json", PAYLOAD_SCHEMA)
    )
    # Build the output columns.
    return decoded.select(
        # One column per field, e.g. payload["speed"] AS speed. Missing keys become NULL.
        *[F.col("payload").getItem(field).alias(field) for field in EVENT_FIELDS],
        # Keep where each record came from in Kafka (for lineage, debugging and de-duplication).
        # `timestamp` here is the Kafka broker/producer time, not the event's own event_timestamp.
        F.struct("topic", "partition", "offset", "timestamp").alias("stream_metadata"),
        # Keep the original JSON so bad or unexpected records can be inspected or re-parsed later.
        "raw_json",
    )
