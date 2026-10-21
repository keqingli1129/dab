# Sending and ingesting telematics data through Kafka

How the code in `src/dab/telematics/` sends fake car telematics events to a Confluent Cloud Kafka topic, and how
Databricks reads that topic into a bronze Delta table.

## The big picture

```
                 SEND (simulator.py)                                INGEST (Spark on Databricks)
┌──────────────────────────────────────────┐                ┌──────────────────────────────────────────┐
│ events.generate_event()  -> dict          │                │ spark.readStream.format("kafka")         │
│ events.to_json()         -> bytes         │                │   .options(**spark_kafka_options(...))   │
│ confluent_kafka.Producer.produce()        │                │   .load()        -> key/value as binary  │
│   key   = chassis_number                  │   Confluent    │                                          │
│   value = JSON event                      │ ─────────────▶ │ parsing.parse_telematics()               │
│                                           │  topic         │   value -> raw_json -> one col per field │
│ creds: kafka_config.read_credentials()    │ "telematics"   │                                          │
│        + producer_config()                │                │ write to bronze Delta table              │
└──────────────────────────────────────────┘                └──────────────────────────────────────────┘
        ▲                                                                    ▲
        └──────── Databricks secret scope "kafka_dev" ───────────────────────┘
                  bootstrap_servers, api_key, api_secret
```

Both sides read the same three secrets. The producer side uses the `confluent-kafka` Python client; the consumer
side uses Spark's built-in Kafka source. They need the same settings, spelled differently, which is why
`kafka_config.py` has one function for each.

## The four files

| File | Role | Runs where |
|---|---|---|
| `kafka_config.py` | Reads credentials from a secret scope; builds connection settings for the producer (`producer_config`) and for Spark (`spark_kafka_options`) | Both sides |
| `events.py` | Generates one fake telematics reading and serializes it to JSON bytes | Send side |
| `simulator.py` | CLI that generates N events and sends them to Kafka, to volume files, or to the console | Send side (laptop or Databricks job) |
| `parsing.py` | Turns the raw Kafka DataFrame into telematics columns plus Kafka metadata | Ingest side (Spark) |

Every line of all four files has an explanatory comment. This doc covers how they fit together.

## The message format

Each Kafka message has:

| Part | Content | Example |
|---|---|---|
| Key | Chassis number, UTF-8 bytes | `CHS000003` |
| Value | The event as JSON, UTF-8 bytes | see below |

```json
{
  "chassis_number": "CHS000003",
  "speed": 34.9,
  "latitude": 29.96667,
  "longitude": -95.195974,
  "event_timestamp": "2026-10-21T17:49:58.523022+00:00"
}
```

| Field | Generated as |
|---|---|
| `chassis_number` | One of 10 fake IDs, `CHS000001` to `CHS000010` |
| `speed` | Random 0–180 km/h, 1 decimal |
| `latitude` | Random 29.5–30.1 (Houston area), 6 decimals |
| `longitude` | Random -95.8 to -95.0, 6 decimals |
| `event_timestamp` | Time the event was generated, UTC, ISO-8601 |

**Why the key matters.** Kafka sends all messages with the same key to the same partition, and order is guaranteed
only within a partition. Keying by chassis number keeps each car's readings in order.

## Prerequisites

### 1. A Confluent Cloud topic and API key

In Confluent Cloud you need:

- A cluster. Copy its **bootstrap server** (`pkc-xxxxx.<region>.<cloud>.confluent.cloud:9092`) from
  Cluster settings.
- A topic, `telematics` by default (`--topic` changes it).
- An **API key and secret** scoped to that cluster, with permission to write to and read from the topic.

### 2. A Databricks secret scope

`kafka_config.read_credentials()` expects a scope (default `kafka_dev`) containing exactly these three keys:

| Secret key | Value |
|---|---|
| `bootstrap_servers` | The bootstrap server, `host:9092` |
| `api_key` | Confluent API key |
| `api_secret` | Confluent API secret |

Create it once per workspace:

```bash
databricks secrets create-scope kafka_dev --profile azure
databricks secrets put-secret kafka_dev bootstrap_servers --string-value "pkc-xxxxx.eastus.azure.confluent.cloud:9092" --profile azure
databricks secrets put-secret kafka_dev api_key    --string-value "<API KEY>"    --profile azure
databricks secrets put-secret kafka_dev api_secret --string-value "<API SECRET>" --profile azure

# Check (values are not shown, only key names)
databricks secrets list-secrets kafka_dev --profile azure
```

Never put the key or secret in code, job parameters or notebooks. Databricks redacts secret values in notebook and
job output.

### 3. The `confluent-kafka` package (send side only)

`simulator.py` imports `confluent_kafka` only when sending to Kafka. It's a project dependency in `pyproject.toml`,
so `uv sync` installs it into `.venv` and the wheel pulls it in on Databricks. `--dry-run` and `--sink files`
don't need it.

The ingest side doesn't need it. Spark's Kafka source is built into Databricks Runtime and serverless.

## Sending data

### Command-line flags

| Flag | Default | Meaning |
|---|---|---|
| `--topic` | `telematics` | Kafka topic to write to |
| `--count` | `20` | Number of events |
| `--interval` | `3.0` | Seconds between events |
| `--secret-scope` | `kafka_dev` | Scope holding the three secrets |
| `--sink` | `kafka` | `kafka`, or `files` to write JSON files into a volume instead |
| `--landing-path` | none | Volume folder, required with `--sink files` |
| `--dry-run` | off | Print events; no network, no credentials. Overrides `--sink` |

### Step 1: dry run

Check the events look right before touching Kafka:

```bash
.venv/Scripts/python.exe -m dab.telematics.simulator --dry-run --count 3 --interval 0
```

```
[dry-run] telematics key=CHS000003 {"chassis_number": "CHS000003", "speed": 34.9, ...}
[dry-run] telematics key=CHS000007 {"chassis_number": "CHS000007", "speed": 38.9, ...}
[dry-run] telematics key=CHS000008 {"chassis_number": "CHS000008", "speed": 78.6, ...}
```

### Step 2: send to Kafka

```bash
.venv/Scripts/python.exe -m dab.telematics.simulator --count 20 --interval 1 --secret-scope kafka_dev
```

```
Sent 20 events to topic telematics
```

What happens, in order (`_send_to_kafka` in `simulator.py`):

1. **Read credentials.** `dbutils.secrets.get()` fetches the three secrets. Locally, `databricks.sdk.runtime`'s
   `dbutils` calls the workspace through the SDK using your VS Code / CLI sign-in; on Databricks it's the runtime's
   own `dbutils`.
2. **Create the producer** with `producer_config()`: TLS (`SASL_SSL`) plus username/password login (`PLAIN`) using
   the API key/secret.
3. **For each event:**
   - `producer.produce(topic, key, value, on_delivery=...)` puts the message in the producer's local queue and
     returns immediately. It has **not** been delivered yet.
   - `producer.poll(0)` lets the client do network work and run delivery callbacks for messages Kafka has already
     acknowledged. `on_delivery` stores any error in `errors`.
   - `time.sleep(interval)` between events.
4. **`producer.flush(30)`** blocks up to 30 seconds until every queued message is acknowledged, and returns how
   many are still undelivered.
5. **Check.** If any message was rejected or is still undelivered, the script exits with a non-zero code and
   `Kafka delivery failed: ...`. A job task running it fails instead of silently succeeding.

Without `flush()`, a script that exits right after `produce()` can lose the last messages still in the queue.

### Alternative: send to volume files instead of Kafka

When Kafka isn't available, `--sink files` writes each event as its own JSON file into a Unity Catalog volume, using
the Databricks SDK Files API:

```bash
.venv/Scripts/python.exe -m dab.telematics.simulator --sink files \
  --landing-path /Volumes/dab_dev/<landing schema>/telematics --count 20 --interval 1
```

Files are named `<UTC timestamp to the microsecond>_<chassis>.json`. The folder's parent volume must already exist.
The bundle creates one for this: the `telematics` volume in the landing schema (`resources/dab.schemas.yml`).
In dev mode the schema is named `dev_<your short name>_landing`.
`parse_telematics()` expects Kafka columns, so read these files with Auto Loader (see
[Ingesting from volume files](#ingesting-from-volume-files)), not with the Kafka code below.

### Running it as a job

`resources/telematics_simulator.job.yml` defines the `telematics_simulator` job. It runs the simulator from the
bundle's wheel through the `telematics_simulator` entry point, on serverless. It has no schedule, so you run it by
hand. Job parameters map to the flags:

| Job parameter | Default | Flag |
|---|---|---|
| `sink` | `kafka` | `--sink` (`kafka` or `files`) |
| `count` | `20` | `--count` |
| `interval` | `3` | `--interval` |
| `topic` | `telematics` | `--topic` |
| `secret_scope` | Bundle variable `kafka_secret_scope`, `kafka_azure_dev` unless a target overrides it | `--secret-scope` |
| `landing_path` | The `telematics` volume in the landing schema | `--landing-path` |

```bash
databricks bundle deploy -t azure-dev
databricks bundle run telematics_simulator -t azure-dev                            # to Kafka
databricks bundle run telematics_simulator -t azure-dev --params sink=files,count=50  # to the landing volume
```

## Ingesting data

### What Spark's Kafka source returns

`spark.readStream.format("kafka").options(**spark_kafka_options(...)).load()` returns one row per Kafka message:

| Column | Type | Content |
|---|---|---|
| `key` | binary | `CHS000003` as bytes |
| `value` | binary | The JSON event as bytes |
| `topic` | string | `telematics` |
| `partition` | int | Partition the message was written to |
| `offset` | long | Position within that partition; `(topic, partition, offset)` uniquely identifies a message |
| `timestamp` | timestamp | When the producer/broker stamped the message (not the event's own time) |
| `timestampType` | int | 0 = CreateTime, 1 = LogAppendTime |

### What `spark_kafka_options()` configures

| Option | Value | Why |
|---|---|---|
| `kafka.bootstrap.servers` | from secret | Where the cluster is |
| `kafka.security.protocol` | `SASL_SSL` | TLS + authentication, required by Confluent Cloud |
| `kafka.sasl.mechanism` | `PLAIN` | API key/secret login |
| `kafka.sasl.jaas.config` | `kafkashaded...PlainLoginModule required username=... password=...;` | The credentials. Databricks bundles a renamed ("shaded") Kafka client, so the class must start with `kafkashaded.`. Using the plain `org.apache.kafka...` name fails with a login-module-not-found error |
| `subscribe` | topic | Which topic to read |
| `startingOffsets` | `earliest` | First run only: read everything still retained in the topic. Later runs resume from the checkpoint |

### What `parse_telematics()` produces

```
value (binary) ──cast──▶ raw_json (string) ──from_json(map<string,string>)──▶ payload ──getItem──▶ columns
```

| Output column | Type | From |
|---|---|---|
| `chassis_number` | string | `payload["chassis_number"]` |
| `speed` | string | `payload["speed"]` |
| `latitude` | string | `payload["latitude"]` |
| `longitude` | string | `payload["longitude"]` |
| `event_timestamp` | string | `payload["event_timestamp"]` |
| `stream_metadata` | struct<topic, partition, offset, timestamp> | Kafka metadata |
| `raw_json` | string | The original message |

Everything is a **string on purpose**. This is the bronze layer: parsing as `map<string,string>` never fails on a
wrong type, so no record is dropped. A missing field becomes `NULL`; a message that isn't valid JSON gives
`NULL` in every field, but `raw_json` still holds it for inspection. Casting to real types happens in silver.

### Option A: Lakeflow pipeline (fits this bundle)

The `dab_etl` pipeline (`resources/dab_etl.pipeline.yml`) already loads every file in
`src/dab_etl/transformations/`, and installs this project with `--editable`, so `dab.telematics` is importable.
Adding a file like this creates a streaming bronze table:

```python
# src/dab_etl/transformations/telematics_bronze.py  (example; not in the repo yet)
from pyspark import pipelines as dp

from dab.telematics import kafka_config, parsing

TOPIC = "telematics"
servers, key, secret = kafka_config.read_credentials(dbutils, "kafka_dev")


@dp.table(comment="Raw telematics events from Kafka, all fields as strings.")
def telematics_bronze():
    raw = spark.readStream.format("kafka").options(**kafka_config.spark_kafka_options(servers, key, secret, TOPIC)).load()
    return parsing.parse_telematics(raw)
```

The pipeline manages the checkpoint. A normal update reads only new messages; a **full refresh** clears the table and
starts again from `earliest`.

Deploy and run:

```bash
databricks bundle deploy -t azure-dev
databricks bundle run dab_etl -t azure-dev
```

### Option B: plain Structured Streaming (notebook or job task)

```python
from databricks.sdk.runtime import dbutils, spark

from dab.telematics import kafka_config, parsing

catalog, schema, topic = "dab_dev", "<schema>", "telematics"
checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/telematics_bronze"

servers, key, secret = kafka_config.read_credentials(dbutils, "kafka_dev")
raw = spark.readStream.format("kafka").options(**kafka_config.spark_kafka_options(servers, key, secret, topic)).load()

(
    parsing.parse_telematics(raw)
    .writeStream.option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)  # read everything new, then stop; works on serverless
    .toTable(f"{catalog}.{schema}.telematics_bronze")
    .awaitTermination()
)
```

- **`checkpointLocation`** records which offsets were already written. Rerunning picks up where the last run
  stopped, with no duplicates. Delete the checkpoint folder only if you really want to re-read from `earliest`.
- **`availableNow=True`** processes all messages available now and then stops, which suits a scheduled job.
  Serverless supports this trigger but not continuous `processingTime` triggers. For always-on streaming use classic
  compute or a continuous pipeline.

### Ingesting from volume files

For events written with `--sink files`:

```python
bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/checkpoints/telematics_files_schema")
    .load(f"/Volumes/{catalog}/{schema}/files/telematics")
)
```

## Verifying end to end

1. Send a few events: `simulator --count 5 --interval 1`.
2. Run the pipeline or job.
3. Query the table:

```sql
SELECT chassis_number, speed, event_timestamp, stream_metadata.partition, stream_metadata.offset
FROM dab_dev.<schema>.telematics_bronze
ORDER BY stream_metadata.timestamp DESC
LIMIT 10;

-- Records that didn't parse
SELECT raw_json FROM dab_dev.<schema>.telematics_bronze WHERE chassis_number IS NULL;
```

The row count should grow by exactly the number of events sent per run. If it grows by more, the checkpoint was
reset or the table was fully refreshed.

## Next step: silver

Bronze keeps strings. A silver table casts them and drops bad rows:

```python
from pyspark.sql import functions as F

silver = (
    spark.readStream.table("telematics_bronze")
    .select(
        "chassis_number",
        F.col("speed").cast("double").alias("speed_kmh"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        "stream_metadata",
    )
    .where("chassis_number IS NOT NULL AND event_timestamp IS NOT NULL")
)
```

In a Lakeflow pipeline, use `@dp.expect_or_drop(...)` for these checks so dropped-row counts show up in the
pipeline UI.

## Current gaps

- No job or pipeline ingests the data yet. The ingest examples above are templates.
- With `--interval 0` on Windows, consecutive events often share the same `event_timestamp` (clock resolution). With
  `--sink files`, two events for the same car in the same clock tick overwrite each other's file. Use an interval
  above 0 for the file sink.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `ModuleNotFoundError: No module named 'confluent_kafka'` | Run `uv sync` to install the project dependencies into `.venv` |
| `Secret does not exist with scope: kafka_dev and key: ...` | Create the scope and all three keys (Prerequisites step 2), with exactly those names |
| `Kafka delivery failed: 20 undelivered` | Producer couldn't reach the broker: wrong bootstrap server, port 9092 blocked, or TLS failure. Locally on the FBISD network, try from Databricks to rule out the firewall (see `local_debugging_setup.md` for the certificate-inspection issue) |
| `Authentication failed` / `SASL authentication error` | Wrong API key/secret, or the key belongs to a different cluster |
| `TOPIC_AUTHORIZATION_FAILED` | The API key has no ACL for the topic |
| Spark: `No LoginModule found for org.apache.kafka...PlainLoginModule` | JAAS class missing the `kafkashaded.` prefix. Use `spark_kafka_options()` |
| Stream runs but the table stays empty | Nothing new since the last checkpoint, or the wrong topic. Send events, check `subscribe` |
| Every column `NULL` except `raw_json` | Message value isn't JSON. Look at `raw_json` |
