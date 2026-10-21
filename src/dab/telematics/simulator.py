"""Send fake telematics events to Kafka, or print them with --dry-run."""

# argparse: parse command-line flags such as --topic and --count.
import argparse

# random: random number generator handed to events.generate_event().
import random

# time: time.sleep() between events.
import time

# Callable: used to describe the shape of a "send" function.
from collections.abc import Callable

# UTC/datetime: timestamps for events and file names.
from datetime import UTC, datetime

# events builds the fake readings; kafka_config builds the Kafka connection settings.
from dab.telematics import events, kafka_config

# Type alias: any function that takes (topic, key bytes, value bytes) and returns nothing.
# run() only depends on this shape, so the same loop can print, write files, or produce to Kafka.
Send = Callable[[str, bytes, bytes], None]  # (topic, key, value)


def run(send: Send, topic: str, count: int, interval: float, rng: random.Random) -> int:
    """Generate `count` events and hand each one to `send`, waiting `interval` seconds between them."""
    # Repeat once per event.
    for i in range(count):
        # Make one fake reading for a random car, stamped with the current UTC time.
        event = events.generate_event(rng, events.DEFAULT_CHASSIS_NUMBERS, datetime.now(UTC))
        # Deliver it. Key = chassis number as bytes, so Kafka puts all events for one car in the
        # same partition (keeping them in order); value = the event as JSON bytes.
        send(topic, event["chassis_number"].encode(), events.to_json(event))
        # Pause between events, but not after the last one.
        if i < count - 1:
            time.sleep(interval)
    # Report how many events were handed to `send`.
    return count


def _print_send(topic: str, key: bytes, value: bytes) -> None:
    # The --dry-run "send": print instead of sending. flush=True makes each line show up immediately.
    print(f"[dry-run] {topic} key={key.decode()} {value.decode()}", flush=True)


def _file_send(landing_path: str) -> Send:
    """Return a `send` that writes each event as one JSON file into a Unity Catalog volume."""
    # Imported here so --dry-run and --sink kafka don't need the Databricks SDK loaded.
    # BytesIO wraps bytes as a file-like object for the upload call.
    from io import BytesIO

    # WorkspaceClient talks to the Databricks REST API using your CLI profile / environment auth.
    from databricks.sdk import WorkspaceClient

    # The Files API client, which can write into /Volumes/... paths.
    files = WorkspaceClient().files

    def send(topic: str, key: bytes, value: bytes) -> None:
        # Unique, sortable file name: microsecond UTC timestamp + chassis number,
        # e.g. "20261021T170512345678_CHS000003.json". `topic` is not used for files.
        name = f"{datetime.now(UTC):%Y%m%dT%H%M%S%f}_{key.decode()}.json"
        # Upload the JSON (plus a newline, so it is valid JSON Lines) into the volume folder.
        files.upload(f"{landing_path}/{name}", BytesIO(value + b"\n"), overwrite=True)

    # Return the inner function; it keeps a reference to `files` and `landing_path`.
    return send


def _send_to_kafka(args: argparse.Namespace) -> None:
    # Kafka producer client from the confluent-kafka package (imported lazily: only needed for this sink).
    from confluent_kafka import Producer

    # dbutils: on Databricks it's the runtime's own; locally it calls the workspace through the SDK.
    from databricks.sdk.runtime import dbutils

    # Read broker address, API key and API secret from the secret scope (default "kafka_dev").
    servers, key, secret = kafka_config.read_credentials(dbutils, args.secret_scope)
    # Create the producer with SASL_SSL/PLAIN settings for Confluent Cloud.
    producer = Producer(kafka_config.producer_config(servers, key, secret))
    # Collects any per-message delivery errors reported by Kafka.
    errors = []

    def on_delivery(err, msg):
        # Called by the producer once Kafka has accepted (err is None) or rejected (err set) a message.
        if err is not None:
            # Remember the failure so we can exit with an error at the end.
            errors.append(err)

    def send(topic: str, key_bytes: bytes, value: bytes) -> None:
        # Queue the message. produce() is asynchronous: it returns before Kafka confirms delivery.
        producer.produce(topic, key=key_bytes, value=value, on_delivery=on_delivery)
        # Let the producer handle network events and fire any pending on_delivery callbacks, without waiting.
        producer.poll(0)

    # Generate and queue all events using the Kafka `send` above.
    run(send, args.topic, args.count, args.interval, random.Random())
    # Block (up to 30 s) until every queued message is delivered; returns how many are still undelivered.
    undelivered = producer.flush(30)
    # Fail loudly (non-zero exit code) if anything was rejected or never delivered.
    if errors or undelivered:
        raise SystemExit(f"Kafka delivery failed: {undelivered} undelivered, errors: {errors[:3]}")
    # Success message.
    print(f"Sent {args.count} events to topic {args.topic}")


def main(argv: list[str] | None = None) -> None:
    # Command-line interface. `argv` lets tests pass flags directly; None means "use sys.argv".
    parser = argparse.ArgumentParser(description="Send fake car telematics events to Kafka.")
    # Kafka topic to write to.
    parser.add_argument("--topic", default="telematics")
    # How many events to send.
    parser.add_argument("--count", type=int, default=20)
    # Delay between events, in seconds.
    parser.add_argument("--interval", type=float, default=3.0, help="seconds between events")
    # Databricks secret scope holding bootstrap_servers / api_key / api_secret.
    parser.add_argument("--secret-scope", default="kafka_dev")
    # Where events go: a Kafka topic, or JSON files in a volume (useful when Kafka isn't available).
    parser.add_argument("--sink", choices=["kafka", "files"], default="kafka")
    # Target volume folder, only used with --sink files.
    parser.add_argument(
        "--landing-path", help="volume folder for --sink files, e.g. /Volumes/<catalog>/<schema>/files/telematics"
    )
    # Print events instead of sending them; needs no credentials or network.
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending them")
    # Parse the flags into `args` (args.topic, args.count, args.secret_scope, ...).
    args = parser.parse_args(argv)

    # --dry-run wins over --sink: just print.
    if args.dry_run:
        run(_print_send, args.topic, args.count, args.interval, random.Random())
    # File sink: write JSON files into a volume.
    elif args.sink == "files":
        # --landing-path is mandatory here; parser.error() prints usage and exits with code 2.
        if not args.landing_path:
            parser.error("--landing-path is required with --sink files")
        run(_file_send(args.landing_path), args.topic, args.count, args.interval, random.Random())
        print(f"Wrote {args.count} event files to {args.landing_path}")
    # Default: produce to Kafka.
    else:
        _send_to_kafka(args)


# Run main() only when executed as a script (python -m dab.telematics.simulator), not when imported.
if __name__ == "__main__":
    main()
