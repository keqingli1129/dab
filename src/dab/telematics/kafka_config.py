"""Connection settings for Confluent Cloud Kafka (SASL_SSL + PLAIN, API key/secret)."""

# Names of the three secrets that must exist in the Databricks secret scope (e.g. `kafka_dev`).
# Order matters: read_credentials() unpacks them in exactly this order.
SECRET_KEYS = ("bootstrap_servers", "api_key", "api_secret")


def read_credentials(dbutils, scope: str) -> tuple[str, str, str]:
    """Read (bootstrap_servers, api_key, api_secret) from a Databricks secret scope."""
    # Fetch each secret by name from the scope. `dbutils` is passed in (not imported) so this module
    # works in notebooks, jobs, pipelines and local Databricks Connect, and can be faked in tests.
    servers, key, secret = (dbutils.secrets.get(scope, name) for name in SECRET_KEYS)
    # Hand back the three values as a tuple: broker address, Confluent API key, Confluent API secret.
    return servers, key, secret


def spark_kafka_options(bootstrap_servers: str, api_key: str, api_secret: str, topic: str) -> dict[str, str]:
    """Options for spark.readStream.format("kafka") on Databricks."""
    # Databricks ships a shaded Kafka client, hence the "kafkashaded." class prefix.
    # JAAS config string: tells the Kafka client to log in with SASL/PLAIN using the API key as the
    # username and the API secret as the password. The trailing ";" is required by JAAS syntax.
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{api_key}" password="{api_secret}";'
    )
    # Options passed to spark.readStream.format("kafka").options(**...).
    # Keys prefixed with "kafka." are forwarded to the underlying Kafka consumer; the rest are Spark source options.
    return {
        # host:port of the Confluent Cloud cluster, e.g. "pkc-xxxxx.eastus.azure.confluent.cloud:9092".
        "kafka.bootstrap.servers": bootstrap_servers,
        # Encrypt traffic with TLS and authenticate with SASL (required by Confluent Cloud).
        "kafka.security.protocol": "SASL_SSL",
        # Use the PLAIN SASL mechanism (username/password = API key/secret).
        "kafka.sasl.mechanism": "PLAIN",
        # The login module + credentials built above.
        "kafka.sasl.jaas.config": jaas,
        # The topic to read from.
        "subscribe": topic,
        # On the very first run (no checkpoint yet) start from the oldest message still retained in the topic.
        # After that, the stream's checkpoint decides where to resume and this option is ignored.
        "startingOffsets": "earliest",
    }


def producer_config(bootstrap_servers: str, api_key: str, api_secret: str) -> dict[str, str]:
    """Config for confluent_kafka.Producer."""
    # Same connection settings as above, but in librdkafka (confluent_kafka) naming, which differs from
    # Spark's: no "kafka." prefix, and username/password are separate keys instead of a JAAS string.
    return {
        # host:port of the Confluent Cloud cluster.
        "bootstrap.servers": bootstrap_servers,
        # TLS + SASL authentication.
        "security.protocol": "SASL_SSL",
        # SASL/PLAIN mechanism (librdkafka spells it "mechanisms", plural).
        "sasl.mechanisms": "PLAIN",
        # Confluent API key.
        "sasl.username": api_key,
        # Confluent API secret.
        "sasl.password": api_secret,
    }
