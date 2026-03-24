# Python Data Ingestion

Data ingestion patterns for cloud storage and streaming sources using the modern `pyspark.pipelines` API.

**Import**: `from pyspark import pipelines as dp`

---

## Auto Loader (Cloud Files)

Auto Loader incrementally processes new data files as they arrive in cloud storage.

**IMPORTANT**: When using `spark.readStream.format("cloudFiles")`, you **must specify a `cloudFiles.schemaLocation`** for Auto Loader schema metadata.

### Schema Location Best Practice

**Never use the source data volume for schema storage** - this causes permission conflicts and pollutes your raw data.

**Recommended pattern:**
```
/Volumes/{catalog}/{schema}/{pipeline_name}_metadata/schemas/{table_name}
```

Configure in pipeline settings:
```yaml
configuration:
  schema_location_base: /Volumes/my_catalog/pipeline_metadata/orders_pipeline_metadata/schemas
```

### Basic Pattern

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Get schema location from pipeline configuration
schema_location_base = spark.conf.get("schema_location_base")

@dp.table(name="bronze_orders", cluster_by=["order_date"])
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_orders")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/my_catalog/my_schema/raw/orders/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
```

### File Formats

**JSON:**
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", f"{schema_location}/table") \
    .load("/Volumes/catalog/schema/raw/data/")
```

**CSV:**
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", f"{schema_location}/table") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .load("/Volumes/catalog/schema/raw/data/")
```

**Parquet:**
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{schema_location}/table") \
    .load("/Volumes/catalog/schema/raw/data/")
```

**Avro:**
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "avro") \
    .option("cloudFiles.schemaLocation", f"{schema_location}/table") \
    .load("/Volumes/catalog/schema/raw/data/")
```

### Schema Handling

**Infer column types:**
```python
.option("cloudFiles.inferColumnTypes", "true")
```

**Schema hints:**
```python
.option("cloudFiles.schemaHints", "order_id STRING, amount DECIMAL(10,2)")
```

**Schema evolution (permissive mode):**
```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("rescuedDataColumn", "_rescued_data")
```

### Rescue Data and Quarantine

Handle malformed records:

```python
schema_location_base = spark.conf.get("schema_location_base")

@dp.table(name="bronze_events", cluster_by=["ingestion_date"])
def bronze_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_events")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/catalog/schema/raw/events/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("_has_errors",
                   F.when(F.col("_rescued_data").isNotNull(), True)
                   .otherwise(False))
    )

@dp.table(name="bronze_events_quarantine")
def bronze_events_quarantine():
    return (
        spark.readStream.table("bronze_events")
        .filter(F.col("_has_errors") == True)
    )

@dp.table(name="silver_events_clean")
def silver_events_clean():
    return (
        spark.readStream.table("bronze_events")
        .filter(F.col("_has_errors") == False)
    )
```

---

## Streaming Sources

### Kafka

```python
@dp.table(name="bronze_kafka_events")
def bronze_kafka_events():
    kafka_brokers = spark.conf.get("kafka_brokers")
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "events-topic")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS event_key",
            "CAST(value AS STRING) AS event_value",
            "topic", "partition", "offset",
            "timestamp AS kafka_timestamp"
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

**With authentication:**
```python
spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", "events-topic") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";') \
    .load()
```

**Multiple topics:**
```python
.option("subscribe", "topic1,topic2,topic3")
```

### Azure Event Hub

```python
@dp.table(name="bronze_eventhub_events")
def bronze_eventhub_events():
    connection_string = spark.conf.get("eventhub_connection_string")
    consumer_group = spark.conf.get("consumer_group")
    return (
        spark.readStream
        .format("eventhubs")
        .option("eventhubs.connectionString", connection_string)
        .option("eventhubs.consumerGroup", consumer_group)
        .option("eventhubs.startingPosition", '{"offset": "-1", "isInclusive": true}')
        .load()
        .selectExpr(
            "CAST(body AS STRING) AS event_body",
            "enqueuedTime AS event_time",
            "offset",
            "sequenceNumber"
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

### AWS Kinesis

```python
@dp.table(name="bronze_kinesis_events")
def bronze_kinesis_events():
    stream_name = spark.conf.get("stream_name")
    aws_region = spark.conf.get("aws_region")
    return (
        spark.readStream
        .format("kinesis")
        .option("streamName", stream_name)
        .option("region", aws_region)
        .option("initialPosition", "LATEST")
        .load()
        .selectExpr(
            "CAST(data AS STRING) AS event_data",
            "partitionKey",
            "sequenceNumber",
            "approximateArrivalTimestamp AS arrival_time"
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

### Parse JSON from Streaming Sources

```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType

# Define schema
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("properties", MapType(StringType(), StringType()))
])

@dp.table(name="silver_kafka_parsed")
def silver_kafka_parsed():
    return (
        spark.readStream.table("bronze_kafka_events")
        .withColumn("event_data", F.from_json("event_value", event_schema))
        .select(
            "event_data.event_id",
            "event_data.event_type",
            "event_data.user_id",
            F.col("event_data.timestamp").alias("event_timestamp"),
            "event_data.properties",
            "kafka_timestamp",
            "_ingested_at"
        )
    )
```

---

## Authentication

### Using Databricks Secrets

```python
# Access secrets via dbutils
username = dbutils.secrets.get(scope="kafka", key="username")
password = dbutils.secrets.get(scope="kafka", key="password")
```

### Using Pipeline Parameters

```python
kafka_brokers = spark.conf.get("kafka_brokers")
input_path = spark.conf.get("input_path")
```

Define in pipeline configuration:
```yaml
configuration:
  kafka_brokers: "broker1:9092,broker2:9092"
  input_path: /Volumes/my_catalog/my_schema/raw
```

---

## Best Practices

### 1. Always Add Ingestion Metadata

```python
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_source_file", F.col("_metadata.file_path"))
.withColumn("_file_timestamp", F.col("_metadata.file_modification_time"))
```

### 2. Use Schema Location for Auto Loader

```python
# Always specify schema location (required for Python)
.option("cloudFiles.schemaLocation", f"{schema_location_base}/table_name")
```

### 3. Handle Rescue Data

```python
.option("rescuedDataColumn", "_rescued_data")
.withColumn("_has_errors", F.col("_rescued_data").isNotNull())
```

### 4. Starting Positions

```python
# Development (new data only)
.option("startingOffsets", "latest")

# Backfill (all available data)
.option("startingOffsets", "earliest")
```

---

## Complete Example

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Configuration
schema_location_base = spark.conf.get("schema_location_base")

@dp.table(
    name="bronze_orders",
    comment="Raw orders from cloud storage",
    cluster_by=["order_date"]
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_orders")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/my_catalog/my_schema/raw/orders/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("_has_errors", F.col("_rescued_data").isNotNull())
    )

@dp.table(name="bronze_orders_quarantine")
def bronze_orders_quarantine():
    return spark.readStream.table("bronze_orders").filter("_has_errors = true")

@dp.table(name="silver_orders", cluster_by=["customer_id", "order_date"])
@dp.expect_or_drop("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        .filter("_has_errors = false")
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        .select("order_id", "customer_id", "amount", "order_date", "_ingested_at")
    )
```

---

## Common Issues

| Issue | Solution |
|-------|----------|
| Missing schemaLocation | Always specify `cloudFiles.schemaLocation` for Auto Loader |
| Schema location permission error | Use dedicated metadata volume, not source data volume |
| Files not picked up | Verify format matches files and path is correct |
| Schema evolution breaking | Enable `rescuedDataColumn` and monitor `_rescued_data` |
| Kafka lag increasing | Check downstream bottlenecks, increase parallelism |
| Duplicate events | Implement deduplication in silver layer |
