# SQL Data Ingestion

Data ingestion patterns for cloud storage and streaming sources.

---

## Auto Loader (Cloud Files)

Auto Loader incrementally processes new data files as they arrive. Use `STREAM read_files()` in streaming table queries.

### Basic Pattern

```sql
CREATE OR REPLACE STREAMING TABLE bronze_orders AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_timestamp
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/orders/',
  format => 'json',
  schemaHints => 'order_id STRING, amount DECIMAL(10,2)'
);
```

### File Formats

**JSON:**
```sql
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/data/',
  format => 'json',
  schemaHints => 'id STRING, timestamp TIMESTAMP'
)
```

**CSV:**
```sql
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/data/',
  format => 'csv',
  schemaHints => 'id STRING, name STRING, amount DECIMAL(10,2)',
  header => true,
  delimiter => ','
)
```

**Parquet** (schema auto-inferred):
```sql
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/data/',
  format => 'parquet'
)
```

**Avro:**
```sql
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/events/',
  format => 'avro',
  schemaHints => 'event_id STRING, event_time TIMESTAMP'
)
```

### Schema Handling

**Explicit hints** (recommended for production):
```sql
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/sales/',
  format => 'json',
  schemaHints => 'sale_id STRING, customer_id STRING, amount DECIMAL(10,2), sale_date DATE'
)
```

**Schema evolution** (permissive mode):
```sql
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/customers/',
  format => 'json',
  schemaHints => 'customer_id STRING, email STRING',
  mode => 'PERMISSIVE'
)
```

### Rescue Data and Quarantine

Handle malformed records with `_rescued_data`:

```sql
-- Bronze: Flag records with parsing errors
CREATE OR REPLACE STREAMING TABLE bronze_events AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  CASE WHEN _rescued_data IS NOT NULL THEN TRUE ELSE FALSE END AS _has_errors
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/raw/events/',
  format => 'json',
  schemaHints => 'event_id STRING, event_time TIMESTAMP'
);

-- Quarantine for investigation
CREATE OR REPLACE STREAMING TABLE bronze_events_quarantine AS
SELECT * FROM STREAM bronze_events WHERE _rescued_data IS NOT NULL;

-- Clean data for downstream
CREATE OR REPLACE STREAMING TABLE silver_events_clean AS
SELECT * FROM STREAM bronze_events WHERE _rescued_data IS NULL;
```

---

## Streaming Sources

### Kafka

```sql
CREATE OR REPLACE STREAMING TABLE bronze_kafka_events AS
SELECT
  CAST(key AS STRING) AS event_key,
  CAST(value AS STRING) AS event_value,
  topic,
  partition,
  offset,
  timestamp AS kafka_timestamp,
  current_timestamp() AS _ingested_at
FROM read_stream(
  format => 'kafka',
  `kafka.bootstrap.servers` => '${kafka_brokers}',
  subscribe => 'events-topic',
  startingOffsets => 'latest',
  `kafka.security.protocol` => 'SASL_SSL',
  `kafka.sasl.mechanism` => 'PLAIN',
  `kafka.sasl.jaas.config` => 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="${kafka_username}" password="${kafka_password}";'
);
```

**Multiple topics:**
```sql
FROM read_stream(
  format => 'kafka',
  `kafka.bootstrap.servers` => '${kafka_brokers}',
  subscribe => 'topic1,topic2,topic3',
  startingOffsets => 'latest'
)
```

### Azure Event Hub

```sql
CREATE OR REPLACE STREAMING TABLE bronze_eventhub_events AS
SELECT
  CAST(body AS STRING) AS event_body,
  enqueuedTime AS event_time,
  offset,
  sequenceNumber,
  current_timestamp() AS _ingested_at
FROM read_stream(
  format => 'eventhubs',
  `eventhubs.connectionString` => '${eventhub_connection_string}',
  `eventhubs.consumerGroup` => '${consumer_group}',
  startingPosition => 'latest'
);
```

### AWS Kinesis

```sql
CREATE OR REPLACE STREAMING TABLE bronze_kinesis_events AS
SELECT
  CAST(data AS STRING) AS event_data,
  partitionKey,
  sequenceNumber,
  approximateArrivalTimestamp AS arrival_time,
  current_timestamp() AS _ingested_at
FROM read_stream(
  format => 'kinesis',
  `kinesis.streamName` => '${stream_name}',
  `kinesis.region` => '${aws_region}',
  `kinesis.startingPosition` => 'LATEST'
);
```

### Parse JSON from Streaming Sources

```sql
-- Parse JSON from Kafka value
CREATE OR REPLACE STREAMING TABLE silver_kafka_parsed AS
SELECT
  from_json(
    event_value,
    'event_id STRING, event_type STRING, user_id STRING, timestamp TIMESTAMP, properties MAP<STRING, STRING>'
  ) AS event_data,
  kafka_timestamp,
  _ingested_at
FROM STREAM bronze_kafka_events;

-- Flatten parsed JSON
CREATE OR REPLACE STREAMING TABLE silver_kafka_flattened AS
SELECT
  event_data.event_id,
  event_data.event_type,
  event_data.user_id,
  event_data.timestamp AS event_timestamp,
  event_data.properties,
  kafka_timestamp,
  _ingested_at
FROM STREAM silver_kafka_parsed;
```

---

## Authentication

### Using Databricks Secrets

**Kafka:**
```sql
`kafka.sasl.jaas.config` => 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{{secrets/kafka/username}}" password="{{secrets/kafka/password}}";'
```

**Event Hub:**
```sql
`eventhubs.connectionString` => '{{secrets/eventhub/connection-string}}'
```

### Using Pipeline Variables

```sql
`kafka.bootstrap.servers` => '${kafka_brokers}'
```

Define in pipeline configuration:
```yaml
configuration:
  kafka_brokers: "broker1:9092,broker2:9092"
```

---

## Best Practices

### 1. Always Add Ingestion Metadata

```sql
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_timestamp
FROM STREAM read_files(...)
```

### 2. Use Schema Hints for Production

```sql
-- Explicit schema prevents surprises
FROM STREAM read_files(
  '/Volumes/my_catalog/my_schema/data/',
  format => 'json',
  schemaHints => 'id STRING, amount DECIMAL(10,2), date DATE'
)

-- Avoid fully inferred schemas in production
-- FROM STREAM read_files('/data/', format => 'json')
```

### 3. Handle Rescue Data

```sql
-- Route errors to quarantine, clean to downstream
CREATE OR REPLACE STREAMING TABLE bronze_quarantine AS
SELECT * FROM STREAM bronze_data WHERE _has_errors;

CREATE OR REPLACE STREAMING TABLE silver_data AS
SELECT * FROM STREAM bronze_data WHERE NOT _has_errors;
```

### 4. Starting Positions

- **Development**: `startingOffsets => 'latest'` (new data only)
- **Backfill**: `startingOffsets => 'earliest'` (all available data)
- **Recovery**: Checkpoints handle automatically

---

## Common Issues

| Issue | Solution |
|-------|----------|
| Files not picked up | Verify format matches files and path is correct |
| Schema evolution breaking | Use `mode => 'PERMISSIVE'` and monitor `_rescued_data` |
| Kafka lag increasing | Check downstream bottlenecks, increase parallelism |
| Duplicate events | Implement deduplication in silver layer |
| Parsing errors | Use rescue data pattern to quarantine malformed records |
| Missing STREAM keyword | Use `FROM STREAM read_files(...)` for streaming tables |
