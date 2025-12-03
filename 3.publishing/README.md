# Publishing

Our current pattern (publish, wait for ack, mark processed/retries) gives precise “producer got an ack” semantics. The tradeoff is that we own and mantain DB polling, retries, delivery callbacks, and Schema Registry integration.

We can offloads publishing, retries, offsets, and Avro handling to the platform by implementing a connector pattern instead. Where we set up a JDBC Connector via configuration and let Kafka do everything.

## How to run

```
docker compose up -d
```

**Register schema**

```
SCHEMA=$(cat day_schema.avsc \
  | sed 's/"/\\"/g' | tr -d '\n')

curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": \"${SCHEMA}\"}" \
  http://localhost:8081/subjects/days-value/versions

curl http://localhost:8081/subjects

curl http://localhost:8081/subjects/days-value/versions/latest
```

**Create connector(s)**

```
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data '{
    "name": "days-connector",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "tasks.max": "1",
      "connection.url": "jdbc:postgresql://host.docker.internal:5432/testdb",
      "connection.user": "aaron",
      "connection.password": "",
      "mode": "incrementing",
      "incrementing.column.name": "id",
      "table.whitelist": "normalized_days_sparse_outbox",
      "topic.prefix": "pg_",
      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "http://schema-registry:8081",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      "poll.interval.ms": "10000"
    }
  }'

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data '{
    "name": "hours-connector",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "tasks.max": "1",
      "connection.url": "jdbc:postgresql://host.docker.internal:5432/testdb",
      "connection.user": "aaron",
      "connection.password": "",
      "mode": "incrementing",
      "incrementing.column.name": "id",
      "table.whitelist": "normalized_hours_sparse_outbox",
      "topic.prefix": "pg_",
      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "http://schema-registry:8081",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      "poll.interval.ms": "10000"
    }
  }'

curl http://localhost:8083/connectors/hours-connector/status | jq
```

**Consume messages**

```
docker compose exec schema-registry bash

kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic pg_normalized_days_sparse_outbox \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081 \
  | grep -v '^\['

kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic pg_normalized_hours_sparse_outbox \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081 \
  | grep -v '^\['
```

**Clean up**

```
docker compose down -v
```