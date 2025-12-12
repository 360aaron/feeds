# Publishing

Our current pattern (publish, wait for ack, mark processed/retries) gives precise “producer got an ack” semantics. The tradeoff is that we own and mantain DB polling, retries, delivery callbacks, and Schema Registry integration.

We can offloads publishing, retries, offsets, and Avro handling by implementing a connector pattern instead. Where we set up a JDBC Connector via configuration and let Kafka do everything.

In fact, we can reduce the scope of our dispatcher almost completely by validating schema during normalization and only outboxing fully compliant events. This significantly decreases effort on DQL implementation and coordination.

## How to run

```
# 1. Start services
docker compose up -d --build

sleep 60

docker compose ps

# 2. Enter tools container
docker compose exec tools bash

# 3. Create topic
kafka-topics --bootstrap-server broker:29092 --create --topic days --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server broker:29092 --list

exit

# 4. Register schema (copy day_schema.avsc into container first)
SCHEMA=$(cat day_schema.avsc \
  | sed 's/"/\\"/g' | tr -d '\n')

curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": \"${SCHEMA}\"}" \
  http://localhost:8081/subjects/days-value/versions

curl http://localhost:8081/subjects

curl http://localhost:8081/subjects/days-value/versions/latest

# 5. Create connector (from your host)
curl -X POST -H "Content-Type: application/json" \
  --data @connector-config.json \
  http://localhost:8083/connectors

curl http://localhost:8083/connectors/days-connector/status | jq

# 6. Consume messages
docker compose exec schema-registry bash

kafka-avro-console-consumer --bootstrap-server broker:29092 --topic days \
  --from-beginning --property schema.registry.url=http://schema-registry:8081

docker compose down -v
```

Local cluster
```
psql -U <INSERT> -d <INSERT> -c "SHOW config_file;"

# REQUIRED for Debezium
wal_level = logical

# Also add these if not present:
max_replication_slots = 10
max_wal_senders = 10

psql -U aaron -d postgres -c "
CREATE ROLE replication WITH LOGIN REPLICATION ENCRYPTED PASSWORD 'password';
ALTER USER replication WITH PASSWORD 'password';
GRANT CONNECT ON DATABASE postgres TO replication;
GRANT USAGE ON SCHEMA public TO replication;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication;
"

psql -U aaron -d postgres -c "
ALTER TABLE public.normalized_days_sparse_outbox REPLICA IDENTITY FULL;
"
```