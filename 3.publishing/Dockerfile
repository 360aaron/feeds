FROM confluentinc/cp-kafka-connect:7.5.0

USER root

# Install Debezium PostgreSQL connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.4.2

USER appuser