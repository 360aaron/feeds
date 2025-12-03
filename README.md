# Feeds

“Give me six hours to chop down a tree, and I will spend the first four sharpening the axe.”

## Executive summary

Data Services currently ingests data from about ten sources across four paradigms, normalizes that data, applies business logic, writes to production‑facing tables, and triggers cross‑service synchronizations. This makes launching new feeds cumbersome and supporting data queries slower than desired.

To address this, the proposal is to revamp the codebase and data flow with four main requirements:

1. Avoid tight coupling with OLTP production tables by using dedicated ingestion and serving schemas.
2. Improve DORA metrics: lead time for changes, deployment frequency, change failure rate, and recovery time from failed deployments.
3. Provide unified, searchable logs (for example in DataDog) across ingestion, normalization, and publishing.
4. Provide replayability through versioned storage and deterministic transformations.

To accomplish this, the proposal is to pivot from trying to do everything in one place to “Weniger, aber besser” (“Less, but better”): simpler responsibilities per layer, smarter tables, and more work delegated to managed infrastructure.

## Ingestion

- There are four main source types:
  - REST APIs (typically paginated)
  - SOAP APIs (often per record or small batch)
  - SFTP servers (whole files)
  - BLOB/MIME sources (whole files via object or file storage)

- One option is to ingest into blob storage to reduce storage size and enable region/chunk‑level hashing, since all sources become files; the main tradeoffs are that ad‑hoc querying becomes harder without a lakehouse layer, and row‑level checksums then require additional metadata structures.

- For sources that cannot provide deltas or change data, ingestion must periodically scan all available records, which increases cost and latency for feeds with large histories.

- A straightforward, observable pattern for ingestion is a three‑table approach:
  1. A staging table that receives raw payloads and generates row‑level hashes over a canonical representation.
  2. A live raw table that stores the latest raw version per business key.
  3. An archive table that stores prior raw versions for audit and replay.

- Archiving can be automated using triggers and functions: when an upsert detects a hash change for a record in the live raw table, the previous version is inserted into the archive table before the new one is written.

- This approach allows logic reuse because all sources end up as tabular, hashed rows with a consistent contract, regardless of original format or protocol.

- Logic for deletions and full replay can be added using the same pattern (for example, representing deletions as tombstone records and using archive plus hashes to rebuild downstream state), but kept separate from the core ingestion flow to preserve simplicity.

- Serverless scheduled jobs and managed connectors (for example, cloud schedulers, secrets managers, and preconfigured JDBC or API connections) should be sufficient to orchestrate ingestion while minimizing custom glue code.

## Normalization

- Normalization becomes the process of transforming live raw records that were created or updated into business‑relevant entities. For example, a single Leonardo raw record might be split into `normalized_images`, `normalized_image_tags`, and `normalized_image_captions` tables.

- At this step, hashes are generated in memory over canonicalized entities rather than by the target tables, because each logical entity may be composed from multiple raw fields or child records.

- Instead of adding another staging layer here, normalized tables can emit changes via a transactional outbox (implemented with triggers or functions) that records what changed in a dedicated outbox table for downstream publishing. This follows the transactional outbox pattern to avoid dual‑write issues between the database and the message bus.

## Publishing

- Publishing can largely be handled through configurable connectors that read from the outbox tables and write to downstream systems such as Kafka, data warehouses, or other services.

- With the outbox pattern, publishing is reliable and at‑least‑once, and any failures in the message bus or consumers do not compromise the integrity of the source database.

## Conclusion

This approach shifts more responsibility into well‑designed tables and standard patterns, reducing ETL logic to moving data in three focused steps: from sources to raw tables, from raw tables to normalized tables, and from outboxes to Kafka or other sinks.

Only new and changed records are published, with full traceability of what was ingested, how it was split, and what was emitted, including version history and lifespans for each record.

By leaning on serverless infrastructure and managed services for databases, secrets, connectivity, job hosting, compute, and storage, the bulk of the complexity moves out of custom application code; in the proof of concept, over half of the code is something other than Python: YAML, SQL and Terraform.
