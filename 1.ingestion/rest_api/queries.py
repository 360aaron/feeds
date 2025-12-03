# queries.py

TRUNCATE_STAGING = "TRUNCATE TABLE staging_restapi_records;"

INSERT_STAGING = """
INSERT INTO staging_restapi_records (source_unique_id, raw_record)
VALUES (%s, %s);
"""

UPSERT_RESTAPI_RECORDS = """
INSERT INTO restapi_records (source_unique_id, raw_record, raw_record_hash, ingested_on)
SELECT source_unique_id, raw_record, raw_record_hash, now()
FROM staging_restapi_records
ON CONFLICT (source_unique_id) DO UPDATE
SET raw_record       = EXCLUDED.raw_record,
    raw_record_hash  = EXCLUDED.raw_record_hash
WHERE restapi_records.raw_record_hash IS DISTINCT FROM EXCLUDED.raw_record_hash;
"""
