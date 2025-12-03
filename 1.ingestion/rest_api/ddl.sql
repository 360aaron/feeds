-- create database testdb;
-- create extension if not exists pgcrypto;
-- DROPS
drop table if exists staging_restapi_records;
drop table if exists restapi_records;
drop table if exists restapi_records_archive;


-- TODO:
-- * Add Data Services metadata: run id, source system, source format.
-- * Add source deletions handling.

-- REST API
create table if not exists staging_restapi_records (
    source_unique_id text,
    raw_record_hash text generated always as (
        encode(digest(raw_record::text, 'sha256'), 'hex')
    ) stored,
    raw_record text
);

create table if not exists restapi_records (
    source_unique_id text unique primary key,
    ingested_on timestamp default current_timestamp,
    archived_on timestamp null,
    raw_record_hash text,
    raw_record text,
    id serial
);

create table if not exists restapi_records_archive(
    source_unique_id text,
    ingested_on timestamp,
    archived_on timestamp,
    raw_record_hash text,
    raw_record text,
    id serial
);

create or replace function archive_restapi_records_on_update()
returns trigger
language plpgsql
as $$
begin
    if new.raw_record_hash is distinct from old.raw_record_hash then
        insert into restapi_records_archive (
            source_unique_id,
            ingested_on,
            archived_on,
            raw_record_hash,
            raw_record,
            id
        )
        values (
            old.source_unique_id,
            old.ingested_on,
            now(),
            old.raw_record_hash,
            old.raw_record,
            old.id
        );
    end if;
    return new;
end;
$$;

create trigger trigger_restapi_records_archive_on_update
before update on restapi_records
for each row
execute function archive_restapi_records_on_update();

-- Tests
update restapi_records set raw_record_hash = 'testhash123' where id = 1;