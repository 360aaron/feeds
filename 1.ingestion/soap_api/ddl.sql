-- DROPS
drop table if exists staging_soapapi_records;
drop table if exists soapapi_records;
drop table if exists soapapi_records_archive;

-- create database testdb;

-- TODO:
-- * Add Data Services metadata: run id, source system, source format.
-- * Add source deletions handling.

-- SOAP API
create table if not exists staging_soapapi_records (
    source_unique_id text,
    raw_record_hash text generated always as (md5(raw_record)) stored,
    raw_record text
);

create table if not exists soapapi_records (
    source_unique_id text unique primary key,
    created_on timestamp default current_timestamp,
    archived_on timestamp null,
    raw_record_hash text,
    raw_record text,
    id serial
);

create table if not exists soapapi_records_archive(
    source_unique_id text,
    created_on timestamp,
    archived_on timestamp,
    raw_record_hash text,
    raw_record text,
    id serial
);

create or replace function archive_soapapi_records_on_update()
returns trigger
language plpgsql
as $$
begin
    if new.raw_record_hash is distinct from old.raw_record_hash then
        insert into soapapi_records_archive (
            source_unique_id,
            created_on,
            archived_on,
            raw_record_hash,
            raw_record,
            id
        )
        values (
            old.source_unique_id,
            old.created_on,
            now(),
            old.raw_record_hash,
            old.raw_record,
            old.id
        );
    end if;
    return new;
end;
$$;

create trigger trigger_soapapi_records_archive_on_update
before update on soapapi_records
for each row
execute function archive_soapapi_records_on_update();
