create table if not exists restapi_validation_errors (
    source_unique_id text,
    target_table text,
    error_details text,
    detected_on timestamp default current_timestamp,
    id serial
);

