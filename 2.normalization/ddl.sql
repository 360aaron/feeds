/* 

- Stage valid ingested records
- Upsert into live normalized tables; if there are changes:
    1. Archive current version of the record
    2. Outbox changes for downstream systems

- TODO: Add outbox_archive(s)

*/
-- DROPS
drop table if exists staging_normalized_days;
drop table if exists staging_normalized_astrology;
drop table if exists staging_normalized_hours;

drop table if exists normalized_days;
drop table if exists normalized_astrology;
drop table if exists normalized_hours;

drop table if exists normalized_days_archive;
drop table if exists normalized_astrology_archive;
drop table if exists normalized_hours_archive;

drop table if exists normalized_days_sparse_outbox;
drop table if exists normalized_astrology_sparse_outbox;
drop table if exists normalized_hours_sparse_outbox;

-- Staging
create table if not exists staging_normalized_days (
    --
    id serial,
    source_unique_id text,
    created_on timestamp,
    --
    date date,
    date_epoch int,
    maxtemp_c int,
    maxtemp_f int,
    mintemp_c int,
    mintemp_f int,
    avgtemp_c int,
    avgtemp_f int,
    maxwind_mph int,
    maxwind_kph int,
    totalprecip_mm int,
    totalprecip_in int,
    totalsnow_cm int,
    avgvis_km int,
    avgvis_miles int,
    avghumidity int,
    daily_will_it_rain int,
    daily_chance_of_rain int,
    daily_will_it_snow int,
    daily_chance_of_snow int,
    condition_text text,
    condition_icon text,
    condition_code int,
    uv int
);

create table if not exists staging_normalized_astrology (
    --
    id serial,
    parent_id text,
    created_on timestamp,
    --
    sunrise text,
    sunset text,
    moonrise text,
    moonset text,
    moon_phase text,
    moon_illumination int
);

create table if not exists staging_normalized_hours (
    --
    id serial,
    parent_id text,
    created_on timestamp,
    --
    time_epoch int,
    time text,
    temp_c int,
    temp_f int,
    is_day int,
    condition_text text,
    condition_icon text,
    condition_code int,
    wind_mph int,
    wind_kph int,
    wind_degree int,
    wind_dir text,
    pressure_mb int,
    pressure_in int,
    precip_mm int,
    precip_in int,
    snow_cm int,
    humidity int,
    cloud int,
    feelslike_c int,
    feelslike_f int,
    windchill_c int,
    windchill_f int,
    heatindex_c int,
    heatindex_f int,
    dewpoint_c int,
    dewpoint_f int,
    will_it_rain int,
    chance_of_rain int,
    will_it_snow int,
    chance_of_snow int,
    vis_km int,
    vis_miles int,
    gust_mph int,
    gust_kph int,
    uv int
);

-- Live
create table if not exists normalized_days (
    --
    source_unique_id text primary key,
    id serial,
    created_on timestamp,
    archived_on timestamp,
    --
    date date,
    date_epoch int,
    maxtemp_c int,
    maxtemp_f int,
    mintemp_c int,
    mintemp_f int,
    avgtemp_c int,
    avgtemp_f int,
    maxwind_mph int,
    maxwind_kph int,
    totalprecip_mm int,
    totalprecip_in int,
    totalsnow_cm int,
    avgvis_km int,
    avgvis_miles int,
    avghumidity int,
    daily_will_it_rain int,
    daily_chance_of_rain int,
    daily_will_it_snow int,
    daily_chance_of_snow int,
    condition_text text,
    condition_icon text,
    condition_code int,
    uv int
);

create table if not exists normalized_astrology (
    --
    id serial primary key,
    parent_id text,
    created_on timestamp,
    archived_on timestamp,
    --
    sunrise text,
    sunset text,
    moonrise text,
    moonset text,
    moon_phase text,
    moon_illumination int
);

create table if not exists normalized_hours (
    --
    id serial primary key,  
    parent_id text,
    created_on timestamp,
    archived_on timestamp,
    --
    time_epoch int,
    time text,
    temp_c int,
    temp_f int,
    is_day int,
    condition_text text,
    condition_icon text,
    condition_code int,
    wind_mph int,
    wind_kph int,
    wind_degree int,
    wind_dir text,
    pressure_mb int,
    pressure_in int,
    precip_mm int,
    precip_in int,
    snow_cm int,
    humidity int,
    cloud int,
    feelslike_c int,
    feelslike_f int,
    windchill_c int,
    windchill_f int,
    heatindex_c int,
    heatindex_f int,
    dewpoint_c int,
    dewpoint_f int,
    will_it_rain int,
    chance_of_rain int,
    will_it_snow int,
    chance_of_snow int,
    vis_km int,
    vis_miles int,
    gust_mph int,
    gust_kph int,
    uv int
);

-- Archive
create table if not exists normalized_days_archive (
    --
    id serial primary key,
    source_unique_id text,
    created_on timestamp,
    archived_on timestamp,
    --
    date date,
    date_epoch int,
    maxtemp_c int,
    maxtemp_f int,
    mintemp_c int,
    mintemp_f int,
    avgtemp_c int,
    avgtemp_f int,
    maxwind_mph int,
    maxwind_kph int,
    totalprecip_mm int,
    totalprecip_in int,
    totalsnow_cm int,
    avgvis_km int,
    avgvis_miles int,
    avghumidity int,
    daily_will_it_rain int,
    daily_chance_of_rain int,
    daily_will_it_snow int,
    daily_chance_of_snow int,
    condition_text text,
    condition_icon text,
    condition_code int,
    uv int
);

create table if not exists normalized_astrology_archive (
    --
    id serial primary key,
    parent_id text,
    created_on timestamp,
    --
    archived_on timestamp,
    sunrise text,
    sunset text,
    moonrise text,
    moonset text,
    moon_phase text,
    moon_illumination int
);

create table if not exists normalized_hours_archive (
    --
    id serial primary key,
    parent_id text,
    created_on timestamp,
    archived_on timestamp,
    --
    time_epoch int,
    time text,
    temp_c int,
    temp_f int,
    is_day int,
    condition_text text,
    condition_icon text,
    condition_code int,
    wind_mph int,
    wind_kph int,
    wind_degree int,
    wind_dir text,
    pressure_mb int,
    pressure_in int,
    precip_mm int,
    precip_in int,
    snow_cm int,
    humidity int,
    cloud int,
    feelslike_c int,
    feelslike_f int,
    windchill_c int,
    windchill_f int,
    heatindex_c int,
    heatindex_f int,
    dewpoint_c int,
    dewpoint_f int,
    will_it_rain int,
    chance_of_rain int,
    will_it_snow int,
    chance_of_snow int,
    vis_km int,
    vis_miles int,
    gust_mph int,
    gust_kph int,
    uv int
);

-- Outboxes
create table if not exists normalized_days_sparse_outbox (
    --
    id serial primary key,
    source_unique_id text,
    created_on timestamp,
    processed boolean default false,
    retries int default 0,
    --
    date date,
    date_epoch int,
    maxtemp_c int,
    maxtemp_f int,
    mintemp_c int,
    mintemp_f int,
    avgtemp_c int,
    avgtemp_f int,
    maxwind_mph int,
    maxwind_kph int,
    totalprecip_mm int,
    totalprecip_in int,
    totalsnow_cm int,
    avgvis_km int,
    avgvis_miles int,
    avghumidity int,
    daily_will_it_rain int,
    daily_chance_of_rain int,
    daily_will_it_snow int,
    daily_chance_of_snow int,
    condition_text text,
    condition_icon text,
    condition_code int,
    uv int
);

create table if not exists normalized_astrology_sparse_outbox (
    --    
    id serial primary key,
    parent_id text,
    created_on timestamp,
    processed boolean default false,
    retries int default 0,
    --    
    sunrise text,
    sunset text,
    moonrise text,
    moonset text,
    moon_phase text,
    moon_illumination int
);

create table if not exists normalized_hours_sparse_outbox (
    --
    id serial primary key,
    parent_id text,
    created_on timestamp,
    processed boolean default false,
    retries int default 0,
    --
    time_epoch int,
    time text,
    temp_c int,
    temp_f int,
    is_day int,
    condition_text text,
    condition_icon text,
    condition_code int,
    wind_mph int,
    wind_kph int,
    wind_degree int,
    wind_dir text,
    pressure_mb int,
    pressure_in int,
    precip_mm int,
    precip_in int,
    snow_cm int,
    humidity int,
    cloud int,
    feelslike_c int,
    feelslike_f int,
    windchill_c int,
    windchill_f int,
    heatindex_c int,
    heatindex_f int,
    dewpoint_c int,
    dewpoint_f int,
    will_it_rain int,
    chance_of_rain int,
    will_it_snow int,
    chance_of_snow int,
    vis_km int,
    vis_miles int,
    gust_mph int,
    gust_kph int,
    uv int
);