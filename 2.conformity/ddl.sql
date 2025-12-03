/* 1. Validate live ingested records:
    a. Missing keys
    b. Unexpected keys
    c. Data type mismatches

Output: bad rows, missing keys or unexpected keys are logged for review.

    Valid rows are moved to normalized tables.
*/
create table if not exists normalized_days (
    source_unique_id text primary key,
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
    uv int,
    id serial
)

normalized_astrology (
    source_unique_id text primary key,
    sunrise text,
    sunset text,
    moonrise text,
    moonset text,
    moon_phase text,
    moon_illumination int,
    id serial
)

normalized_hours (
    source_unique_id text primary key,
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
    uv int,
    id serial
);
/* 

Raw -> Tabularly split and validated -> Domain outboxes -> Avro Kakfka messages

Why Avro?

- Reduce message sizes by 4-5x​
- Improve throughput and latency​
- Easier schema evolution

--
https://www.diva-portal.org/smash/get/diva2:1878772/FULLTEXT01.pdf

*/

-- Outboxes

-- 1. Properties
-- 2. Event spaces
-- 3. Amenities
-- 4. Restaurants
-- 5. Translations

-- 6. Room types
-- 7. Availability

-- 8. Images
-- 9. Image tags
-- 10. Image captions

