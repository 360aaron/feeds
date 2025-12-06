'''
Code-wise, what we are calling "normalization" is:
(1) fanning out records from raw, ingested and validated columns
(2) detecting changes to records and archiving prior versions
(3) preparing changes for outbox pattern to downstream systems.
'''
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import execute_values, DictCursor
# TODO:
# - Sparkify
# - Add children counts in parent object
# - Implement connection pooling

db_conn_str = "host=localhost port=5432 dbname=testdb user=aaron"

def sha256_hex_from_json(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def insert_day(source_unique_id, day):
    day_hash = sha256_hex_from_json(day)
    with psycopg2.connect(db_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging_normalized_days (
                    record_hash, source_unique_id, date, date_epoch,
                    maxtemp_c, maxtemp_f, mintemp_c, mintemp_f,
                    avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph,
                    totalprecip_mm, totalprecip_in,
                    totalsnow_cm, avgvis_km, avgvis_miles,
                    avghumidity, daily_will_it_rain, 
                    daily_chance_of_rain, daily_will_it_snow,
                    daily_chance_of_snow, condition_text, 
                    condition_icon, condition_code, uv
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    day_hash,
                    source_unique_id,
                    raw_record["date"],
                    raw_record["date_epoch"],
                    int(day["maxtemp_c"]),
                    int(day["maxtemp_f"]),
                    int(day["mintemp_c"]),
                    int(day["mintemp_f"]),
                    int(day["avgtemp_c"]),
                    int(day["avgtemp_f"]),
                    int(day["maxwind_mph"]),
                    int(day["maxwind_kph"]),
                    int(day["totalprecip_mm"]),
                    int(day["totalprecip_in"]),
                    int(day["totalsnow_cm"]),
                    int(day["avgvis_km"]),
                    int(day["avgvis_miles"]),
                    int(day["avghumidity"]),
                    int(day["daily_will_it_rain"]),
                    int(day["daily_chance_of_rain"]),
                    int(day["daily_will_it_snow"]),
                    int(day["daily_chance_of_snow"]),
                    day["condition"]["text"],
                    day["condition"]["icon"],
                    int(day["condition"]["code"]),
                    int(day["uv"]),
                ),
            )
            conn.commit()

def insert_astro(source_unique_id, astro):
    astro_hash = sha256_hex_from_json(astro)
    with psycopg2.connect(db_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
            """
                INSERT INTO staging_normalized_astrology (
                    record_hash, parent_id, sunrise, sunset, moonrise, moonset,
                    moon_phase, moon_illumination
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    astro_hash,
                    source_unique_id,
                    astro["sunrise"],
                    astro["sunset"],
                    astro["moonrise"],
                    astro["moonset"],
                    astro["moon_phase"],
                    int(astro["moon_illumination"]),
                ),
            )
            conn.commit()

def insert_hours(source_unique_id, hour):
    hour_hash = sha256_hex_from_json(hour)
    with psycopg2.connect(db_conn_str) as conn:
        with conn.cursor() as cur:
            hour_rows = []
            for h in hour:
                hour_rows.append(
                    (
                        hour_hash,
                        source_unique_id,
                        int(h["time_epoch"]),
                        h["time"],
                        int(h["temp_c"]),
                        int(h["temp_f"]),
                        int(h["is_day"]),
                        h["condition"]["text"],
                        h["condition"]["icon"],
                        int(h["condition"]["code"]),
                        int(h["wind_mph"]),
                        int(h["wind_kph"]),
                        int(h["wind_degree"]),
                        h["wind_dir"],
                        int(h["pressure_mb"]),
                        int(h["pressure_in"]),
                        int(h["precip_mm"]),
                        int(h["precip_in"]),
                        int(h["snow_cm"]),
                        int(h["humidity"]),
                        int(h["cloud"]),
                        int(h["feelslike_c"]),
                        int(h["feelslike_f"]),
                        int(h["windchill_c"]),
                        int(h["windchill_f"]),
                        int(h["heatindex_c"]),
                        int(h["heatindex_f"]),
                        int(h["dewpoint_c"]),
                        int(h["dewpoint_f"]),
                        int(h["will_it_rain"]),
                        int(h["chance_of_rain"]),
                        int(h["will_it_snow"]),
                        int(h["chance_of_snow"]),
                        int(h["vis_km"]),
                        int(h["vis_miles"]),
                        int(h["gust_mph"]),
                        int(h["gust_kph"]),
                        int(h["uv"]),
                    )
                )
            execute_values(
                cur,
                """
                INSERT INTO staging_normalized_hours (
                    record_hash, parent_id, time_epoch, time,
                    temp_c, temp_f, is_day, condition_text, 
                    condition_icon, condition_code, wind_mph, 
                    wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in,
                    precip_mm, precip_in, snow_cm, humidity, cloud,
                    feelslike_c, feelslike_f, windchill_c, windchill_f,
                    heatindex_c, heatindex_f, dewpoint_c, dewpoint_f,
                    will_it_rain, chance_of_rain, will_it_snow, 
                    chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
                ) VALUES %s
                """,
                hour_rows,
            )
            conn.commit()

with psycopg2.connect(db_conn_str) as conn:
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
        select * from restapi_records where id = 1702;
        """)
        row = cur.fetchone()
        source_unique_id = row['source_unique_id']
        raw_record = json.loads(row['raw_record'])
        day = raw_record["day"]
        astro = raw_record["astro"]
        hour = raw_record["hour"]
        with ThreadPoolExecutor(max_workers=3) as ex:
            ex.submit(insert_day, source_unique_id, day)
            ex.submit(insert_astro, source_unique_id, astro)
            ex.submit(insert_hours, source_unique_id, hour)