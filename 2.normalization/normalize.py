import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import execute_values, DictCursor

from queries import *

BATCH_SIZE = 100 # TODO: measure on specific workload and tune accordingly

DB_CONN_STR = "host=localhost port=5432 dbname=testdb user=aaron"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--mode",
    type=str,
    choices=["full", "since", "targeted"], # conditional?
    default="full"
)
parser.add_argument(
    "--date",
    type=str, 
    help="Start date in YYYY-MM-DD format (required when --mode=since)"
)
parser.add_argument(
    "--ids",
    type=str,
    help="Comma-separated IDs (required when --mode=targeted)"
)

args = parser.parse_args()

if args.mode == "since" and not args.date:
    parser.error("--date is required when --mode=since")

if args.mode == "targeted" and not args.ids:
    parser.error("--ids is required when --mode=targeted")

def sha256_hex_from_json(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def batchify(iterable, BATCH_SIZE):
    for i in range(0, len(iterable), BATCH_SIZE):
        yield iterable[i:i + BATCH_SIZE]

def build_day_row(source_unique_id: str, day: dict):
    day_hash = sha256_hex_from_json(day)
    return (
        day_hash,
        source_unique_id,
        float(day["maxtemp_c"]),
        float(day["maxtemp_f"]),
        float(day["mintemp_c"]),
        float(day["mintemp_f"]),
        float(day["avgtemp_c"]),
        float(day["avgtemp_f"]),
        float(day["maxwind_mph"]),
        float(day["maxwind_kph"]),
        float(day["totalprecip_mm"]),
        float(day["totalprecip_in"]),
        float(day["totalsnow_cm"]),
        float(day["avgvis_km"]),
        float(day["avgvis_miles"]),
        float(day["avghumidity"]),
        int(day["daily_will_it_rain"]),
        int(day["daily_chance_of_rain"]),
        int(day["daily_will_it_snow"]),
        int(day["daily_chance_of_snow"]),
        day["condition"]["text"],
        day["condition"]["icon"],
        int(day["condition"]["code"]),
        float(day["uv"])
    )


def build_astro_row(parent_id: str, astro: dict):
    astro_hash = sha256_hex_from_json(astro)
    return (
        astro_hash,
        f"{parent_id}_{1}",
        parent_id,
        astro["sunrise"],
        astro["sunset"],
        astro["moonrise"],
        astro["moonset"],
        astro["moon_phase"],
        int(astro["moon_illumination"])
    )


def build_hour_rows(source_unique_id: str, hours: list):
    rows = []
    for h in hours:
        hour_hash = sha256_hex_from_json(h)
        rows.append(
            (
                hour_hash,
                f"{source_unique_id}_{hour_hash}",
                source_unique_id,
                int(h["time_epoch"]),
                h["time"],
                float(h["temp_c"]),
                float(h["temp_f"]),
                int(h["is_day"]),
                h["condition"]["text"],
                h["condition"]["icon"],
                int(h["condition"]["code"]),
                float(h["wind_mph"]),
                float(h["wind_kph"]),
                int(h["wind_degree"]),
                h["wind_dir"],
                float(h["pressure_mb"]),
                float(h["pressure_in"]),
                float(h["precip_mm"]),
                float(h["precip_in"]),
                float(h["snow_cm"]),
                int(h["humidity"]),
                int(h["cloud"]),
                float(h["feelslike_c"]),
                float(h["feelslike_f"]),
                float(h["windchill_c"]),
                float(h["windchill_f"]),
                float(h["heatindex_c"]),
                float(h["heatindex_f"]),
                float(h["dewpoint_c"]),
                float(h["dewpoint_f"]),
                int(h["will_it_rain"]),
                int(h["chance_of_rain"]),
                int(h["will_it_snow"]),
                int(h["chance_of_snow"]),
                float(h["vis_km"]),
                float(h["vis_miles"]),
                float(h["gust_mph"]),
                float(h["gust_kph"]),
                float(h["uv"])
            )
        )
    return rows

def bulk_upsert(conn, sql: str, rows, page_size: int = 100):
    if not rows:
        return
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=page_size) 
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise
    except Exception as e:
        print(e)
        raise

def get_restapi_raw_records():
    try:
        with psycopg2.connect(DB_CONN_STR) as raw_conn:
            with raw_conn.cursor(cursor_factory=DictCursor) as raw_cur:
                if args.mode == "full":
                    raw_cur.execute(FULL_SELECT)
                elif args.mode == "since":
                    raw_cur.execute(SINCE_SELECT, (args.date,))
                elif args.mode == "targeted":
                    ids_list = args.ids.split(",")
                    raw_cur.execute(TARGETED_SELECT, (ids_list,))
                else:
                    raw_cur.execute(BAU_SELECT)
                return raw_cur.fetchall()
    except Exception as e:
        print(e)
        raise

def normalize_ingested_data():
    day_conn = astro_conn = hour_conn = None
    try:
        restapi_raw_records = get_restapi_raw_records()
        if not restapi_raw_records:
            print("No records to normalize")
            return

        day_conn = psycopg2.connect(DB_CONN_STR)
        astro_conn = psycopg2.connect(DB_CONN_STR)
        hour_conn = psycopg2.connect(DB_CONN_STR)

        with ThreadPoolExecutor(max_workers=3) as ex:
            for row_batch in batchify(restapi_raw_records, BATCH_SIZE):
                day_rows = []
                astro_rows = []
                hour_rows = []
                for row in row_batch:
                    source_id = row["source_unique_id"]
                    raw_record = json.loads(row["raw_record"])
                    day_rows.append(build_day_row(source_id, raw_record["day"]))
                    astro_rows.append(build_astro_row(source_id, raw_record["astro"]))
                    hour_rows.extend(build_hour_rows(source_id, raw_record["hour"]))

                ex.submit(bulk_upsert, day_conn, UPSERT_DAY, day_rows)
                ex.submit(bulk_upsert, astro_conn, UPSERT_ASTRO, astro_rows)
                ex.submit(bulk_upsert, hour_conn, UPSERT_HOUR, hour_rows)
    except Exception as e:
        print(e)
    finally:
        if day_conn:
            day_conn.close()
        if astro_conn:
            astro_conn.close()
        if hour_conn:
            hour_conn.close()


if __name__ == '__main__':
    normalize_ingested_data()
    # TODO:
    # - Canonicalize hashing
    # - sparkify
    # - implement connection pooling