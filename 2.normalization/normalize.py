import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import execute_values, DictCursor

from queries import *
from validate_rows import is_day_valid

BATCH_SIZE = 400 # TODO: measure on specific workload and tune accordingly

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


def process_day(source_unique_id: str, day: dict):
    nr = {
        "id": 0,                                # placeholder; DB autoincrements real id
        "source_unique_id": source_unique_id,
        "outboxed_on": None,                    # placeholder; set by DB
        "maxtemp_c": float(day["maxtemp_c"]),
        "maxtemp_f": float(day["maxtemp_f"]),
        "mintemp_c": float(day["mintemp_c"]),
        "mintemp_f": float(day["mintemp_f"]),
        "avgtemp_c": float(day["avgtemp_c"]),
        "avgtemp_f": float(day["avgtemp_f"]),
        "maxwind_mph": float(day["maxwind_mph"]),
        "maxwind_kph": float(day["maxwind_kph"]),
        "totalprecip_mm": float(day["totalprecip_mm"]),
        "totalprecip_in": float(day["totalprecip_in"]),
        "totalsnow_cm": float(day["totalsnow_cm"]),
        "avgvis_km": float(day["avgvis_km"]),
        "avgvis_miles": float(day["avgvis_miles"]),
        "avghumidity": float(day["avghumidity"]),
        "daily_will_it_rain": int(day["daily_will_it_rain"]),
        "daily_chance_of_rain": int(day["daily_chance_of_rain"]),
        "daily_will_it_snow": int(day["daily_will_it_snow"]),
        "daily_chance_of_snow": int(day["daily_chance_of_snow"]),
        "condition_text": day["condition"]["text"],
        "condition_icon": day["condition"]["icon"],
        "condition_code": int(day["condition"]["code"]),
        "uv": float(day["uv"]),
    }
    if not is_day_valid(nr):
        return None, False
    day_hash = sha256_hex_from_json(nr)
    day_tuple = (
        day_hash,
        source_unique_id,
        nr["maxtemp_c"],
        nr["maxtemp_f"],
        nr["mintemp_c"],
        nr["mintemp_f"],
        nr["avgtemp_c"],
        nr["avgtemp_f"],
        nr["maxwind_mph"],
        nr["maxwind_kph"],
        nr["totalprecip_mm"],
        nr["totalprecip_in"],
        nr["totalsnow_cm"],
        nr["avgvis_km"],
        nr["avgvis_miles"],
        nr["avghumidity"],
        nr["daily_will_it_rain"],
        nr["daily_chance_of_rain"],
        nr["daily_will_it_snow"],
        nr["daily_chance_of_snow"],
        nr["condition_text"],
        nr["condition_icon"],
        nr["condition_code"],
        nr["uv"],
    )
    return day_tuple, True


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


def normalize_ingested_data(day_conn, raw_records):
    try:
        with ThreadPoolExecutor(max_workers=3) as ex:
            for row_batch in batchify(raw_records, BATCH_SIZE):
                day_rows, exceptions = [], []
                for row in row_batch:
                    source_id = row["source_unique_id"]
                    raw_record = json.loads(row["raw_record"])
                    day, is_valid = process_day(source_id, raw_record["day"])
                    if is_valid:
                        day_rows.append(day)
                    else:
                        print(f"Invalid day for source_id {source_id}")
                ex.submit(bulk_upsert, day_conn, UPSERT_DAY, day_rows)
                if exceptions:
                    print(exceptions)
    except Exception as e:
        print(e)


def establish_connections():
    return psycopg2.connect(DB_CONN_STR)


def orchestrate():
    day_conn = establish_connections()
    
    raw_records = get_restapi_raw_records()
    if not raw_records:
        print("No records to normalize")
        return
    
    normalize_ingested_data(day_conn, raw_records)
    
    if day_conn:
        day_conn.close()


if __name__ == '__main__':
    orchestrate()
    # TODO:
    # - Canonicalize hashing
    # - sparkify
    # - implement connection pooling