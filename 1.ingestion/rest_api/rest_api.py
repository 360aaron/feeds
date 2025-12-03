import json
import psycopg2
import requests
import argparse

from validate_responses import validate_restapi_payload
from queries import * 

db_conn_str = "host=localhost port=5432 dbname=testdb user=aaron"
# cities = [
#     "Seattle", "Sacramento", "Portland", "Bozeman",
#     "Chicago", "Nashville", "Atlanta", "Miami", "Boston"
# ]
# cities = ["Nashville"]
cities = []

ranges = [
    "2025-06-01&end_dt=2025-07-01",
    "2025-07-01&end_dt=2025-08-01",
    "2025-08-01&end_dt=2025-09-01",
    "2025-09-01&end_dt=2025-10-01",
    "2025-10-01&end_dt=2025-11-01",
    "2025-11-01&end_dt=2025-12-01"
]

parser = argparse.ArgumentParser()
parser.add_argument(
    "--mode",
    type=str,
    choices=["full", "since", "targeted"], # conditional?
    default="full" # "full" is BAU unless source allows incremental
)
parser.add_argument(
    "--date",
    type=str, 
    help="Start date in YYYY-MM-DD format (required when --mode=since)"
)
parser.add_argument(
    "--ids",
    type=lambda s: [x.strip() for x in s.split(",") if x.strip()],
    help="Comma-separated IDs (required when --mode=targeted)"
)

args = parser.parse_args()

if args.mode == "since" and not args.date:
    parser.error("--date is required when --mode=since")

if args.mode == "targeted" and not args.ids:
    parser.error("--ids is required when --mode=targeted")

if args.mode == "targeted":
    cities.extend(args.ids)

def ingest_rest_api_data():
    try:
        if not cities:
            raise ValueError("City list is empty")
        with psycopg2.connect(db_conn_str) as conn:
            with conn.cursor() as cur:
                for city in cities:
                    for range in ranges:
                        endpoint = (
                            f"https://api.weatherapi.com/v1/history.json"
                            f"?key=0a37b8dba09b4b0c913151847250312"
                            f"&q={city}"
                            f"&dt={range}"
                            f"&lang=en"
                        )
                        response = requests.get(endpoint, timeout=30)
                        response.raise_for_status()
                        
                        validation_results = validate_restapi_payload(response.json())
                        print(validation_results) # this could be logged, stored or notified

                        forecastday_list =  response.json() \
                            .get("forecast", {}) \
                            .get("forecastday", [])
                        if forecastday_list:
                            cur.execute(TRUNCATE_STAGING)
                            conn.commit()
                            for date_object in forecastday_list:
                                source_unique_id = city.lower() + date_object.get("date").replace("-", "")
                                raw_record = json.dumps(date_object)
                                cur.execute(INSERT_STAGING, (source_unique_id, raw_record)
                                )
                                conn.commit()
                                cur.execute(UPSERT_RESTAPI_RECORDS)
                                conn.commit()
                            cur.execute(TRUNCATE_STAGING)
                            conn.commit()
    except Exception as e:
        print(e)
        raise


if __name__ == '__main__':
    ingest_rest_api_data()
    # TODO: 
	# - Batch process
    # - Split into functions
    # - Add ingest-validation
    # - Sparkify
