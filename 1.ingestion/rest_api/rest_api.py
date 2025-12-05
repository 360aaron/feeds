import json
import psycopg2
import requests

db_conn_str = "host=localhost port=5432 dbname=testdb user=aaron"
cities = [
    "Seattle", "Sacramento", "Portland", "Bozeman",
    "Chicago", "Nashville", "Atlanta", "Miami", "Boston"
]

ranges = [
    "2025-06-01&end_dt=2025-07-01",
    "2025-07-01&end_dt=2025-08-01",
    "2025-08-01&end_dt=2025-09-01",
    "2025-09-01&end_dt=2025-10-01",
    "2025-10-01&end_dt=2025-11-01",
    "2025-11-01&end_dt=2025-12-01"
]

def ingest_rest_api_data():
    try:
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
                        forecastday_list =  response.json() \
                            .get("forecast", {}) \
                            .get("forecastday", [])
                        if forecastday_list:
                            for date_object in forecastday_list:
                                source_unique_id = city.lower() + date_object.get("date").replace("-", "")
                                raw_record = json.dumps(date_object)
                                cur.execute("truncate table staging_restapi_records;")
                                conn.commit()
                                cur.execute("""
                                insert into staging_restapi_records (source_unique_id, raw_record) values (%s, %s);
                                """, (source_unique_id, raw_record)
                                )
                                conn.commit()
                                cur.execute("""
                                insert into restapi_records (source_unique_id, raw_record, raw_record_hash, created_on)
                                select source_unique_id, raw_record, raw_record_hash, now()
                                from staging_restapi_records
                                on conflict (source_unique_id) do update
                                set raw_record  = excluded.raw_record,
                                    raw_record_hash        = excluded.raw_record_hash,
                                    archived_on = now()
                                where restapi_records.raw_record_hash is distinct from excluded.raw_record_hash;
                                """)
                                conn.commit()
                            cur.execute("truncate table staging_restapi_records;")
                            conn.commit()
    except Exception as e:
        print(e)
        raise


if __name__ == '__main__':
    ingest_rest_api_data()
    # TODO: 
	# - Batch process
    # - Split into functions
    # - Add in-memory diffing
    # - Sparkify
