import csv
import psycopg2

test_file_path = "/Users/aaron/Documents/mi/mi_inv_sample.csv"

db_conn_str = "host=localhost port=5432 dbname=testdb user=aaron"
def ingest_csv_data():
    try:
        with psycopg2.connect(db_conn_str) as conn:
            with conn.cursor() as cur:
                with open(test_file_path, mode='r') as file:
                    csv_reader = csv.reader(file)
                    for i, row in enumerate(csv_reader):
                        if i == 0:
                            continue
                        source_unique_id = row[0] + row[1]
                        raw_record = ','.join(row)
                        cur.execute("truncate table staging_csv_records;")
                        conn.commit()
                        cur.execute("""
                        insert into staging_csv_records (source_unique_id, raw_record) values (%s, %s);
                        """, (source_unique_id, raw_record)
                        )
                        conn.commit()
                        cur.execute("""
                        insert into csv_records (source_unique_id, raw_record, raw_record_hash, created_on)
                        select source_unique_id, raw_record, raw_record_hash, now()
                        from staging_csv_records
                        on conflict (source_unique_id) do update
                        set raw_record  = excluded.raw_record,
                            raw_record_hash        = excluded.raw_record_hash,
                            archived_on = now()
                        where csv_records.raw_record_hash is distinct from excluded.raw_record_hash;
                        """)
                        conn.commit()
                    cur.execute("truncate table staging_csv_records;")
                    conn.commit()
    except Exception as e:
        print(e)
        raise

if __name__ == '__main__':
    ingest_csv_data()
    # TODO: 
	# - Batch process
    # - Split into functions
    # - Add in-memory diffing
    # - Sparkify