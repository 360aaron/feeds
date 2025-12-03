"""
https://sftpcloud.io/tools/free-sftp-server
`sftp 3456a43fd1e5487fbf00adfa278b228a@eu-central-1.sftpcloud.io`
`put /Users/aaron/Documents/mi/mi_inv_sample.tsv mi_inv_sample.tsv`
"""

import paramiko
import psycopg2

db_conn_str = "host=localhost port=5432 dbname=testdb user=aaron"

def ingest_sftp_data():
    try:
        with psycopg2.connect(db_conn_str) as conn:
            with conn.cursor() as cur:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(
                    hostname="eu-central-1.sftpcloud.io",
                    port=22,
                    username="3456a43fd1e5487fbf00adfa278b228a",
                    password="gXkBfLrSxkCtIENJFivcTwgBNqFxa31G"
                )
                sftp = ssh.open_sftp()
                with sftp.open("mi_inv_sample.tsv", "r") as remote_file:
                    data = remote_file.read()
                    decoded_data = data.decode("utf-8")
                    for i, line in enumerate(decoded_data.splitlines()):
                        if i == 0:
                            continue
                        prop_code, inv_date = line.split("\t")[:2]
                        source_unique_id = prop_code + inv_date
                        raw_record = line
                        cur.execute("truncate table staging_sftp_records;")
                        conn.commit()
                        cur.execute("""
                        insert into staging_sftp_records (source_unique_id, raw_record) values (%s, %s);
                        """, (source_unique_id, raw_record)
                        )
                        conn.commit()
                        cur.execute("""
                        insert into sftp_records (source_unique_id, raw_record, raw_record_hash, created_on)
                        select source_unique_id, raw_record, raw_record_hash, now()
                        from staging_sftp_records
                        on conflict (source_unique_id) do update
                        set raw_record  = excluded.raw_record,
                            raw_record_hash        = excluded.raw_record_hash,
                            archived_on = now()
                        where sftp_records.raw_record_hash is distinct from excluded.raw_record_hash;
                        """)
                        conn.commit()
                    cur.execute("truncate table staging_sftp_records;")
                    conn.commit()
        sftp.close()
        ssh.close()
    except Exception as e:
        print(e)
        raise

if __name__ == '__main__':
    ingest_sftp_data()
    # TODO: 
	# - Batch process
    # - Split into functions
    # - Add in-memory diffing
    # - Sparkify