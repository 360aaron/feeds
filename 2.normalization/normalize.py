'''
Code-wise, what we are calling "normalization" is:
(1) fanning out records from raw, ingested and validated columns
(2) detecting changes to records and archiving prior versions
(3) preparing changes for outbox pattern to downstream systems.
'''

import json
import concurrent.futures
from psycopg_pool import ConnectionPool  # psycopg 3 recommended

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
SOURCE_DSN = "postgresql://user:pass@service_a/source_db"
TARGET_DSN = "postgresql://user:pass@service_b/target_db"
WRITE_THREADS = 10  # How many parallel connections to Service B
BATCH_SIZE = 500    # How many records each thread processes at once

# ---------------------------------------------------------
# 1. THE SERIAL READ (Service A)
# ---------------------------------------------------------
def fetch_all_data():
    print("Fetching data from Source...")
    # Standard single-threaded read. 
    # NOTE: If this is huge, use a server-side cursor to stream it instead of loading all into RAM.
    with psycopg.connect(SOURCE_DSN) as conn:
        with conn.execute("SELECT id, raw_record FROM records WHERE archived_on IS NULL") as cur:
             # Returns list of tuples: [(1, {...}), (2, {...}), ...]
            return cur.fetchall() 

# ---------------------------------------------------------
# 2. THE PARALLEL WRITE WORKER (Service B)
# ---------------------------------------------------------
def process_and_write_chunk(pool, chunk_of_records):
    """
    This function runs inside a thread.
    It receives a list of X records, parses them, and writes to Service B.
    """
    # Buffers for the 3 target tables
    days_data = []
    astrology_data = []
    hours_data = []
    
    # A. CPU WORK: Parse the JSONs in this chunk
    for record_id, raw_json in chunk_of_records:
        # --- LOGIC: Parse JSON into the 3 lists ---
        # (Use the parsing logic from the previous answer here)
        # This is pure CPU work, which Python threads handle okay-ish, 
        # but since the bottleneck is usually DB Network I/O, threads are fine.
        
        # ... [Insert Parsing Code Here to populate days_data, astrology_data, etc] ...
        pass 

    # B. I/O WORK: Write to Target DB
    # We grab a connection from the pool dedicated to Service B
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Use COPY for maximum speed
            if days_data:
                with cur.copy("COPY normalized_days (...) FROM STDIN") as copy:
                    for row in days_data: copy.write_row(row)
            
            if astrology_data:
                with cur.copy("COPY normalized_astrology (...) FROM STDIN") as copy:
                    for row in astrology_data: copy.write_row(row)

            if hours_data:
                with cur.copy("COPY normalized_hours (...) FROM STDIN") as copy:
                    for row in hours_data: copy.write_row(row)
        
        # Commit this chunk
        conn.commit()
    
    return len(chunk_of_records)

# ---------------------------------------------------------
# 3. ORCHESTRATION
# ---------------------------------------------------------
def main():
    # Step 1: Load Memory
    all_records = fetch_all_data()
    total_records = len(all_records)
    print(f"Loaded {total_records} records into memory.")

    # Step 2: Create Chunks (Slicing the list)
    # Example: [Record1...Record1000] -> [[Record1...500], [Record501...1000]]
    chunks = [all_records[i:i + BATCH_SIZE] for i in range(0, len(all_records), BATCH_SIZE)]
    print(f"Split into {len(chunks)} batches to be processed by {WRITE_THREADS} threads.")

    # Step 3: Fan-Out Writes
    # We create a connection pool for Service B so threads don't have to open/close TCP sockets constantly
    with ConnectionPool(TARGET_DSN, min_size=WRITE_THREADS, max_size=WRITE_THREADS) as pool:
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=WRITE_THREADS) as executor:
            # Submit all chunks to the thread pool
            futures = [executor.submit(process_and_write_chunk, pool, chunk) for chunk in chunks]
            
            # Wait for all to finish
            for future in concurrent.futures.as_completed(futures):
                try:
                    count = future.result()
                    # Optional: Progress bar logic here
                except Exception as e:
                    print(f"A batch failed: {e}")

    print("Migration Complete.")

if __name__ == "__main__":
    main()
