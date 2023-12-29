import os
from concurrent.futures import ProcessPoolExecutor
import sqlite3
import time
import libsql_client # pip install libsql-client
from faker import Faker # pip install Faker

# Get the Turso URL and auth token from the environment variables
# export TURSO_URL=https://your-database.turso.io
# export TURSO_AUTH_TOKEN=your_auth_token
turso_url = os.environ.get("TURSO_URL")
turso_auth_token = os.environ.get("TURSO_AUTH_TOKEN")

# Variable to determine if local sqllite file will be used
# process_count must be 1 if this is set to True as I don't have any concurrency controls in place
use_local_sqlite = True

# Variable to determine if the sqlite driver will be used
# process_count must be 1 if this is set to True as I don't have any concurrency controls in place
use_sqlite_driver = False

# If using local sqlite file, set the path to the sqlite file
# The file will automatically be created if it doesn't exist
sqlite_file_path = "file:./turso-benchmark.db"

# Number of processes to use
process_count =1

# Number of rows to insert per process
insert_row_count = 1000

# Number of rows to select per process
select_row_count = 1

create_example_users_table_query = '''
CREATE TABLE IF NOT EXISTS example_users (
    uid TEXT PRIMARY KEY,
    email text
);
'''
create_runs_table_query = '''
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    process_id INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration FLOAT,
    num_rows INTEGER,
    query_type TEXT,
    driver TEXT
);
'''
# Query to insert into the runs table
insert_run_query = "INSERT INTO runs (process_id, start_time, end_time, duration, num_rows, query_type, driver) VALUES (?, ?, ?, ?, ?, ?, ?)"

def insert_rows(iteration):
    # Get the Process ID
    pid = os.getpid()
    
    # Create a Faker instance
    fake = Faker()

    # Create an empty list to store the output
    output = []

    conn = sqlite3.connect(sqlite_file_path)

    driver = ""

    # Create a client to the Turso database
    if use_sqlite_driver:
        client = conn.cursor()
        driver = "sqlite"
    elif use_local_sqlite:
        client = libsql_client.create_client_sync(url=sqlite_file_path)
        driver = "libsql-local-sqlite"
    else:
        client = libsql_client.create_client_sync(url=turso_url, auth_token=turso_auth_token)
        driver = "turso"

    if use_sqlite_driver:
        # Record the start time
        start_time = time.time()
        for _ in range(insert_row_count):
            result_set = client.execute("insert into example_users values (?, ?);", [fake.uuid4(), fake.email()])
        # Record the end time after the operation is completed
        end_time = time.time()
        # Calculate the elapsed time
        elapsed_time = time.time() - start_time
        output.append(f"Pid {pid}: Iteration {iteration}: Elapsed time to insert {insert_row_count} rows: {elapsed_time:.6f} seconds")
        rs = client.execute(insert_run_query, [pid, start_time, end_time, elapsed_time, insert_row_count, "insert", driver])
        
        # Record the start time
        start_time = time.time()
        rs = client.execute("select * from example_users limit ?;", [select_row_count])
        rows = rs.fetchall()
        # Calculate the elapsed time
        elapsed_time = time.time() - start_time
        output.append(f"Pid {pid}: Iteration {iteration}: Elapsed time to select {len(rows)} rows: {elapsed_time:.6f} seconds")
        rs = client.execute(insert_run_query, [pid, start_time, end_time, elapsed_time, select_row_count, "select", driver])
    else:
        with client:
            # Record the start time
            start_time = time.time()
            for _ in range(insert_row_count):
                result_set = client.execute("insert into example_users values (?, ?);", [fake.uuid4(), fake.email()])
            # Record the end time after the operation is completed
            end_time = time.time()
            # Calculate the elapsed time
            elapsed_time = time.time() - start_time
            output.append(f"Pid {pid}: Iteration {iteration}: Elapsed time to insert {insert_row_count} rows: {elapsed_time:.6f} seconds")
            rs = client.execute(insert_run_query, [pid, start_time, end_time, elapsed_time, insert_row_count, "insert", driver])
            
            # Record the start time
            start_time = time.time()
            rs = client.execute("select * from example_users limit ?;", [select_row_count])
            # Calculate the elapsed time
            elapsed_time = time.time() - start_time
            output.append(f"Pid {pid}: Iteration {iteration}: Elapsed time to select {len(rs.rows)} rows: {elapsed_time:.6f} seconds")
            rs = client.execute(insert_run_query, [pid, start_time, end_time, elapsed_time, select_row_count, "select", driver])
    return output

if __name__ == "__main__":

    # Create a client to the Turso database
    if use_local_sqlite:
        client = libsql_client.create_client_sync(url=sqlite_file_path)
        result_set = client.execute('PRAGMA journal_mode = WAL;')
    else:
        client = libsql_client.create_client_sync(url=turso_url, auth_token=turso_auth_token)

    with client:
        result_set = client.execute(create_example_users_table_query)
        result_set = client.execute(create_runs_table_query)

    # Record the start time
    start_time = time.time()
    with ProcessPoolExecutor(max_workers=process_count) as executor:
        # Submit tasks to the executor
        all_outputs = list(executor.map(insert_rows, range(process_count)))
    # Calculate the elapsed time
        elapsed_time = time.time() - start_time

    # Print outputs to the console
    for i, output in enumerate(all_outputs):
        print(f"Process {i} output:")
        for line in output:
            print(line)
        print("\n")

    if use_sqlite_driver:
        print ("Using sqlite driver on local sqlite")
    elif use_local_sqlite:
        print ("Using libsql driver on local sqlite")
    else:
        print ("Using libsql driver on Turso")
    
    # Print the total elapsed time
    print(f"Total elapsed time: {elapsed_time} seconds")