import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def truncate_tables(cur, conn):
    truncate_queries = [
        "TRUNCATE TABLE staging_songs;",
        "TRUNCATE TABLE staging_events;",
        "TRUNCATE TABLE songplays;",
        "TRUNCATE TABLE users;",
        "TRUNCATE TABLE songs;",
        "TRUNCATE TABLE artists;",
        "TRUNCATE TABLE time;" 
    ]
    for query in truncate_queries:
        cur.execute(query)
        conn.commit()

# Staging validation queries to confirm data is loaded
staging_validation_queries = {
    "staging_events": "SELECT COUNT(*) FROM staging_events;",
    "staging_songs": "SELECT COUNT(*) FROM staging_songs;"
}

# Copy validation queries to confirm data was copied to final tables
copy_validation_queries = {
    "songplays": "SELECT COUNT(*) FROM songplays;",
    "users": "SELECT COUNT(*) FROM users;",
    "songs": "SELECT COUNT(*) FROM songs;",
    "artists": "SELECT COUNT(*) FROM artists;",
    "time": "SELECT COUNT(*) FROM time;"
}

# Load data into staging tables
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(f"Running staging query: {query[5:20]}...")
        cur.execute(query)
        conn.commit()
        print("Staging query executed successfully.")

# Validate staging tables
def run_staging_validation_queries(cur):
    print("Validating staging tables...")
    for table_name, query in staging_validation_queries.items():
        cur.execute(query)
        count = cur.fetchone()[0]
        if count > 0:
            print(f"Validation passed: '{table_name}' loaded with {count} records.")
        else:
            print(f"Validation failed: '{table_name}' is empty.")

# Insert data into final tables
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f"Running insert query: {query[:30]}...")
        cur.execute(query)
        conn.commit()
        print("Insert query executed successfully.")

# Validate final tables after copying
def run_copy_validation_queries(cur):
    print("Validating final tables...")
    for table_name, query in copy_validation_queries.items():
        cur.execute(query)
        count = cur.fetchone()[0]
        if count > 0:
            print(f"Validation passed: '{table_name}' populated with {count} records.")
        else:
            print(f"Validation failed: '{table_name}' is empty.")

# Main function to run ETL process
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_ENDPOINT           = config.get("IAM_ROLE","DWH_ENDPOINT")
#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    print("Truncating tables so there is no duplicates on reruns if the script fails in between")
    truncate_tables(cur, conn) 

    print("Loading data into staging tables...")
    load_staging_tables(cur, conn)
    run_staging_validation_queries(cur)  # Validate staging table loads

    print("\nInserting data into final tables...")
    insert_tables(cur, conn)
    run_copy_validation_queries(cur)  # Validate data copy to final tables

    conn.close()

if __name__ == "__main__":
    main()

# import configparser
# import psycopg2
# from sql_queries import copy_table_queries, insert_table_queries


# def load_staging_tables(cur, conn):
#     for query in copy_table_queries:
#         cur.execute(query)
#         conn.commit()


# def insert_tables(cur, conn):
#     for query in insert_table_queries:
#         cur.execute(query)
#         conn.commit()


# def main():
#     config = configparser.ConfigParser()
#     config.read('dwh.cfg')

#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#     cur = conn.cursor()
    
#     load_staging_tables(cur, conn)
#     insert_tables(cur, conn)

#     conn.close()


# if __name__ == "__main__":
#     main()