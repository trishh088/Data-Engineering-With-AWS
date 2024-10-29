import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Define validation queries as a dictionary
drop_validation_queries = {
    "songs": "SELECT NOT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='songs');",
    "users": "SELECT NOT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='users');",
    "artists": "SELECT NOT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='artists');",
    "time": "SELECT NOT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='time');",
    "songplays": "SELECT NOT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='songplays');"
}

# Validation function that uses table names directly from dictionary keys
def run_drop_validation_queries(cur, conn):
    print("Running drop statement validation queries...")
    for table_name, query in drop_validation_queries.items():
        cur.execute(query)
        exists = cur.fetchone()[0]  # Fetch result of EXISTS query
        if exists:
            print(f"Validation passed: Table '{table_name}' dropped.")
        else:
            print(f"Validation failed: Table '{table_name}' still exist.")


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# Define validation queries as a dictionary
create_validation_queries = {
    "songs": "SELECT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='songs');",
    "users": "SELECT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='users');",
    "artists": "SELECT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='artists');",
    "time": "SELECT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='time');",
    "songplays": "SELECT EXISTS (SELECT 1 FROM pg_table_def WHERE tablename='songplays');"
}

# Validation function that uses table names directly from dictionary keys
def run_create_validation_queries(cur, conn):
    print("Running create statement validation queries...")
    for table_name, query in create_validation_queries.items():
        cur.execute(query)
        exists = cur.fetchone()[0]  # Fetch result of EXISTS query
        if exists:
            print(f"Validation passed: Table '{table_name}' exists.")
        else:
            print(f"Validation failed: Table '{table_name}' does not exist.")



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_ENDPOINT           = config.get("IAM_ROLE","DWH_ENDPOINT")

    # conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    run_drop_validation_queries(cur, conn)
    create_tables(cur, conn)
    run_create_validation_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()