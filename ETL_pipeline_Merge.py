#                                                            SCD-1 TYPE USING MERGE
#                                                            ======================          
                                                          
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
from preprocessing import load_and_clean_data
from logger_config import logger
from decorators import execution_time
from decorators import manage_connection
from decorators import retry

load_dotenv()

#                                                                  

# Database Connection 
# -----------------------------------

@execution_time
@retry(3)
def get_db_connections():
    """
    Connect to PostgreSQL databaseP
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'postgres'),
            database=os.getenv('DB_NAME', 'postgres'),
            port=os.getenv('DB_PORT', '5432'),
            password=os.getenv('DB_PASSWORD', '')
        )
        print("Connection to PostgreSQL successful")
        return conn

    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise



# Create Main Table  (SCD Type 1)
# ─-------------------------------------------------------------------

@execution_time
@retry(3)
def create_table(conn):
    """
    Create bank_transactions_merge table if it does not exist.

    SCD1 schema: no row_id, no start_date, no end_date.
    no duplicates , scd1 right history aint preseved
    """
    table_structure = """
        CREATE TABLE IF NOT EXISTS bank_transactions_merge (
            transaction_id            VARCHAR(50)    PRIMARY KEY,
            account_id                VARCHAR(50),
            transaction_amount        DECIMAL(15, 2),
            transaction_date          TIMESTAMP,
            transaction_type          VARCHAR(50),
            location                  VARCHAR(100),
            device_id                 VARCHAR(50),
            ip_address                VARCHAR(50),
            merchant_id               VARCHAR(50),
            channel                   VARCHAR(50),
            customer_age              INTEGER,
            customer_occupation       VARCHAR(100),
            transaction_duration      INTEGER,
            login_attempts            INTEGER,
            account_balance           DECIMAL(15, 2),
            previous_transaction_date TIMESTAMP
        );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(table_structure)
        conn.commit()
        print("Table bank_transactions_merge created successfully")
        cursor.close()

    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
        raise



# Create Staging Table
# ----------------------------

@execution_time
def create_staging_table(conn):
    """
    Create a temporary staging table for the current session.
    No row_id, start_date, or end_date  raw incoming data only.
    ON COMMIT PRESERVE ROWS keeps data alive across explicit commits.
    """
    staging_ddl = """
        CREATE TEMPORARY TABLE IF NOT EXISTS staging_transactions (
            transaction_id            VARCHAR(50),
            account_id                VARCHAR(50),
            transaction_amount        DECIMAL(15, 2),
            transaction_date          TIMESTAMP,
            transaction_type          VARCHAR(50),
            location                  VARCHAR(100),
            device_id                 VARCHAR(50),
            ip_address                VARCHAR(50),
            merchant_id               VARCHAR(50),
            channel                   VARCHAR(50),
            customer_age              INTEGER,
            customer_occupation       VARCHAR(100),
            transaction_duration      INTEGER,
            login_attempts            INTEGER,
            account_balance           DECIMAL(15, 2),
            previous_transaction_date TIMESTAMP
        ) ON COMMIT PRESERVE ROWS;
    """
    try:
        cursor = conn.cursor()
        cursor.execute(staging_ddl)
        conn.commit()
        print("Staging table created successfully")
        cursor.close()

    except Exception as e:
        print(f"Error creating staging table: {e}")
        conn.rollback()
        raise



# Load DataFrame into Staging
# ---------------------------------------------------------------------------------------------

STAGING_COLUMNS = [
    "TransactionID",
    "AccountID",
    "TransactionAmount",
    "TransactionDate",
    "TransactionType",
    "Location",
    "DeviceID",
    "IP Address",
    "MerchantID",
    "Channel",
    "CustomerAge",
    "CustomerOccupation",
    "TransactionDuration",
    "LoginAttempts",
    "AccountBalance",
    "PreviousTransactionDate",
]


@execution_time
def load_staging(conn, df):
    """
    Bulk-insert the preprocessed DataFrame into staging_transactions.
    Uses execute_values for efficiency , no row-by-row Python inserts.
    """
    insert_sql = """
        INSERT INTO staging_transactions (
            transaction_id, account_id, transaction_amount, transaction_date,
            transaction_type, location, device_id, ip_address, merchant_id,
            channel, customer_age, customer_occupation, transaction_duration,
            login_attempts, account_balance, previous_transaction_date
        )
        VALUES %s
    """

    try:
        rows = [
            tuple(row[col] for col in STAGING_COLUMNS)
            for _, row in df.iterrows()
        ]

        cursor = conn.cursor()
        execute_values(cursor, insert_sql, rows, page_size=500)
        conn.commit()
        print(f"Loaded {len(rows)} rows into staging_transactions")
        cursor.close()

    except Exception as e:
        print(f"Error loading staging: {e}")
        conn.rollback()
        raise

#important 
# merge logic for scd1
# if id exists and data changed -> update
# if id not there -> insert
# if same row -> skip

_MERGE_SCD1_SQL = """
   merge into bank_transactions_merge bt
using staging_transactions st
on bt.transaction_id = st.transaction_id

-- same id but data changed overwrite it

when matched and(

bt.account_id is distinct from st.account_id
or bt.transaction_amount is distinct from st.transaction_amount
or bt.transaction_date is distinct from st.transaction_date
or bt.transaction_type is distinct from st.transaction_type
or bt.location is distinct from st.location
or bt.device_id is distinct from st.device_id
or bt.ip_address is distinct from st.ip_address
or bt.merchant_id is distinct from st.merchant_id
or bt.channel is distinct from st.channel
or bt.customer_age is distinct from st.customer_age
or bt.customer_occupation is distinct from st.customer_occupation
or bt.transaction_duration is distinct from st.transaction_duration
or bt.login_attempts is distinct from st.login_attempts
or bt.account_balance is distinct from st.account_balance
or bt.previous_transaction_date is distinct from st.previous_transaction_date

)

then update set

account_id = st.account_id,
transaction_amount=st.transaction_amount,
transaction_date = st.transaction_date,
transaction_type=st.transaction_type,
location = st.location,
device_id=st.device_id,
ip_address = st.ip_address,
merchant_id = st.merchant_id,
channel=st.channel,
customer_age = st.customer_age,
customer_occupation=st.customer_occupation,
transaction_duration = st.transaction_duration,
login_attempts=st.login_attempts,
account_balance = st.account_balance,
previous_transaction_date=st.previous_transaction_date

-- new transaction -insert

when not matched then insert(

transaction_id,
account_id,
transaction_amount,
transaction_date,
transaction_type,
location,
device_id,
ip_address,
merchant_id,
channel,
customer_age,
customer_occupation,
transaction_duration,
login_attempts,
account_balance,
previous_transaction_date

)

values(

st.transaction_id,
st.account_id,
st.transaction_amount,
st.transaction_date,
st.transaction_type,
st.location,
st.device_id,
st.ip_address,
st.merchant_id,
st.channel,
st.customer_age,
st.customer_occupation,
st.transaction_duration,
st.login_attempts,
st.account_balance,
st.previous_transaction_date

);
"""


@execution_time
def merge_scd1(conn):
    
    """
    merge staging table into target table
    scd1 overwrite logic
"""
    
    try:
        cursor = conn.cursor()

        logger.info("Running MERGE (SCD Type 1) ...")
        cursor.execute(_MERGE_SCD1_SQL)
        affected = cursor.rowcount          # total rows touched (inserts + updates)
        logger.info(f"  MERGE affected {affected} row(s)")

        conn.commit()
        cursor.close()

        print(f"\nMERGE complete  total rows affected: {affected}")
        return {"affected": affected}

    except Exception as e:
        print(f"Error during MERGE: {e}")
        conn.rollback()
        raise



# Cleanup Staging
#--------------#

@execution_time
def cleanup_staging(conn):
    """
    Truncate staging table after merge so it is clean for the next run.
    The TEMPORARY table is also auto-dropped when the session closes,
    but explicit truncation is safer with long-lived connection pools.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE staging_transactions;")
        conn.commit()
        print("Staging table truncated")
        cursor.close()

    except Exception as e:
        print(f"Error during staging cleanup: {e}")
        conn.rollback()
        raise



# QA Checks
# ------------------

@execution_time
def run_qa_checks(conn):
    """
    Run basic QA checks on the loaded data
    """
    try:
        cursor = conn.cursor()

        # Total row count
        cursor.execute("SELECT COUNT(*) FROM bank_transactions_merge;")
        row_count = cursor.fetchone()[0]
        print(f"\nTotal rows in database: {row_count}")

        # technically should never happen because transaction_id is pk
      # still checking just in case
        print("\nDuplicate PK check")
        cursor.execute("""
            SELECT transaction_id
            FROM bank_transactions_merge
            GROUP BY transaction_id
            HAVING COUNT(*) > 1;
        """)
        duplicate_keys = list(cursor.fetchall())
        print(f"Duplicate key list: {duplicate_keys}")

        # Under-age customers
        cursor.execute("""
            SELECT transaction_id
            FROM bank_transactions_merge
            WHERE customer_age < 18;
        """)
        print(f"Illegal age: {cursor.fetchall()}")

        # Negative balances
        cursor.execute("""
            SELECT transaction_id
            FROM bank_transactions_merge
            WHERE account_balance < 0;
        """)
        print(f"Negative balance list: {cursor.fetchall()}")

        # Null transaction amounts
        cursor.execute("""
            SELECT COUNT(*)
            FROM bank_transactions_merge
            WHERE transaction_amount IS NULL;
        """)
        print(f"Null transaction amounts: {cursor.fetchone()[0]}")

        cursor.close()

    except Exception as e:
        print(f"Error running QA checks: {e}")
        raise



# Main
# -----------

@manage_connection(get_db_connections)
def main(conn):
    """
    ETL pipeline — Staging + SQL MERGE + SCD Type 1
    """
    logger.info("ETL pipeline (merge/staging) started")

    # Step 1: Preprocess CSV
    logger.info("Step 1: Data preprocessing started ...")
    df = load_and_clean_data("bank_transactions_data_2.csv")

    # Step 2: Connection already provided by @manage_connection
    logger.info("Step 2: Database connected")

    # Step 3: Create main table
    print("\n=== Step 3: Create Table ===")
    create_table(conn)

    # Step 4: Create staging table
    print("\n=== Step 4: Create Staging Table ===")
    create_staging_table(conn)

    # Step 5: Bulk load staging
    print("\n=== Step 5: Load Staging ===")
    load_staging(conn, df)

    # Step 6: SQL-driven SCD1 merge
    print("\n=== Step 6: SCD Type 1 Merge ===")
    merge_scd1(conn)

    # Step 7: QA checks
    print("\n=== Step 7: QA Checks ===")
    run_qa_checks(conn)

    # Step 8: Cleanup staging
    print("\n=== Step 8: Cleanup Staging ===")
    cleanup_staging(conn)

    print("\nETL Pipeline (merge) completed successfully!")
    logger.info("ETL pipeline (merge/staging) finished")


if __name__ == "__main__":
    main()