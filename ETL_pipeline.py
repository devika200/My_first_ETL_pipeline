import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import preprocessing 
from logger_config import logger
from decorators import execution_time
from decorators import manage_connection
from decorators import retry
from datetime import datetime
import boto3
from preprocessing import get_data_from_s3

load_dotenv()


@execution_time
@retry(3)
def get_db_connections():
    """
    Connect to PostgreSQL database
    """

    try:

        conn=psycopg2.connect(
            host=os.getenv('DB_HOST','localhost'),
            user=os.getenv('DB_USER','postgres'),
            database=os.getenv('DB_NAME','postgres'),
            port=os.getenv('DB_PORT','5432'),
            password=os.getenv('DB_PASSWORD','')

        )

        print("connection to postgresSQL successful")
        return conn
    
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise 


@execution_time
@retry(3)
def create_table(conn):

    """
    Create database table
    """
    #adding start_date TIMESTAMP DEFAULT NOW(),  end_date TIMESTAMP DEFAULT NULL for SCD-2
    table_structure="""
    CREATE TABLE IF NOT EXISTS bank_transactions(
        row_id SERIAL PRIMARY KEY,
        transaction_id VARCHAR(50) ,
        account_id VARCHAR(50),
        transaction_amount DECIMAL(15, 2),
        transaction_date TIMESTAMP,
        transaction_type VARCHAR(50),
        location VARCHAR(100),
        device_id VARCHAR(50),
        ip_address VARCHAR(50),
        merchant_id VARCHAR(50),
        channel VARCHAR(50),
        customer_age INTEGER,
        customer_occupation VARCHAR(100),
        transaction_duration INTEGER,
        login_attempts INTEGER,
        account_balance DECIMAL(15, 2),
        previous_transaction_date TIMESTAMP,
        start_date TIMESTAMP DEFAULT NOW(),  
        end_date TIMESTAMP DEFAULT NULL

    );

    """

    try:
        cursor=conn.cursor()
        cursor.execute(table_structure)
        conn.commit()
        print("Table created succesfully")
        cursor.close()

    except Exception as e:
        print(f"error occured:{e}")
        raise 


@execution_time
def insert_data(conn,df):

    """
    insert data
    """

    insert_query= """
    INSERT INTO bank_transactions
        (Transaction_id, account_id, transaction_amount, transaction_date, 
        transaction_type, location, device_id, ip_address, merchant_id, 
        channel, customer_age, customer_occupation, transaction_duration, 
        login_attempts, account_balance, previous_transaction_date,start_date,end_date )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
    
"""
    
    update_query="""
        UPDATE bank_transactions
        SET
            account_id=%s,
            transaction_amount=%s,
            transaction_date=%s,
            transaction_type=%s,
            location=%s,
            device_id=%s,
            ip_address=%s,
            merchant_id=%s,
            channel=%s,
            customer_age=%s,
            customer_occupation=%s,
            transaction_duration=%s,
            login_attempts=%s,
            account_balance=%s,
            previous_transaction_date=%s

        WHERE transaction_id=%s


"""
    for _,row in df.iterrows():

        row_values=(
            #[(a,b,c,d...)->row 1,(g,h,k,l...)->row2, etc] = values from dataframe
            row['TransactionID'],
            row['AccountID'],           
            row['TransactionAmount'],
            row['TransactionDate'],
            row['TransactionType'],
            row['Location'],
            row['DeviceID'],
            row['IP Address'],
            row['MerchantID'],
            row['Channel'],
            row['CustomerAge'],
            row['CustomerOccupation'],
            row['TransactionDuration'],
            row['LoginAttempts'],
            row['AccountBalance'],
            row['PreviousTransactionDate']
        )
        
        
        try:

            cursor=conn.cursor()
            id=row_values[0]

            query="""
                  select *
                  from bank_transactions 
                  where Transaction_id=%s
                  and end_date is NULL
                  """
            
            cursor.execute(query,(id,))
            existing_row_DB=cursor.fetchone()

            if existing_row_DB is None:
                logger.info(f"inserted{id}")
                cursor.execute(insert_query,row_values + (datetime.now(), None))

            else:

                existing_business_values = tuple(
                round(float(x),2) if str(x).replace('.','',1).isdigit() else str(x)
                for x in existing_row_DB[1:17]
            )

                incoming_values = tuple(
                round(float(x),2) if str(x).replace('.','',1).isdigit() else str(x)
                for x in row_values
)

                if existing_business_values != incoming_values:   #SHOULD IMPROVISE - why???? The order will be diff, an

                    print(existing_business_values)
                    print(incoming_values)

    
                    """

                    #CASE 1: OVERWRITE | SCD TYPE- 1
                   # -----------------------------
                    logger.info(f"overwrite{id}")

                    cursor.execute(update_query,
                            (
                            row_values[1],
                            row_values[2],
                            row_values[3],
                            row_values[4],
                            row_values[5],
                            row_values[6],
                            row_values[7],
                            row_values[8],
                            row_values[9],
                            row_values[10],
                            row_values[11],
                            row_values[12],
                            row_values[13],
                            row_values[14],
                            row_values[15],
                            id
                           )
                        )
                    
                   """

                    #CASE 2: HISTORY PRESERVATION | SCD TYPE 2
                #--------------------------------------------------
                    logger.info(f"History Preservation {id}")
                    
                    

                    query2="""
                           update bank_transactions
                           set end_date=NOW()
                           where Transaction_id=%s and end_date is NULL

                           """
                    cursor.execute(query2,(id,))

                    
                        
                    
                    cursor.execute(insert_query,row_values+(datetime.now(),None))

                else:
                    logger.info(f"skipped:{id}")
 
            conn.commit()
            logger.info(f"insertion fo {id} successfull")
            

        except Exception as e:
            print(f" error:{e}")
            conn.rollback()
            raise

        finally:
            cursor.close()

@execution_time
def run_qa_checks(conn):
    """
    Run basic QA checks on the loaded data
    """
    try:
        cursor = conn.cursor()
        
        # Row count
        cursor.execute("SELECT COUNT(*) FROM bank_transactions;")
        row_count = cursor.fetchone()[0]
        print(f"\nTotal rows in database: {row_count}")
        
        # Null counts per column
        print("\nDuplicate PK check")
        cursor.execute("""
            SELECT
            transaction_id
            
            FROM bank_transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1;
        """)

        duplicate_keys=list(cursor.fetchall())
        print(f"duplicate key list {duplicate_keys}")
        cursor.execute("""
        SELECT transaction_id
        FROM bank_transactions
        WHERE customer_age < 18
        
        """)

        print(f"illegal age{cursor.fetchall()}")
        cursor.execute("""
            SELECT transaction_id
            FROM bank_transactions
            WHERE account_balance < 0;
                       """)
        print(f"negative balance list{cursor.fetchall()}")
        cursor.close()
    except Exception as e:
        print(f"Error running QA checks: {e}")
        raise

def raw_to_archive(key):
    s3=boto3.client("s3")
    copy_source={
         "Bucket": 'devika-etl-pipeline-practice',
         "Key": key
     }
    
    s3.copy_object(
        CopySource=copy_source,
        Bucket='devika-etl-pipeline-practice',
        Key = key.replace("raw/", "archive/")

    )
    print("copied to archive")
    s3.delete_object(
        Bucket='devika-etl-pipeline-practice',
        Key=key
    )
    print(" deleted raw")
     
    
def list_objects_in_s3():
    s3=boto3.client("s3")
    response=s3.list_objects_v2(
        Bucket="devika-etl-pipeline-practice",
        Prefix="raw/"
    )

    if "Contents" in response:
        list_of_files = []
        for obj in response["Contents"]:
            list_of_files.append(obj["Key"])
        return list_of_files
        
    else:
        return None




@manage_connection(get_db_connections)
def main(conn):
    """
    Main ETL pipeline
    """

    logger.info("ETL started")
    
    list_of_files=list_objects_in_s3()
    if list_of_files:
        for key in list_of_files:

            raw_data = preprocessing.get_data_from_s3(key)
            logger.info(" data fetched form s3")

            df = preprocessing.load_and_clean_data(raw_data)
            logger.info("preprocessed")

            # Step 2
            logger.info("Database connected")

            # Step 3
            print("\n=== Step 3: Create Table ===")
            create_table(conn)

            # Step 4
            print("\n=== Step 4: Load Data ===")
            insert_data(conn, df)

            # Step 5
            print("\n=== Step 5: QA Checks ===")
            run_qa_checks(conn)

            print(" raw to archive loading")
            raw_to_archive(key)

    print("\nETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()