import os
import boto3
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ------------------------------
# MinIO settings
# ------------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"  # Airflow needs absolute path

# ------------------------------
# Snowflake settings
# ------------------------------
SNOWFLAKE_USER = "karanbhatt09"
SNOWFLAKE_PASSWORD = "KaranAnilBhatt7"
SNOWFLAKE_ACCOUNT = "vi04907.ap-southeast-1"
SNOWFLAKE_WAREHOUSE = "STOCK_WH"
SNOWFLAKE_DB = "Stocks_DB"
SNOWFLAKE_SCHEMA = "Stocks_Sch"
SNOWFLAKE_TABLE = "Bronze_Layer"  # Table to load JSON into

# ------------------------------
# DAG default arguments
# ------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------
# DAG definition
# ------------------------------
with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 1 minute
    catchup=False,
) as dag:

    # ------------------------------
    # Task 1: Download files from MinIO
    # ------------------------------
    def download_from_minio():
        os.makedirs(LOCAL_DIR, exist_ok=True)
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        local_files = []
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files.append(local_file)
        return local_files

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    # ------------------------------
    # Task 2: Load files to Snowflake
    # ------------------------------
    def load_to_snowflake(**kwargs):
        local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')
        if not local_files:
            print("No files to load.")
            return

        try:
            # Connect to Snowflake
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DB,
                schema=SNOWFLAKE_SCHEMA
            )
            cur = conn.cursor()

            # Upload files to table stage
            for f in local_files:
                cur.execute(f"PUT file://{f} @%{SNOWFLAKE_TABLE}")
                print(f"Uploaded {f} to Snowflake stage @%{SNOWFLAKE_TABLE}")

            # Load JSON into table
            cur.execute(f"""
                COPY INTO {SNOWFLAKE_TABLE} (v)
                FROM(
                    SELECT $1
                    FROM@%{SNOWFLAKE_TABLE}
                )
                FILE_FORMAT = (TYPE=JSON)
            """)
            print("COPY INTO executed successfully")

        except ProgrammingError as e:
            # Catch Snowflake errors like warehouse over quota
            print(f"Snowflake error code {e.errno}: {e.msg}")
            print("Skipping Snowflake load due to warehouse issue.")
            return  # Task succeeds but skips load

        finally:
            cur.close()
            conn.close()

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    # ------------------------------
    # Task dependencies
    # ------------------------------
    task1 >> task2
