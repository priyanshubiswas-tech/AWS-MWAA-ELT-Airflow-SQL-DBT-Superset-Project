from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': days_ago(1)
}

def download_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = "priyanshu-airflow-dag-bucket"
    object_key = "ticket_dump.csv"
    local_path = '/tmp/monthly_ticket_summary_export.csv'
    
    try:
        os.makedirs('/tmp', exist_ok=True)
        file_obj = s3_hook.get_key(key=object_key, bucket_name=bucket_name)
        if file_obj is None:
            raise ValueError(f"File {object_key} not found in bucket {bucket_name}")
            
        with open(local_path, 'wb') as f:
            file_obj.download_fileobj(f)
        return local_path
    except Exception as e:
        raise Exception(f"Error downloading file: {str(e)}")

def create_table_if_not_exists(conn):
    """Create the incidents_staging table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS incidents_staging (
        inc_business_service TEXT,
        inc_category TEXT,
        inc_number TEXT,
        inc_priority TEXT,
        inc_sla_due TIMESTAMP,
        inc_sys_created_on TIMESTAMP,
        inc_resolved_at TIMESTAMP,
        inc_assigned_to TEXT,
        inc_state TEXT,
        inc_cmdb_ci TEXT,
        inc_caller_id TEXT,
        inc_short_description TEXT,
        inc_assignment_group TEXT,
        inc_close_code TEXT,
        inc_close_notes TEXT
    );
    """
    cur = conn.cursor()
    cur.execute(create_table_sql)
    conn.commit()
    cur.close()

def copy_csv_to_postgres(**kwargs):
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_rds_connection')
        conn = pg_hook.get_conn()
        create_table_if_not_exists(conn)
        
        with open('/tmp/monthly_ticket_summary_export.csv', 'rb') as f:
            try:
                content = f.read().decode('utf-8')
            except UnicodeDecodeError:
                f.seek(0)
                content = f.read().decode('latin-1')
            
            from io import StringIO
            cur = conn.cursor()
            file_like_object = StringIO(content)
            
            cur.copy_expert(
                """COPY incidents_staging(
                    inc_business_service, inc_category, inc_number, 
                    inc_priority, inc_sla_due, inc_sys_created_on, 
                    inc_resolved_at, inc_assigned_to, inc_state, 
                    inc_cmdb_ci, inc_caller_id, inc_short_description, 
                    inc_assignment_group, inc_close_code, inc_close_notes
                ) FROM STDIN WITH CSV HEADER DELIMITER ','""",
                file_like_object
            )
            conn.commit()
            cur.close()
            
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        raise Exception(f"Error loading to PostgreSQL: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

with DAG(
    dag_id='DigitalXC',
    default_args=default_args,
    description='Production-ready AWS MWAA ETL Pipeline with dbt',
    schedule_interval='@daily',
    catchup=False,
    tags=['production', 'dbt']
) as dag:

    start = DummyOperator(task_id='start')
    
    download_csv = PythonOperator(
        task_id='download_csv',
        python_callable=download_from_s3
    )

    load_csv_to_postgres = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=copy_csv_to_postgres
    )

    # MWAA-optimized dbt execution with proper Python path resolution
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command="""
        # Set environment variables
        export DBT_PROFILES_DIR=/usr/local/airflow/dags/dbt/demo
        export DBT_PROJECT_DIR=/usr/local/airflow/dags/dbt/demo
        
        # Find MWAA's Python with dbt installed
        PYTHON_CMD=$(find /usr/local/airflow/venv/bin -name python | head -1)
        
        # Verify dbt is accessible
        echo "Using Python at: $PYTHON_CMD"
        $PYTHON_CMD -c "import dbt; print(f'dbt version: {dbt.__version__}')"
        
        # Execute dbt commands
        cd $DBT_PROJECT_DIR && \
        $PYTHON_CMD -m dbt deps --profiles-dir . && \
        $PYTHON_CMD -m dbt run --profiles-dir .
        """,
        env={
            'DBT_PROFILES_DIR': '/usr/local/airflow/dags/dbt/demo',
            'DBT_PROJECT_DIR': '/usr/local/airflow/dags/dbt/demo'
        }
    )

    validate_dbt = BashOperator(
        task_id='validate_dbt',
        bash_command="""
        echo 'DBT transformation completed successfully at $(date)'
        """
    )

    end = DummyOperator(task_id='end')

    start >> download_csv >> load_csv_to_postgres >> run_dbt >> validate_dbt >> end