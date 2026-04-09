from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "haji",
    "depends_on_past" : False,
    "retries": 1,
}

with DAG(
    dag_id = "crypto_market_data_pipeline",
    default_args = default_args,
    description = "ETL pipeline for crypto market data" ,
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "crypto", "data-engineering"],
) as dag:
    
    extract_task = BashOperator(
        task_id = "extract_api_data",
        bash_command = "python /opt/airflow/scripts/extract_api_data.py",
    )

    transform_task = BashOperator(
        task_id = "transform_crypto_data",
        bash_command = "python /opt/airflow/scripts/transform_crypto_data.py"
    )
    
    load_task = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/scripts/load_to_postgres.py",
    )

    extract_task >> transform_task >> load_task