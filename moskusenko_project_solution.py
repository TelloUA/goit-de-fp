from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from moskusenko_landing_to_bronze import landing_to_bronze_task
from moskusenko_bronze_to_silver import bronze_to_silver_task
from moskusenko_silver_to_gold import silver_to_gold_task

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 20, 0, 0),
}

dag = DAG(
    'moskusenko_olympic_data_pipeline',
    default_args=default_args,
    description='Olympic Data Lake Pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['moskusenko', 'final']
)

landing_to_bronze = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application='dags/moskusenko_landing_to_bronze.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag
)

bronze_to_silver = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application='dags/moskusenko_bronze_to_silver.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag
)

silver_to_gold = SparkSubmitOperator(
    task_id='silver_to_gold',
    application='dags/moskusenko_silver_to_gold.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag
)

landing_to_bronze >> bronze_to_silver >> silver_to_gold