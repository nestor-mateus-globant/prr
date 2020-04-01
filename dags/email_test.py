from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'fabio.lissi',
    'start_date': datetime(2018, 8, 20),
    'email': ['sdcde@smiledirectclub.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'email_test', default_args=default_args, schedule_interval='45 09 * * *',
    dagrun_timeout=timedelta(hours=2))

email_test = EmailOperator(
    task_id='email_test',
    to=["sdcde@smiledirectclub.com"],
    retries=3,
    dag=dag,
    subject="Running",
    html_content="<h3>Airflow is running</h3>")