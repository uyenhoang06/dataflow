import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from crawler.crawl import fetch_companies
import json

default_args = {
    'owner': 'companies',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False
}

dag = DAG(
    "ingest_companies",
    default_args=default_args,
    description="Load company data to PostgreSQL",
    schedule_interval="@weekly",
    max_active_runs=1
)

def fetch_data(**kwargs):
    try:
        df = fetch_companies()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Lỗi kết nối API: {e}")

    if df.empty:
        return None
    
    json_data = df.to_json(orient='records')
    kwargs['ti'].xcom_push(key='company_data', value=json_data)


fetch_data_task = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

def generate_sql(**kwargs):
    ti = kwargs['ti']
    # company_data = ti.xcom_pull(task_ids='fetch_data_task', key='company_data')
    company_data_json = ti.xcom_pull(task_ids='fetch_data_task', key='company_data')
    company_data = json.loads(company_data_json) if company_data_json else []

    if not company_data:
        return "SELECT 1;"
    
    values = []
    for row in company_data:
        ticker = f"'{row['ticker']}'" if row.get('ticker') else "NULL"
        name = f"'{row['name']}'" if row.get('name') else "NULL"
        industry = f"'{row['industry']}'" if row.get('industry') else "NULL"
        exchange = f"'{row['exchange']}'" if row.get('exchange') else "NULL"
        established_year = row.get('established_year', "NULL") if row.get('established_year') is not None else 'NULL'
        foreign_percent = row.get('foreign_percent', "NULL") if row.get('foreign_percent') is not None else 'NULL'

        values.append(f"({ticker}, {name}, {industry}, {exchange}, {established_year}, {foreign_percent})")

    values_str = ", ".join(values)


    sql_query = f"""
        INSERT INTO companies (ticker, name, industry, exchange, established_year, foreign_percent) 
        VALUES {values_str}
        ON CONFLICT (ticker) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            industry = EXCLUDED.industry,
            exchange = EXCLUDED.exchange,
            established_year = EXCLUDED.established_year,
            foreign_percent = EXCLUDED.foreign_percent;
    """
    
    ti.xcom_push(key='insert_sql', value=sql_query)




generate_sql_task = PythonOperator(
    task_id='generate_sql_task',
    python_callable=generate_sql,
    provide_context=True,
    dag=dag
)

insert_data_task = PostgresOperator(
    task_id="insert_data_task",
    postgres_conn_id="postgres_demo",
    sql="{{ ti.xcom_pull(task_ids='generate_sql_task', key='insert_sql') }}",
    dag=dag
)

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_demo",
    sql="""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(255),
            industry VARCHAR(255),
            exchange VARCHAR(50),
            established_year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag,
)

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)
end_operator = EmptyOperator(task_id='End_execution', dag=dag)

start_operator >> create_table >> fetch_data_task >> generate_sql_task >> insert_data_task >> end_operator
