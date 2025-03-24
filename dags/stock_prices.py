from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import json
from crawler.crawl import fetch_stock_prices

default_args = {
    'owner': 'stock_data',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}



dag = DAG(
    'ingest_stock_prices_dag',
    default_args=default_args,
    description='Load stock prices data to PostgreSQL',
    schedule_interval = '0 10 * * *', 
    max_active_runs=1,
    tags=["daily"]  
)


def fetch_data(**kwargs):
    today = datetime.now().date()
    df = fetch_stock_prices(start=str(today), end=str(today))
    
    if df.empty:
        return None
    
    df = df.rename(columns={
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
        "volumn": "volume"
    })
    
    df = df.where(pd.notna(df), None)
    json_data = df.to_json(orient='records')
    kwargs['ti'].xcom_push(key='stock_data', value=json_data)

def generate_sql(**kwargs):
    ti = kwargs['ti']
    stock_data_json = ti.xcom_pull(task_ids='fetch_data_task', key='stock_data')
    stock_data = json.loads(stock_data_json) if stock_data_json else []
    
    if not stock_data:
        return "SELECT 1;"
    
    insert_stmt = """
        INSERT INTO stock_prices (ticker, open_price, high_price, low_price, close_price, volume, date) VALUES
    """
    
    values = [
        f"('" + row.get('ticker', '') + "', "
        f"{row.get('open_price', 'NULL') if row.get('open_price') is not None else 'NULL'}, "
        f"{row.get('high_price', 'NULL') if row.get('high_price') is not None else 'NULL'}, "
        f"{row.get('low_price', 'NULL') if row.get('low_price') is not None else 'NULL'}, "
        f"{row.get('close_price', 'NULL') if row.get('close_price') is not None else 'NULL'}, "
        f"{row.get('volume', 'NULL') if row.get('volume') is not None else 'NULL'}, "
        f"'{row.get('date', '')}'" 
        ")" for row in stock_data
    ]
    
    sql_query = insert_stmt + ",\n".join(values) + ";"
    ti.xcom_push(key='insert_sql', value=sql_query)

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_demo",
    sql="""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) REFERENCES companies(ticker) ON DELETE CASCADE,
            open_price NUMERIC,
            high_price NUMERIC,
            low_price NUMERIC,
            close_price NUMERIC,
            volume NUMERIC,
            date DATE NOT NULL
        );
    """,
    dag=dag,
)

fetch_data_task = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data,
    dag=dag
)

generate_sql_task = PythonOperator(
    task_id='generate_sql_task',
    python_callable=generate_sql,
    dag=dag
)

insert_data_task = PostgresOperator(
    task_id="insert_data_task",
    postgres_conn_id="postgres_demo",
    sql="{{ ti.xcom_pull(task_ids='generate_sql_task', key='insert_sql') }}",
    dag=dag
)

end_operator = EmptyOperator(task_id='End_execution', dag=dag)

start_operator >> create_table >> fetch_data_task >> generate_sql_task >> insert_data_task >> end_operator
