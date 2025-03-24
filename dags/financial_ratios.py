from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
from crawler.crawl import fetch_financial_ratio_quarter
import json

def fetch_data(**kwargs):
    try:
        df = fetch_financial_ratio_quarter()
    except Exception as e:
        raise Exception(f"Lỗi khi lấy dữ liệu: {e}")
    
    if df.empty:
        return None
    
    df = df.rename(columns={
        "priceToEarning": "price_to_earning",
        "priceToBook": "price_to_book",
        "roe": "roe",
        "roa": "roa",
        "debtOnEquity": "debt_on_equity",
        "ebitOnInterest": "ebit_on_interest",
        "epsChange": "eps_change",
        "earningPerShare": "earning_per_share",
        "currentPayment": "current_payment",
        "quickPayment": "quick_payment"
    })
    
    df = df.where(pd.notna(df), None)
    df["ticker"] = df["ticker"].astype(str)
    df["quarter"] = pd.to_numeric(df["quarter"], errors='coerce').fillna(0).astype(int)
    df["year"] = pd.to_numeric(df["year"], errors='coerce').fillna(0).astype(int)
    
    json_data = df.to_json(orient='records')
    kwargs['ti'].xcom_push(key='financial_data', value=json_data)

def generate_sql(**kwargs):
    ti = kwargs['ti']
    financial_data_json = ti.xcom_pull(task_ids='fetch_data_task', key='financial_data')
    financial_data = json.loads(financial_data_json) if financial_data_json else []
    
    if not financial_data:
        return "SELECT 1;"

    values = []
    for row in financial_data:
        values.append(f"""(
            '{row.get('ticker', '')}',
            {row.get('quarter', 0)},
            {row.get('year', 0)},
            {row.get('price_to_earning', 'NULL') if row.get('price_to_earning') is not None else 'NULL'},
            {row.get('price_to_book', 'NULL') if row.get('price_to_book') is not None else 'NULL'},
            {row.get('roa', 'NULL') if row.get('roa') is not None else 'NULL'},
            {row.get('roe', 'NULL') if row.get('roe') is not None else 'NULL'},
            {row.get('debt_on_equity', 'NULL') if row.get('debt_on_equity') is not None else 'NULL'},
            {row.get('ebit_on_interest', 'NULL') if row.get('ebit_on_interest') is not None else 'NULL'},
            {row.get('eps_change', 'NULL') if row.get('eps_change') is not None else 'NULL'},
            {row.get('earning_per_share', 'NULL') if row.get('earning_per_share') is not None else 'NULL'},
            {row.get('current_payment', 'NULL') if row.get('current_payment') is not None else 'NULL'},
            {row.get('quick_payment', 'NULL') if row.get('quick_payment') is not None else 'NULL'}
        )""")

    sql_query = f"""
        INSERT INTO financial_ratios_quarter (
            ticker, quarter, year, price_to_earning, price_to_book, roa, roe,
            debt_on_equity, ebit_on_interest, eps_change, earning_per_share, 
            current_payment, quick_payment
        ) VALUES {", ".join(values)}
        ON CONFLICT (ticker, quarter, year) 
        DO UPDATE SET 
            price_to_earning = EXCLUDED.price_to_earning,
            price_to_book = EXCLUDED.price_to_book,
            roa = EXCLUDED.roa,
            roe = EXCLUDED.roe,
            debt_on_equity = EXCLUDED.debt_on_equity,
            ebit_on_interest = EXCLUDED.ebit_on_interest,
            eps_change = EXCLUDED.eps_change,
            earning_per_share = EXCLUDED.earning_per_share,
            current_payment = EXCLUDED.current_payment,
            quick_payment = EXCLUDED.quick_payment;
    """

    ti.xcom_push(key='insert_sql', value=sql_query)

default_args = {
    'owner': 'financial',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False
}

dag = DAG(
    'ingest_financial_ratios_quarter',
    default_args=default_args,
    description='Load financial ratio quarterly data to PostgreSQL',
    schedule_interval='@quarterly',
    max_active_runs=1
)

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_demo",
    sql="""
        CREATE TABLE IF NOT EXISTS financial_ratios_quarter (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) REFERENCES companies(ticker) ON DELETE CASCADE,
            quarter INTEGER NOT NULL,
            year INTEGER NOT NULL,
            price_to_earning NUMERIC,
            price_to_book NUMERIC,
            roa NUMERIC,
            roe NUMERIC,
            debt_on_equity NUMERIC,
            ebit_on_interest NUMERIC,
            eps_change NUMERIC,
            earning_per_share NUMERIC,
            current_payment NUMERIC,
            quick_payment NUMERIC,
            
            UNIQUE (ticker, quarter, year)
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