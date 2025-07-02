from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

TARGET_SCHEMA = "analytics"

def create_inventory_tables():
    hook = PostgresHook(postgres_conn_id='superset')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_item (
                    item_key SERIAL PRIMARY KEY,
                    item_name TEXT UNIQUE,
                    category TEXT,
                    unit TEXT
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.fact_inventory_requests (
                    request_key SERIAL PRIMARY KEY,
                    request_id TEXT UNIQUE,
                    property_id TEXT,
                    item_key INT REFERENCES {TARGET_SCHEMA}.d_item(item_key),
                    quantity NUMERIC,
                    status TEXT,
                    raised_date_key INT,
                    updated_date_key INT
                );
            """)
            conn.commit()

def date_to_key(dt):
    return int(dt.strftime("%Y%m%d")) if dt else None

def load_inventory_requests():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='superset')

    data = src.get_records("""
        SELECT 
            ir.request_id,
            ir.property_id,
            ir.item_name,
            ir.category,
            ir.unit,
            ir.quantity,
            ir.status,
            ir.raised_at,
            ir.updated_at
        FROM inventory_management.inventory_requests ir
        WHERE ir.request_id IS NOT NULL
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                (
                    request_id, property_id, item_name, category, unit,
                    quantity, status, raised_at, updated_at
                ) = row

                raised_date_key = date_to_key(raised_at)
                updated_date_key = date_to_key(updated_at)

                # Insert into d_item
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.d_item (item_name, category, unit)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (item_name) DO NOTHING
                    RETURNING item_key;
                """, (item_name, category, unit))
                item_key_row = cur.fetchone()
                if not item_key_row:
                    cur.execute(f"SELECT item_key FROM {TARGET_SCHEMA}.d_item WHERE item_name = %s", (item_name,))
                    item_key = cur.fetchone()[0]
                else:
                    item_key = item_key_row[0]

                # Insert fact with deduplication
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.fact_inventory_requests (
                        request_id, property_id, item_key,
                        quantity, status, raised_date_key, updated_date_key
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {TARGET_SCHEMA}.fact_inventory_requests
                        WHERE request_id = %s
                    )
                """, (
                    request_id, property_id, item_key,
                    quantity, status, raised_date_key, updated_date_key,
                    request_id
                ))

            conn.commit()

# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='inv_rqst_etl',
    default_args=default_args,
    description='ETL for inventory requests into analytics schema',
    schedule_interval='@hourly',
    catchup=False,
    tags=['inventory', 'etl']
)

create_tables_task = PythonOperator(
    task_id='create_inventory_tables',
    python_callable=create_inventory_tables,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_inventory_data',
    python_callable=load_inventory_requests,
    dag=dag,
)

create_tables_task >> load_data_task
