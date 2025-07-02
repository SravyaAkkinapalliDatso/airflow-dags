from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

TARGET_SCHEMA = "analytics"

def create_inventory_stock_tables():
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

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.fact_inventory_stock (
                    stock_key SERIAL PRIMARY KEY,
                    item_key INT REFERENCES {TARGET_SCHEMA}.d_item(item_key),
                    project_id TEXT,
                    property_id TEXT,
                    location TEXT,
                    current_quantity NUMERIC,
                    spoiled_quantity NUMERIC,
                    updated_date_key INT
                );
            """)
            conn.commit()

def date_to_key(dt):
    return int(dt.strftime("%Y%m%d")) if dt else None

def load_inventory_stock():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='superset')

    data = src.get_records("""
        SELECT 
            item_name,
            category,
            unit,
            project_id,
            property_id,
            location,
            current_quantity,
            spoiled_quantity,
            updated_at
        FROM inventory_management.inventory
        WHERE item_name IS NOT NULL
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                (
                    item_name, category, unit,
                    project_id, property_id, location,
                    current_quantity, spoiled_quantity, updated_at
                ) = row

                updated_date_key = date_to_key(updated_at)

                # Insert or fetch item_key from d_item
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

                # Insert into fact_inventory_stock (dedup on item+location+property)
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.fact_inventory_stock (
                        item_key, project_id, property_id,
                        location, current_quantity, spoiled_quantity, updated_date_key
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {TARGET_SCHEMA}.fact_inventory_stock
                        WHERE item_key = %s AND property_id = %s AND location = %s
                    )
                """, (
                    item_key, project_id, property_id,
                    location, current_quantity, spoiled_quantity, updated_date_key,
                    item_key, property_id, location
                ))

            conn.commit()

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='inventory_stock_etl',
    default_args=default_args,
    description='ETL for inventory stock from source to analytics schema',
    schedule_interval='@hourly',
    catchup=False,
)

create_tables_task = PythonOperator(
    task_id='create_inventory_stock_tables',
    python_callable=create_inventory_stock_tables,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_inventory_stock',
    python_callable=load_inventory_stock,
    dag=dag,
)

create_tables_task >> load_data_task
