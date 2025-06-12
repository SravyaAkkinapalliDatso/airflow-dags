from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

# Default DAG args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define DAG
dag = DAG(
    dag_id='inventory_18',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

# ---------- Utility Functions ----------

def execute_sql_on_dashboards(sql):
    hook = PostgresHook(postgres_conn_id='dashboards')
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for statement in sql.strip().split(';'):
                if statement.strip():
                    cursor.execute(statement)
        conn.commit()

def fetch_data_from_avenue(query):
    hook = PostgresHook(postgres_conn_id='avenue')
    return hook.get_pandas_df(query)

# ---------- Table Creation Task ----------

def create_olap_tables():
    sql = """
        CREATE SCHEMA IF NOT EXISTS inventory;

        CREATE TABLE IF NOT EXISTS inventory.d_item (
            item_key SERIAL PRIMARY KEY,
            item_name VARCHAR,
            parsed_item_name TEXT
        );

        CREATE TABLE IF NOT EXISTS inventory.d_location (
            location_key SERIAL PRIMARY KEY,
            location VARCHAR,
            parsed_location TEXT
        );

        CREATE TABLE IF NOT EXISTS inventory.d_warehouse (
            warehouse_key SERIAL PRIMARY KEY,
            warehouse VARCHAR,
            parsed_warehouse TEXT
        );

        CREATE TABLE IF NOT EXISTS inventory.d_status (
            status_key SERIAL PRIMARY KEY,
            status VARCHAR
        );

        CREATE TABLE IF NOT EXISTS inventory.d_invoice (
            invoice_key SERIAL PRIMARY KEY,
            invoice_id VARCHAR,
            avenue_created_invoice_id TEXT
        );

        CREATE TABLE IF NOT EXISTS inventory.f_inventory (
            inventory_key SERIAL PRIMARY KEY,
            inventory_id VARCHAR,
            item_key INT REFERENCES inventory.d_item(item_key),
            location_key INT REFERENCES inventory.d_location(location_key),
            warehouse_key INT REFERENCES inventory.d_warehouse(warehouse_key),
            status_key INT REFERENCES inventory.d_status(status_key),
            invoice_key INT REFERENCES inventory.d_invoice(invoice_key),
            created_date_key INT REFERENCES dashboards.d_date(date_key),
            updated_date_key INT REFERENCES dashboards.d_date(date_key),
            requested_quantity NUMERIC,
            raised_quantity NUMERIC,
            issued_quantity NUMERIC,
            total_received_quantity NUMERIC,
            available_quantity NUMERIC,
            quantity NUMERIC
        );

        CREATE TABLE IF NOT EXISTS inventory.f_inventory_batch (
            batch_key SERIAL PRIMARY KEY,
            batch_id VARCHAR,
            item_key INT REFERENCES inventory.d_item(item_key),
            location_key INT REFERENCES inventory.d_location(location_key),
            warehouse_key INT REFERENCES inventory.d_warehouse(warehouse_key),
            status_key INT REFERENCES inventory.d_status(status_key),
            invoice_key INT REFERENCES inventory.d_invoice(invoice_key),
            created_date_key INT REFERENCES dashboards.d_date(date_key),
            updated_date_key INT REFERENCES dashboards.d_date(date_key),
            quantity NUMERIC,
            unit_price NUMERIC,
            requested_quantity NUMERIC,
            raised_quantity NUMERIC,
            issued_quantity NUMERIC,
            total_received_quantity NUMERIC,
            available_quantity NUMERIC
        );
    """
    execute_sql_on_dashboards(sql)

# ---------- Load Dimensions Task ----------

def load_dimensions():
    query = """
        SELECT DISTINCT 
            item_name, parsed_item_name, 
            location, parsed_location, 
            warehouse, parsed_warehouse, 
            status, invoice_id, avenue_created_invoice_id 
        FROM inventory_management.inventory
    """
    df = fetch_data_from_avenue(query)

    execute_sql_on_dashboards("TRUNCATE TABLE inventory.d_item, inventory.d_location, inventory.d_warehouse, inventory.d_status, inventory.d_invoice RESTART IDENTITY CASCADE;")

    hook = PostgresHook(postgres_conn_id='dashboards')
    conn = hook.get_conn()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("INSERT INTO inventory.d_item (item_name, parsed_item_name) VALUES (%s, %s) ON CONFLICT DO NOTHING", (row.item_name, row.parsed_item_name))
        cursor.execute("INSERT INTO inventory.d_location (location, parsed_location) VALUES (%s, %s) ON CONFLICT DO NOTHING", (row.location, row.parsed_location))
        cursor.execute("INSERT INTO inventory.d_warehouse (warehouse, parsed_warehouse) VALUES (%s, %s) ON CONFLICT DO NOTHING", (row.warehouse, row.parsed_warehouse))
        cursor.execute("INSERT INTO inventory.d_status (status) VALUES (%s) ON CONFLICT DO NOTHING", (row.status,))
        cursor.execute("INSERT INTO inventory.d_invoice (invoice_id, avenue_created_invoice_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (row.invoice_id, row.avenue_created_invoice_id))

    conn.commit()

# ---------- Load Fact Inventory ----------

def load_fact_inventory():
    df = fetch_data_from_avenue("SELECT * FROM inventory_management.inventory")
    hook = PostgresHook(postgres_conn_id='dashboards')
    conn = hook.get_conn()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO inventory.f_inventory (
                inventory_id, item_key, location_key, warehouse_key, status_key, invoice_key,
                created_date_key, updated_date_key, requested_quantity, raised_quantity, issued_quantity,
                total_received_quantity, available_quantity, quantity
            ) VALUES (
                %s,
                (SELECT item_key FROM inventory.d_item WHERE parsed_item_name = %s LIMIT 1),
                (SELECT location_key FROM inventory.d_location WHERE parsed_location = %s LIMIT 1),
                (SELECT warehouse_key FROM inventory.d_warehouse WHERE parsed_warehouse = %s LIMIT 1),
                (SELECT status_key FROM inventory.d_status WHERE status = %s LIMIT 1),
                (SELECT invoice_key FROM inventory.d_invoice WHERE avenue_created_invoice_id = %s LIMIT 1),
                (SELECT date_key FROM dashboards.d_date WHERE DATE(date) = %s LIMIT 1),
                (SELECT date_key FROM dashboards.d_date WHERE DATE(date) = %s LIMIT 1),
                %s, %s, %s, %s, %s, %s
            )
        """, (
            row['inventory_id'], row['parsed_item_name'], row['parsed_location'], row['parsed_warehouse'],
            row['status'], row['avenue_created_invoice_id'], row['created_date'].date(), row['updated_date'].date(),
            row['requested_quantity'], row['raised_quantity'], row['issued_quantity'],
            row['total_received_quantity'], row['available_quantity'], row['quantity']
        ))

    conn.commit()

# ---------- REPLACED Load Fact Inventory Batch (Optimized Pandas version) ----------

def load_fact_inventory_batch():
    query = "SELECT * FROM inventory_management.inventory_batch"
    df = fetch_data_from_avenue(query)

    dashboards_hook = PostgresHook(postgres_conn_id='dashboards')
    engine = dashboards_hook.get_sqlalchemy_engine()

    def get_dim_key_map(table, key_col, ref_col):
        dim_df = pd.read_sql(f"SELECT {key_col}, {ref_col} FROM inventory.{table}", engine)
        return dim_df.set_index(ref_col)[key_col].to_dict()

    def get_date_key_map():
        df_dates = pd.read_sql("SELECT date_key, date::date FROM dashboards.d_date", engine)
        return df_dates.set_index("date")["date_key"].to_dict()

    df['item_key'] = df['item_name'].map(get_dim_key_map('d_item', 'item_key', 'item_name'))
    df['location_key'] = df['location'].map(get_dim_key_map('d_location', 'location_key', 'location'))
    df['warehouse_key'] = df['warehouse'].map(get_dim_key_map('d_warehouse', 'warehouse_key', 'warehouse'))
    df['status_key'] = df['status'].map(get_dim_key_map('d_status', 'status_key', 'status'))
    df['invoice_key'] = df['invoice_id'].astype(str).map(get_dim_key_map('d_invoice', 'invoice_key', 'invoice_id'))

    date_map = get_date_key_map()
    df['created_date_key'] = pd.to_datetime(df['created_date']).dt.date.map(date_map)
    df['updated_date_key'] = pd.to_datetime(df['updated_date']).dt.date.map(date_map)

    df.drop(columns=['item_name', 'location', 'warehouse', 'status', 'invoice_id',
                     'qr_code_path', 'created_date', 'updated_date'], errors='ignore', inplace=True)

    df.to_sql('f_inventory_batch', engine, schema='inventory', if_exists='append', index=False)

# ---------- Task Definitions ----------

create_tables_task = PythonOperator(
    task_id='create_olap_tables',
    python_callable=create_olap_tables,
    dag=dag
)

load_dim_task = PythonOperator(
    task_id='load_dimensions',
    python_callable=load_dimensions,
    dag=dag
)

load_fact_inventory_task = PythonOperator(
    task_id='load_fact_inventory',
    python_callable=load_fact_inventory,
    dag=dag
)

load_fact_inventory_batch_task = PythonOperator(
    task_id='load_fact_inventory_batch',
    python_callable=load_fact_inventory_batch,
    dag=dag
)

# DAG Flow
create_tables_task >> load_dim_task >> [load_fact_inventory_task, load_fact_inventory_batch_task]