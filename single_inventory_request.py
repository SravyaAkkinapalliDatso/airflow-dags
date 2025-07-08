from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging

# =======================
# ✅ Utility Functions
# =======================

def get_engine(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    uri = f"postgresql+psycopg2://{conn.login}:{quote_plus(conn.password)}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(uri)

def insert_date_if_missing(conn, date_value):
    with conn.begin():
        conn.execute(text("""
            INSERT INTO analytics.d_date (date, year, month, day, day_of_week, week_of_year, quarter)
            VALUES (
                :date,
                EXTRACT(YEAR FROM :date),
                EXTRACT(MONTH FROM :date),
                EXTRACT(DAY FROM :date),
                EXTRACT(DOW FROM :date),
                EXTRACT(WEEK FROM :date),
                EXTRACT(QUARTER FROM :date)
            )
            ON CONFLICT (date) DO NOTHING
        """), {"date": date_value})

def insert_if_missing(conn, table, column, value):
    with conn.begin():
        conn.execute(text(f"""
            INSERT INTO analytics.{table} ({column})
            VALUES (:value)
            ON CONFLICT ({column}) DO NOTHING
        """), {"value": value})

# =======================
# 🚀 Main ETL Logic from Payload
# =======================

def load_inventory_requests_from_payload(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    records = conf.get("records", [])

    if not records or not isinstance(records, list):
        logging.warning("❌ No valid 'records' provided in DAG payload.")
        return

    dest_engine = get_engine('superset')
    upserted = 0

    with dest_engine.connect() as conn:
        for row in records:
            request_id = row.get("request_id")
            item_name = row.get("item_name")
            quantity = row.get("quantity")
            location = row.get("location")
            requested_date_str = row.get("requested_date")

            if not all([request_id, item_name, quantity, location, requested_date_str]):
                logging.warning(f"⚠️ Skipping incomplete record: {row}")
                continue

            try:
                requested_date = datetime.strptime(requested_date_str, "%Y-%m-%d").date()
            except ValueError:
                logging.warning(f"⚠️ Invalid date format for request_id {request_id}")
                continue

            # Ensure dimensions
            insert_date_if_missing(conn, requested_date)
            insert_if_missing(conn, "d_item", "item_name", item_name)
            insert_if_missing(conn, "d_project", "project_name", location)

            # Lookups
            date_key = conn.execute(text("SELECT date_key FROM analytics.d_date WHERE date = :date"),
                                    {"date": requested_date}).scalar()
            item_key = conn.execute(text("SELECT item_key FROM analytics.d_item WHERE item_name = :val"),
                                    {"val": item_name}).scalar()
            project_key = conn.execute(text("SELECT project_key FROM analytics.d_project WHERE project_name = :val"),
                                       {"val": location}).scalar()

            if not all([date_key, item_key, project_key]):
                logging.warning(f"⚠️ Skipping request_id {request_id} — missing dimension key(s)")
                continue

            # Insert/Upsert into fact table
            with conn.begin():
                conn.execute(text("""
                    INSERT INTO analytics.f_inventory_requests (
                        request_id, item_key, quantity, project_key, date_key
                    )
                    VALUES (
                        :request_id, :item_key, :quantity, :project_key, :date_key
                    )
                    ON CONFLICT (request_id) DO UPDATE SET
                        item_key = EXCLUDED.item_key,
                        quantity = EXCLUDED.quantity,
                        project_key = EXCLUDED.project_key,
                        date_key = EXCLUDED.date_key
                """), {
                    "request_id": request_id,
                    "item_key": item_key,
                    "quantity": quantity,
                    "project_key": project_key,
                    "date_key": date_key
                })
                upserted += 1
                logging.info(f"✅ Upserted request_id: {request_id}")

    logging.info(f"🎯 Finished. Total inserted/updated: {upserted} record(s).")

# =======================
# 🗓️ DAG Definition
# =======================

with DAG(
    dag_id='inventory_requests_etl_from_payload',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    description="ETL inventory requests from direct payload input (no source table)"
) as dag:

    process_payload = PythonOperator(
        task_id='load_inventory_requests_from_payload',
        python_callable=load_inventory_requests_from_payload,
        provide_context=True
    )