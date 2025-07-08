from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging

# =======================
# ✅ Setup Logging
# =======================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =======================
# ✅ Utility Functions
# =======================
def get_engine(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    uri = f"postgresql+psycopg2://{conn.login}:{quote_plus(conn.password)}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(uri)

def insert_date_if_missing(conn, date_value):
    logger.debug(f"📅 Ensuring date in d_date: {date_value}")
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
    logger.debug(f"🧩 Ensuring {value} exists in {table}.{column}")
    with conn.begin():
        conn.execute(text(f"""
            INSERT INTO analytics.{table} ({column})
            VALUES (:value)
            ON CONFLICT ({column}) DO NOTHING
        """), {"value": value})

# =======================
# 🚀 Main ETL Logic
# =======================
def load_inventory_requests_from_payload(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    logger.info("📥 Received payload:")
    logger.info(conf)

    records = conf.get("records", [])
    if not records or not isinstance(records, list):
        logger.warning("❌ No valid 'records' provided in DAG payload.")
        return

    dest_engine = get_engine('superset')
    upserted = 0

    with dest_engine.connect() as conn:
        for row in records:
            logger.info(f"🔍 Processing row: {row}")
            request_id = row.get("request_id")
            item_name = row.get("item_name")
            quantity = row.get("quantity")
            location = row.get("location")
            requested_date_str = row.get("requested_date")

            if not all([request_id, item_name, quantity, location, requested_date_str]):
                logger.warning(f"⚠️ Incomplete record: {row}")
                continue

            try:
                requested_date = datetime.strptime(requested_date_str, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"⚠️ Invalid date format for request_id {request_id}: {requested_date_str}")
                continue

            # Insert dimension records if missing
            insert_date_if_missing(conn, requested_date)
            insert_if_missing(conn, "d_item", "item_name", item_name)
            insert_if_missing(conn, "d_project", "project_name", location)

            # Look up dimension keys
            date_key = conn.execute(text("SELECT date_key FROM analytics.d_date WHERE date = :date"),
                                    {"date": requested_date}).scalar()
            item_key = conn.execute(text("SELECT item_key FROM analytics.d_item WHERE item_name = :val"),
                                    {"val": item_name}).scalar()
            project_key = conn.execute(text("SELECT project_key FROM analytics.d_project WHERE project_name = :val"),
                                       {"val": location}).scalar()

            logger.debug(f"🔑 Keys for request_id {request_id} → date_key={date_key}, item_key={item_key}, project_key={project_key}")

            if not all([date_key, item_key, project_key]):
                logger.warning(f"⚠️ Skipping request_id {request_id} — missing keys")
                continue

            try:
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
                    logger.info(f"✅ Upserted request_id: {request_id}")
            except Exception as e:
                logger.error(f"❌ Failed to upsert request_id {request_id}: {e}")

    logger.info(f"🏁 Finished ETL. Total upserted: {upserted} record(s)")

# =======================
# 🗓️ DAG Definition
# =======================
with DAG(
    dag_id='inventory_requests_etl_from_payload_1',
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
