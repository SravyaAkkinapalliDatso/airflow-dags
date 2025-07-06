from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import create_engine, text
import logging
from urllib.parse import quote_plus

# 📦 Engine utility
def get_engine(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    uri = f"postgresql+psycopg2://{conn.login}:{quote_plus(conn.password)}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(uri)

# 🧠 Insert date if missing
def insert_date_if_missing(conn, date_value):
    try:
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
        logging.info(f"✅ Date ensured: {date_value}")
    except Exception as e:
        logging.error(f"❌ Error inserting date {date_value}: {e}")

# 🧠 Insert item if missing
def insert_item_if_missing(conn, item_name):
    try:
        with conn.begin():
            conn.execute(text("""
                INSERT INTO analytics.d_item (item_name)
                VALUES (:item_name)
                ON CONFLICT (item_name) DO NOTHING
            """), {"item_name": item_name})
        logging.info(f"✅ Item ensured: {item_name}")
    except Exception as e:
        logging.error(f"❌ Error inserting item '{item_name}': {e}")

# 🧠 Insert project if missing
def insert_project_if_missing(conn, project_name):
    try:
        with conn.begin():
            conn.execute(text("""
                INSERT INTO analytics.d_project (project_name)
                VALUES (:project_name)
                ON CONFLICT (project_name) DO NOTHING
            """), {"project_name": project_name})
        logging.info(f"✅ Project ensured: {project_name}")
    except Exception as e:
        logging.error(f"❌ Error inserting project '{project_name}': {e}")

# 🛠️ Main ETL
def load_inventory_data():
    src = PostgresHook(postgres_conn_id='avenue')
    dest_engine = get_engine('superset')

    inventory_data = src.get_records("""
        SELECT inventory_id, item_name, quantity, location, updated_date
        FROM inventory_management.inventory
    """)

    logging.info(f"📦 Retrieved {len(inventory_data)} inventory records")

    with dest_engine.connect() as conn:
        upserted = 0
        for row in inventory_data:
            inventory_id, item_name, quantity, location, updated_date = row
            logging.info(f"🔍 Processing: {inventory_id}, {item_name}, {quantity}, {location}, {updated_date}")

            # Step 1: Date handling
            date_val = updated_date.date()
            insert_date_if_missing(conn, date_val)
            result = conn.execute(text("""
                SELECT date_key FROM analytics.d_date WHERE date = :date
            """), {"date": date_val}).fetchone()
            if result is None:
                logging.warning(f"⚠️ Missing date_key for {date_val}, skipping")
                continue
            date_key = result[0]

            # Step 2: Item handling
            insert_item_if_missing(conn, item_name)
            item_key = conn.execute(text("""
                SELECT item_key FROM analytics.d_item WHERE item_name = :item_name
            """), {"item_name": item_name}).scalar()
            if not item_key:
                logging.warning(f"⚠️ Missing item_key for {item_name}, skipping")
                continue

            # Step 3: Project handling
            insert_project_if_missing(conn, location)
            project_key = conn.execute(text("""
                SELECT project_key FROM analytics.d_project WHERE project_name = :project_name
            """), {"project_name": location}).scalar()
            if not project_key:
                logging.warning(f"⚠️ Missing project_key for {location}, skipping")
                continue

            # Step 4: Upsert into fact table
            try:
                with conn.begin():
                    conn.execute(text("""
                        INSERT INTO analytics.f_inventory (
                            inventory_id, item_key, quantity, project_key, date_key
                        )
                        VALUES (
                            :inventory_id, :item_key, :quantity, :project_key, :date_key
                        )
                        ON CONFLICT (inventory_id) DO UPDATE SET
                            item_key = EXCLUDED.item_key,
                            quantity = EXCLUDED.quantity,
                            project_key = EXCLUDED.project_key,
                            date_key = EXCLUDED.date_key
                    """), {
                        "inventory_id": inventory_id,
                        "item_key": item_key,
                        "quantity": quantity,
                        "project_key": project_key,
                        "date_key": date_key
                    })
                upserted += 1
                logging.info(f"✅ Upserted inventory_id: {inventory_id}")
            except Exception as e:
                logging.error(f"❌ Failed upserting inventory_id {inventory_id}: {e}")

        logging.info(f"🚀 Total upserts into f_inventory: {upserted}")

# DAG Definition
with DAG(
    dag_id='inventory_fact_etl_final_5',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    load_inventory = PythonOperator(
        task_id='load_inventory_data',
        python_callable=load_inventory_data
    )