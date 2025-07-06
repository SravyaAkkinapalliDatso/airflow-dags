from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

def create_tables():
    tgt = PostgresHook(postgres_conn_id="superset")
    with tgt.get_conn() as tgt_conn:
        cur = tgt_conn.cursor()
        logging.info("🔧 Creating tables if not exist...")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics.d_employees (
                emp_key SERIAL PRIMARY KEY,
                emp_id TEXT UNIQUE,
                first_name TEXT,
                last_name TEXT,
                email TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics.d_item_type (
                item_type_key SERIAL PRIMARY KEY,
                item_type TEXT UNIQUE
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics.d_request_status (
                status_key SERIAL PRIMARY KEY,
                status_name TEXT UNIQUE
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics.fact_inventory_requests (
                request_key SERIAL PRIMARY KEY,
                request_id TEXT UNIQUE,
                property_key INTEGER,
                project_key INTEGER,
                item_key INTEGER,
                item_type_key INTEGER REFERENCES analytics.d_item_type(item_type_key),
                quantity NUMERIC,
                status INTEGER REFERENCES analytics.d_request_status(status_key),
                raised_date_key INTEGER,
                updated_date_key INTEGER
            );
        """)

        tgt_conn.commit()
        logging.info("✅ Tables created or already exist.")

def load_dimensions():
    src = PostgresHook(postgres_conn_id="avenue")
    tgt = PostgresHook(postgres_conn_id="superset")

    with src.get_conn() as src_conn, tgt.get_conn() as tgt_conn:
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        logging.info("📦 Loading dimensions...")

        # d_employees
        src_cur.execute("""
            SELECT DISTINCT employee_code AS emp_id, first_name, last_name, email 
            FROM public.employees
        """)
        employees = src_cur.fetchall()
        logging.info(f"👥 Employees fetched: {len(employees)}")
        for emp in employees:
            tgt_cur.execute("""
                INSERT INTO analytics.d_employees (emp_id, first_name, last_name, email)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (emp_id) DO NOTHING
            """, emp)

        # d_item_type
        src_cur.execute("""
            SELECT DISTINCT item_type FROM inventory_management.inventory_requests
            WHERE item_type IS NOT NULL AND TRIM(item_type) != ''
        """)
        item_types = src_cur.fetchall()
        logging.info(f"📦 Item types fetched: {len(item_types)}")
        for (item_type,) in item_types:
            tgt_cur.execute("""
                INSERT INTO analytics.d_item_type (item_type)
                VALUES (%s)
                ON CONFLICT (item_type) DO NOTHING
            """, (item_type,))

        # d_request_status
        src_cur.execute("""
            SELECT DISTINCT status FROM inventory_management.inventory_requests
            WHERE status IS NOT NULL AND TRIM(status) != ''
        """)
        statuses = src_cur.fetchall()
        logging.info(f"📄 Status values fetched: {len(statuses)}")
        for (status,) in statuses:
            tgt_cur.execute("""
                INSERT INTO analytics.d_request_status (status_name)
                VALUES (%s)
                ON CONFLICT (status_name) DO NOTHING
            """, (status,))

        # Cleanup
        tgt_cur.execute("DELETE FROM analytics.d_item_type WHERE item_type IS NULL OR TRIM(item_type) = ''")
        tgt_cur.execute("DELETE FROM analytics.d_request_status WHERE status_name IS NULL OR TRIM(status_name) = ''")
        tgt_cur.execute("DELETE FROM analytics.d_item WHERE item_name IS NULL OR TRIM(item_name) = ''")
        tgt_cur.execute("DELETE FROM analytics.d_project WHERE project_name IS NULL OR TRIM(project_name) = ''")
        tgt_cur.execute("DELETE FROM analytics.d_property WHERE property_name IS NULL OR TRIM(property_name) = ''")
        tgt_conn.commit()
        logging.info("✅ Dimension tables loaded and cleaned.")

def load_fact_inventory_requests():
    src = PostgresHook(postgres_conn_id="avenue")
    tgt = PostgresHook(postgres_conn_id="superset")

    with src.get_conn() as src_conn, tgt.get_conn() as tgt_conn:
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute("""
            SELECT 
                ir.request_id,
                ir.property_name,
                ir.project_name,
                ir.item_name,
                ir.item_type,
                ir.requested_quantity AS quantity,
                ir.status,
                TO_CHAR(ir.created_at, 'YYYYMMDD')::int AS raised_date_key,
                TO_CHAR(ir.updated_at, 'YYYYMMDD')::int AS updated_date_key
            FROM inventory_management.inventory_requests ir
        """)

        rows = src_cur.fetchall()
        logging.info(f"📊 Total inventory requests fetched: {len(rows)}")
        inserted, skipped = 0, 0

        for row in rows:
            (request_id, property_name, project_name, item_name, item_type, quantity,
             status, raised_date_key, updated_date_key) = row

            if not all([request_id, property_name, project_name, item_name, item_type, status]):
                skipped += 1
                logging.info(f"⛔ Skipping request {request_id} due to missing critical fields.")
                continue

            # Resolve FKs
            tgt_cur.execute("SELECT status_key FROM analytics.d_request_status WHERE status_name = %s", (status,))
            status_row = tgt_cur.fetchone()
            if not status_row:
                logging.info(f"⚠️ Skipping {request_id} — status '{status}' not found.")
                skipped += 1
                continue
            status_key = status_row[0]

            tgt_cur.execute("SELECT item_type_key FROM analytics.d_item_type WHERE item_type = %s", (item_type,))
            item_type_row = tgt_cur.fetchone()
            if not item_type_row:
                logging.info(f"⚠️ Skipping {request_id} — item_type '{item_type}' not found.")
                skipped += 1
                continue
            item_type_key = item_type_row[0]

            tgt_cur.execute("SELECT project_key FROM analytics.d_project WHERE project_name = %s", (project_name,))
            project_row = tgt_cur.fetchone()
            if not project_row:
                logging.info(f"⚠️ Skipping {request_id} — project_name '{project_name}' not found.")
                skipped += 1
                continue
            project_key = project_row[0]

            tgt_cur.execute("SELECT property_key FROM analytics.d_property WHERE property_name = %s", (property_name,))
            property_row = tgt_cur.fetchone()
            if not property_row:
                logging.info(f"⚠️ Skipping {request_id} — property '{property_name}' not found.")
                skipped += 1
                continue
            property_key = property_row[0]

            tgt_cur.execute("SELECT item_key FROM analytics.d_item WHERE item_name = %s", (item_name,))
            item_row = tgt_cur.fetchone()
            if not item_row:
                logging.info(f"⚠️ Skipping {request_id} — item_name '{item_name}' not found.")
                skipped += 1
                continue
            item_key = item_row[0]

            tgt_cur.execute("""
                INSERT INTO analytics.fact_inventory_requests (
                    request_id, property_key, project_key,
                    item_key, item_type_key, quantity, status,
                    raised_date_key, updated_date_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id) DO NOTHING
            """, (
                request_id, property_key, project_key, item_key,
                item_type_key, quantity, status_key, raised_date_key, updated_date_key
            ))
            inserted += 1

        tgt_conn.commit()
        logging.info(f"✅ Inserted: {inserted}, ❌ Skipped: {skipped}")

def create_dag():
    with DAG(
        dag_id="inventory_requests_etl_debugged_3",
        default_args={
            'owner': 'airflow',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2024, 1, 1),
        },
        schedule_interval="@daily",
        catchup=False,
        tags=["inventory", "etl", "debug"]
    ) as dag:

        create_tables_task = PythonOperator(
            task_id="create_tables",
            python_callable=create_tables
        )

        load_dimensions_task = PythonOperator(
            task_id="load_dimensions",
            python_callable=load_dimensions
        )

        load_fact_task = PythonOperator(
            task_id="load_fact_inventory_requests",
            python_callable=load_fact_inventory_requests
        )

        create_tables_task >> load_dimensions_task >> load_fact_task

    return dag

globals()["inventory_requests_etl_debugged"] = create_dag()