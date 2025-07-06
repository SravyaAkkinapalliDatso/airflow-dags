from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import calendar
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

TARGET_SCHEMA = "analytics"

def insert_date_if_missing_and_get_key(cur, date):
    if not date:
        return None
    date_key = int(date.strftime("%Y%m%d"))
    cur.execute(f"""
        INSERT INTO {TARGET_SCHEMA}.d_date (date_key, date, month, month_name, quarter, year)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING
    """, (
        date_key,
        date,
        date.month,
        calendar.month_name[date.month],
        (date.month - 1) // 3 + 1,
        date.year
    ))
    return date_key

def insert_and_get_project_key(cur, project_id, project_name):
    cur.execute(f"""
        INSERT INTO {TARGET_SCHEMA}.d_project (project_id, project_name)
        VALUES (%s, %s)
        ON CONFLICT (project_id) DO UPDATE SET project_name = EXCLUDED.project_name
    """, (project_id, project_name))
    cur.execute(f"SELECT project_key FROM {TARGET_SCHEMA}.d_project WHERE project_id = %s", (project_id,))
    return cur.fetchone()[0]

def insert_and_get_property_key(cur, property_id, property_name, budget, usedbudget):
    cur.execute(f"""
        INSERT INTO {TARGET_SCHEMA}.d_property (property_id, property_name, budget, usedbudget)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (property_id) DO UPDATE SET 
            property_name = EXCLUDED.property_name,
            budget = EXCLUDED.budget,
            usedbudget = EXCLUDED.usedbudget
    """, (property_id, property_name, budget, usedbudget))
    cur.execute(f"SELECT property_key FROM {TARGET_SCHEMA}.d_property WHERE property_id = %s", (property_id,))
    return cur.fetchone()[0]

def insert_and_get_phase_key(cur, phase_name):
    cur.execute(f"""
        INSERT INTO {TARGET_SCHEMA}.d_phase (phase_name)
        VALUES (%s)
        ON CONFLICT (phase_name) DO NOTHING
    """, (phase_name,))
    cur.execute(f"SELECT phase_key FROM {TARGET_SCHEMA}.d_phase WHERE phase_name = %s", (phase_name,))
    return cur.fetchone()[0]

def insert_and_get_status_key(cur, status_name):
    cur.execute(f"""
        INSERT INTO {TARGET_SCHEMA}.d_status (status_name)
        VALUES (%s)
        ON CONFLICT (status_name) DO NOTHING
    """, (status_name,))
    cur.execute(f"SELECT status_key FROM {TARGET_SCHEMA}.d_status WHERE status_name = %s", (status_name,))
    return cur.fetchone()[0]

def load_property_schedule():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='superset')

    data = src.get_records("""
        SELECT 
            ps.propertyid,
            p.projectid,
            mp.project_name,
            p.name as property_name,
            p.budget,
            p.usedbudget,
            s.scheduleid,
            s.phasename,
            s.startdate,
            s.enddate,
            s.status,
            s.percentage
        FROM master.property_schedule ps
        JOIN master.schedule s ON ps.propertyscheduleid = s.propertyscheduleid
        JOIN master.properties p ON ps.propertyid = p.propertyid
        JOIN master.projects mp ON p.projectid = mp.project_id
        WHERE ps.is_deleted = false
        ORDER BY ps.propertyid, s.startdate;
    """)
    logging.info(f"📦 Retrieved {len(data)} records")

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            upsert_count = 0
            for row in data:
                try:
                    (
                        property_id, project_id, project_name, property_name,
                        budget, usedbudget, scheduleid, phasename,
                        start_date, end_date, status, percentage
                    ) = row

                    start_date_key = insert_date_if_missing_and_get_key(cur, start_date)
                    end_date_key = insert_date_if_missing_and_get_key(cur, end_date)
                    project_key = insert_and_get_project_key(cur, project_id, project_name)
                    property_key = insert_and_get_property_key(cur, property_id, property_name, budget, usedbudget)
                    phase_key = insert_and_get_phase_key(cur, phasename)
                    status_key = insert_and_get_status_key(cur, status)

                    logging.info(f"🧩 Keys: schedule={scheduleid}, project={project_key}, property={property_key}, phase={phase_key}, status={status_key}")

                    cur.execute(f"""
                        INSERT INTO {TARGET_SCHEMA}.f_property_schedule (
                            scheduleid, property_key, project_key, phase_key,
                            status_key, start_date_key, end_date_key, percentage
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (scheduleid) DO UPDATE SET
                            property_key = EXCLUDED.property_key,
                            project_key = EXCLUDED.project_key,
                            phase_key = EXCLUDED.phase_key,
                            status_key = EXCLUDED.status_key,
                            start_date_key = EXCLUDED.start_date_key,
                            end_date_key = EXCLUDED.end_date_key,
                            percentage = EXCLUDED.percentage
                    """, (
                        scheduleid, property_key, project_key, phase_key,
                        status_key, start_date_key, end_date_key, percentage
                    ))
                    upsert_count += 1
                    logging.info(f"✅ Upserted schedule {scheduleid}")
                except Exception as e:
                    logging.error(f"❌ Error processing schedule {row[6]}: {e}")
                    continue
        logging.info(f"✅ Finished loading. Total upserts: {upsert_count}")

def create_tables():
    hook = PostgresHook(postgres_conn_id='superset')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_date (
                    date_key INT PRIMARY KEY,
                    date DATE UNIQUE,
                    month INT,
                    month_name VARCHAR(20),
                    quarter INT,
                    year INT
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_project (
                    project_key SERIAL PRIMARY KEY,
                    project_id TEXT UNIQUE,
                    project_name TEXT
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_property (
                    property_key SERIAL PRIMARY KEY,
                    property_id TEXT UNIQUE,
                    property_name TEXT,
                    budget NUMERIC,
                    usedbudget NUMERIC
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_phase (
                    phase_key SERIAL PRIMARY KEY,
                    phase_name VARCHAR(255) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_status (
                    status_key SERIAL PRIMARY KEY,
                    status_name VARCHAR(100) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.f_property_schedule (
                    scheduleid BIGINT PRIMARY KEY,
                    property_key INT REFERENCES {TARGET_SCHEMA}.d_property(property_key),
                    project_key INT REFERENCES {TARGET_SCHEMA}.d_project(project_key),
                    phase_key INT REFERENCES {TARGET_SCHEMA}.d_phase(phase_key),
                    status_key INT REFERENCES {TARGET_SCHEMA}.d_status(status_key),
                    start_date_key INT REFERENCES {TARGET_SCHEMA}.d_date(date_key),
                    end_date_key INT REFERENCES {TARGET_SCHEMA}.d_date(date_key),
                    percentage NUMERIC
                );
            """)
            logging.info("✅ All analytics schema tables created or already exist.")

with DAG(
    dag_id='project_properties_42',
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'analytics', 'property'],
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_analytics_tables',
        python_callable=create_tables
    )

    load_data_task = PythonOperator(
        task_id='load_property_schedule_to_analytics',
        python_callable=load_property_schedule
    )

    create_tables_task >> load_data_task