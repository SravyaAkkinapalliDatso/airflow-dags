from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import calendar
import os
# Load environment variables
load_dotenv(dotenv_path="/opt/airflow/.env")
TARGET_SCHEMA = "analytics"
def create_property_schedule_tables():
    hook = PostgresHook(postgres_conn_id='superset')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};
                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_project (
                    project_key SERIAL PRIMARY KEY,
                    project_id VARCHAR(100) UNIQUE,
                    project_name TEXT
                );
                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_property (
                    property_key SERIAL PRIMARY KEY,
                    property_id VARCHAR(100) UNIQUE,
                    project_id VARCHAR(100),
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
                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_date (
                    date_key INT PRIMARY KEY,
                    date DATE UNIQUE,
                    month INT,
                    month_name VARCHAR(20),
                    quarter INT,
                    year INT
                );
                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.f_property_schedule (
                    schedule_key SERIAL PRIMARY KEY,
                    property_key INT REFERENCES {TARGET_SCHEMA}.d_property(property_key),
                    phase_key INT REFERENCES {TARGET_SCHEMA}.d_phase(phase_key),
                    status_key INT REFERENCES {TARGET_SCHEMA}.d_status(status_key),
                    start_date_key INT REFERENCES {TARGET_SCHEMA}.d_date(date_key),
                    end_date_key INT REFERENCES {TARGET_SCHEMA}.d_date(date_key),
                    percentage_complete NUMERIC,
                    remarks TEXT
                );
            """)
            conn.commit()
def ensure_date_exists(cur, date_obj):
    if not date_obj:
        return None
    date_key = int(date_obj.strftime('%Y%m%d'))
    cur.execute(f"""
        SELECT 1 FROM {TARGET_SCHEMA}.d_date
        WHERE date_key = %s OR date = %s
    """, (date_key, date_obj))
    if not cur.fetchone():
        cur.execute(f"""
            INSERT INTO {TARGET_SCHEMA}.d_date (
                date_key, date, month, month_name, quarter, year
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            date_key,
            date_obj,
            date_obj.month,
            calendar.month_name[date_obj.month],
            (date_obj.month - 1) // 3 + 1,
            date_obj.year
        ))
    return date_key
def load_property_schedule():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='superset')
    data = src.get_records("""
        SELECT
            ps.propertyid,
            p.projectid,
            mp.project_name,
            p.name AS property_name,
            p.budget,
            p.usedbudget,
            s.scheduleid,
            s.phasename,
            s.startdate,
            s.enddate,
            s.status,
            s.remarks,
            s.percentage
        FROM master.property_schedule ps
        JOIN master.schedule s ON ps.propertyscheduleid = s.propertyscheduleid
        JOIN master.properties p ON ps.propertyid = p.propertyid
        JOIN master.projects mp ON p.projectid = mp.project_id
        WHERE ps.is_deleted = false AND s.status = 'In Progress'
        ORDER BY ps.propertyid, s.startdate;
    """)
    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                (property_id, project_id, project_name, property_name, budget, usedbudget,
                 scheduleid, phase_name, start_date, end_date, status, remarks, percentage) = row
                start_date_key = ensure_date_exists(cur, start_date)
                end_date_key = ensure_date_exists(cur, end_date)
                # d_project
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.d_project (project_id, project_name)
                    VALUES (%s, %s)
                    ON CONFLICT (project_id) DO UPDATE SET project_name = EXCLUDED.project_name
                """, (project_id, project_name))
                cur.execute(f"SELECT project_key FROM {TARGET_SCHEMA}.d_project WHERE project_id = %s", (project_id,))
                project_key = cur.fetchone()[0]
                # d_property
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.d_property (
                        property_id, project_id, property_name, budget, usedbudget
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (property_id) DO UPDATE SET
                        property_name = EXCLUDED.property_name,
                        budget = EXCLUDED.budget,
                        usedbudget = EXCLUDED.usedbudget
                """, (property_id, project_id, property_name, budget, usedbudget))
                cur.execute(f"SELECT property_key FROM {TARGET_SCHEMA}.d_property WHERE property_id = %s", (property_id,))
                property_key = cur.fetchone()[0]
                # d_phase
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.d_phase (phase_name)
                    VALUES (%s)
                    ON CONFLICT (phase_name) DO NOTHING
                """, (phase_name,))
                cur.execute(f"SELECT phase_key FROM {TARGET_SCHEMA}.d_phase WHERE phase_name = %s", (phase_name,))
                phase_key = cur.fetchone()[0]
                # d_status
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.d_status (status_name)
                    VALUES (%s)
                    ON CONFLICT (status_name) DO NOTHING
                """, (status,))
                cur.execute(f"SELECT status_key FROM {TARGET_SCHEMA}.d_status WHERE status_name = %s", (status,))
                status_key = cur.fetchone()[0]
                # f_property_schedule
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.f_property_schedule (
                        property_key, phase_key, status_key,
                        start_date_key, end_date_key,
                        percentage_complete, remarks
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {TARGET_SCHEMA}.f_property_schedule
                        WHERE property_key = %s
                          AND phase_key = %s
                          AND status_key = %s
                          AND start_date_key = %s
                          AND end_date_key = %s
                    )
                """, (
                    property_key, phase_key, status_key,
                    start_date_key, end_date_key, percentage, remarks,
                    property_key, phase_key, status_key,
                    start_date_key, end_date_key
                ))
            conn.commit()
# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(
    dag_id='property_schedule_etl_31',
    default_args=default_args,
    description='ETL for property schedule with auto date handling',
    schedule_interval='@daily',
    catchup=False,
)
create_tables = PythonOperator(
    task_id='create_property_schedule_tables',
    python_callable=create_property_schedule_tables,
    dag=dag,
)
load_data = PythonOperator(
    task_id='load_property_schedule_data',
    python_callable=load_property_schedule,
    dag=dag,
)
create_tables >> load_data
