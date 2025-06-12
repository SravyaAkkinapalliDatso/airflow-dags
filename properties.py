from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import os

# Load environment variables if needed
load_dotenv(dotenv_path="/opt/airflow/.env")

def create_property_schedule_tables():
    hook = PostgresHook(postgres_conn_id='dashboards')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS dashboards;

                CREATE TABLE IF NOT EXISTS dashboards.d_project (
                    project_key SERIAL PRIMARY KEY,
                    project_id VARCHAR(100) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS dashboards.d_property (
                    property_key SERIAL PRIMARY KEY,
                    property_id VARCHAR(100) UNIQUE,
                    project_id VARCHAR(100),
                    budget NUMERIC,
                    usedbudget NUMERIC
                );

                CREATE TABLE IF NOT EXISTS dashboards.d_phase (
                    phase_key SERIAL PRIMARY KEY,
                    phase_name VARCHAR(255) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS dashboards.d_status (
                    status_key SERIAL PRIMARY KEY,
                    status_name VARCHAR(100) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS dashboards.f_property_schedule (
                    schedule_key SERIAL PRIMARY KEY,
                    property_key INT REFERENCES dashboards.d_property(property_key),
                    phase_key INT REFERENCES dashboards.d_phase(phase_key),
                    status_key INT REFERENCES dashboards.d_status(status_key),
                    start_date_key INT,
                    end_date_key INT,
                    percentage_complete NUMERIC,
                    remarks TEXT
                );
            """)
            conn.commit()

def date_to_key(dt):
    return int(dt.strftime("%Y%m%d")) if dt else None

def load_property_schedule():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='dashboards')

    data = src.get_records("""
        SELECT 
            ps.propertyid,
            p.projectid,
            p.budget,
            p.usedbudget,
            s.phasename,
            s.startdate,
            s.enddate,
            s.status,
            s.remarks,
            s.percentage
        FROM master.property_schedule ps
        JOIN master.schedule s ON ps.propertyscheduleid = s.propertyscheduleid
        JOIN master.properties p ON ps.propertyid = p.propertyid
        WHERE ps.is_deleted = false
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                property_id, project_id, budget, usedbudget, phase_name, start_date, end_date, status, remarks, percentage = row

                start_date_key = date_to_key(start_date)
                end_date_key = date_to_key(end_date)

                # Project
                cur.execute("""
                    INSERT INTO dashboards.d_project (project_id)
                    VALUES (%s)
                    ON CONFLICT (project_id) DO NOTHING
                """, (project_id,))
                cur.execute("SELECT project_key FROM dashboards.d_project WHERE project_id = %s", (project_id,))
                project_key = cur.fetchone()[0]

                # Property
                cur.execute("""
                    INSERT INTO dashboards.d_property (property_id, project_id, budget, usedbudget)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (property_id) DO NOTHING
                """, (property_id, project_id, budget, usedbudget))
                cur.execute("SELECT property_key FROM dashboards.d_property WHERE property_id = %s", (property_id,))
                property_key = cur.fetchone()[0]

                # Phase
                cur.execute("""
                    INSERT INTO dashboards.d_phase (phase_name)
                    VALUES (%s)
                    ON CONFLICT (phase_name) DO NOTHING
                """, (phase_name,))
                cur.execute("SELECT phase_key FROM dashboards.d_phase WHERE phase_name = %s", (phase_name,))
                phase_key = cur.fetchone()[0]

                # Status
                cur.execute("""
                    INSERT INTO dashboards.d_status (status_name)
                    VALUES (%s)
                    ON CONFLICT (status_name) DO NOTHING
                """, (status,))
                cur.execute("SELECT status_key FROM dashboards.d_status WHERE status_name = %s", (status,))
                status_key = cur.fetchone()[0]

                # Fact table
                cur.execute("""
                    INSERT INTO dashboards.f_property_schedule (
                        property_key, phase_key, status_key,
                        start_date_key, end_date_key,
                        percentage_complete, remarks
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    property_key, phase_key, status_key,
                    start_date_key, end_date_key,
                    percentage, remarks
                ))
            conn.commit()

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
    dag_id='property_schedule_etl',  # changed!
    default_args=default_args,
    description='ETL for property schedule data into dashboards schema',
    schedule_interval='@daily',
    catchup=False,
)

create_tables_task = PythonOperator(
    task_id='create_dashboards_property_schedule_tables',
    python_callable=create_property_schedule_tables,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_property_schedule_data',
    python_callable=load_property_schedule,
    dag=dag,
)

create_tables_task >> load_data_task
