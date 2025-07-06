from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

TARGET_SCHEMA = "analytics"

def get_engine(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    uri = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(uri)

def create_schema_and_tables():
    engine = get_engine('superset')
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.dim_issue_type (
                issue_type_key SERIAL PRIMARY KEY,
                issue_type TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.dim_severity (
                severity_key SERIAL PRIMARY KEY,
                severity TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.dim_priority (
                priority_key SERIAL PRIMARY KEY,
                priority TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.dim_status (
                status_key SERIAL PRIMARY KEY,
                status TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.dim_reporter (
                reporter_key SERIAL PRIMARY KEY,
                reported_by_email TEXT UNIQUE
            );

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
                property_name TEXT
            );

            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.fact_issues (
                issue_id TEXT PRIMARY KEY,
                project_key INT REFERENCES {TARGET_SCHEMA}.d_project(project_key),
                property_key INT REFERENCES {TARGET_SCHEMA}.d_property(property_key),
                issue_type_key INT REFERENCES {TARGET_SCHEMA}.dim_issue_type(issue_type_key),
                severity_key INT REFERENCES {TARGET_SCHEMA}.dim_severity(severity_key),
                priority_key INT REFERENCES {TARGET_SCHEMA}.dim_priority(priority_key),
                status_key INT REFERENCES {TARGET_SCHEMA}.dim_status(status_key),
                reporter_key INT REFERENCES {TARGET_SCHEMA}.dim_reporter(reporter_key),
                updated_date_key INT REFERENCES {TARGET_SCHEMA}.d_date(date_key)
            );
        """))

def extract_transform_load():
    src_engine = get_engine('avenue')
    tgt_engine = get_engine('superset')

    query = """
        SELECT 
            im.issue_id, im.project_id, p.project_name,
            im.property_id, pr.name as property_name,
            im.reported_by_email, im.issue_type, im.severity, im.priority, im.status, im.updated_at
        FROM tickets.issue_master im
        LEFT JOIN master.projects p ON im.project_id = p.project_id
        LEFT JOIN master.properties pr ON im.property_id = pr.propertyid;
    """

    df = pd.read_sql(query, src_engine)

    with tgt_engine.begin() as conn:
        def get_or_create_key(table, key_col, val_col, val):
            conn.execute(text(f"""
                INSERT INTO {TARGET_SCHEMA}.{table} ({val_col})
                VALUES (:val)
                ON CONFLICT ({val_col}) DO NOTHING
            """), {'val': val})
            result = conn.execute(text(f"""
                SELECT {key_col} FROM {TARGET_SCHEMA}.{table}
                WHERE {val_col} = :val
            """), {'val': val}).fetchone()
            return result[0] if result else None

        for _, row in df.iterrows():
            issue_id = row['issue_id']
            project_id = row['project_id']
            project_name = row['project_name']
            property_id = row['property_id']
            property_name = row['property_name']

            issue_type_key = get_or_create_key("dim_issue_type", "issue_type_key", "issue_type", row['issue_type'])
            severity_key = get_or_create_key("dim_severity", "severity_key", "severity", row['severity'])
            priority_key = get_or_create_key("dim_priority", "priority_key", "priority", row['priority'])
            status_key = get_or_create_key("dim_status", "status_key", "status", row['status'])
            reporter_key = get_or_create_key("dim_reporter", "reporter_key", "reported_by_email", row['reported_by_email'])

            # Insert project and property
            conn.execute(text(f"""
                INSERT INTO {TARGET_SCHEMA}.d_project (project_id, project_name)
                VALUES (:id, :name)
                ON CONFLICT (project_id) DO UPDATE SET project_name = EXCLUDED.project_name
            """), {'id': project_id, 'name': project_name})
            project_key = conn.execute(text(f"""
                SELECT project_key FROM {TARGET_SCHEMA}.d_project WHERE project_id = :id
            """), {'id': project_id}).fetchone()[0]

            conn.execute(text(f"""
                INSERT INTO {TARGET_SCHEMA}.d_property (property_id, property_name)
                VALUES (:id, :name)
                ON CONFLICT (property_id) DO UPDATE SET property_name = EXCLUDED.property_name
            """), {'id': property_id, 'name': property_name})
            property_key = conn.execute(text(f"""
                SELECT property_key FROM {TARGET_SCHEMA}.d_property WHERE property_id = :id
            """), {'id': property_id}).fetchone()[0]

            # d_date insert and lookup
            updated_at = row['updated_at']
            date_only = updated_at.date()
            date_key = int(date_only.strftime("%Y%m%d"))
            month = updated_at.month
            quarter = (month - 1) // 3 + 1
            month_name = updated_at.strftime('%B')
            year = updated_at.year

            conn.execute(text(f"""
                INSERT INTO {TARGET_SCHEMA}.d_date (date_key, date, month, month_name, quarter, year)
                VALUES (:date_key, :date, :month, :month_name, :quarter, :year)
                ON CONFLICT (date) DO NOTHING
            """), {
                'date_key': date_key,
                'date': date_only,
                'month': month,
                'month_name': month_name,
                'quarter': quarter,
                'year': year
            })

            updated_date_key = conn.execute(text(f"""
                SELECT date_key FROM {TARGET_SCHEMA}.d_date WHERE date = :date
            """), {'date': date_only}).fetchone()[0]

            # Final insert into fact_issues
            metadata = MetaData()
            fact_issues_table = Table('fact_issues', metadata, autoload_with=tgt_engine, schema=TARGET_SCHEMA)

            stmt = insert(fact_issues_table).values(
                issue_id=issue_id,
                project_key=project_key,
                property_key=property_key,
                issue_type_key=issue_type_key,
                severity_key=severity_key,
                priority_key=priority_key,
                status_key=status_key,
                reporter_key=reporter_key,
                updated_date_key=updated_date_key
            ).on_conflict_do_update(
                index_elements=['issue_id'],
                set_={
                    'project_key': project_key,
                    'property_key': property_key,
                    'issue_type_key': issue_type_key,
                    'severity_key': severity_key,
                    'priority_key': priority_key,
                    'status_key': status_key,
                    'reporter_key': reporter_key,
                    'updated_date_key': updated_date_key
                }
            )
            conn.execute(stmt)

# DAG definition
default_args = {
    'start_date': datetime(2025, 7, 3),
    'retries': 1
}

dag = DAG(
    dag_id='issues_fact_etl_final_29',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

with dag:
    create_schema = PythonOperator(
        task_id='create_analytics_schema',
        python_callable=create_schema_and_tables
    )

    load_fact = PythonOperator(
        task_id='load_fact_issues_data',
        python_callable=extract_transform_load
    )

    create_schema >> load_fact
