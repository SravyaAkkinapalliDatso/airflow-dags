from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

TARGET_SCHEMA = "analytics_core"

def upsert_tickets():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='analytics_postgres')

    data = src.get_records("""
        SELECT 
            im.issue_id, im.project_id, mp.project_name,
            im.property_id, pr.name AS property_name,
            im.reported_by_email, im.issue_type, im.severity, 
            im.priority, im.status, im.updated_at::DATE AS date
        FROM tickets.issue_master im
        LEFT JOIN master.projects mp ON im.project_id = mp.project_id
        LEFT JOIN master.properties pr ON im.property_id = pr.propertyid
        WHERE im.issue_id IS NOT NULL
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                (
                    issue_id, project_id, project_name, property_id, property_name,
                    reported_by_email, issue_type, severity, priority,
                    status, updated_date
                ) = row

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_project (project_id, project_name)
                    VALUES (%s, %s)
                    ON CONFLICT (project_id) DO UPDATE SET project_name = EXCLUDED.project_name
                    RETURNING project_key
                """, (project_id, project_name))
                project_key = cur.fetchone()[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_property (property_id, property_name)
                    VALUES (%s, %s)
                    ON CONFLICT (property_id) DO UPDATE SET property_name = EXCLUDED.property_name
                    RETURNING property_key
                """, (property_id, property_name))
                property_key = cur.fetchone()[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_user (user_email)
                    VALUES (%s)
                    ON CONFLICT (user_email) DO NOTHING
                    RETURNING user_key
                """, (reported_by_email,))
                reporter_key = cur.fetchone()[0] if cur.rowcount else dest.get_first(f"""
                    SELECT user_key FROM {TARGET_SCHEMA}.dim_user WHERE user_email = %s
                """, parameters=(reported_by_email,))[0]

                for dim_table, col_name, value in [
                    ("dim_issue_type", "issue_type", issue_type),
                    ("dim_severity", "severity", severity),
                    ("dim_priority", "priority", priority),
                    ("dim_status", "status_name", status)
                ]:
                    cur.execute(f"""
                        INSERT INTO {TARGET_SCHEMA}.{dim_table} ({col_name})
                        VALUES (%s) ON CONFLICT DO NOTHING
                    """, (value,))

                cur.execute(f"""
                    SELECT issue_type_key FROM {TARGET_SCHEMA}.dim_issue_type WHERE issue_type = %s
                """, (issue_type,))
                issue_type_key = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT severity_key FROM {TARGET_SCHEMA}.dim_severity WHERE severity = %s
                """, (severity,))
                severity_key = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT priority_key FROM {TARGET_SCHEMA}.dim_priority WHERE priority = %s
                """, (priority,))
                priority_key = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT status_key FROM {TARGET_SCHEMA}.dim_status WHERE status_name = %s
                """, (status,))
                status_key = cur.fetchone()[0]

                cur.execute(f"""
                    SELECT date_key FROM {TARGET_SCHEMA}.dim_date WHERE date = %s
                """, (updated_date,))
                updated_date_key = cur.fetchone()[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.fact_tickets (
                        issue_id, project_key, property_key, reporter_key,
                        issue_type_key, severity_key, priority_key, status_key,
                        updated_date_key
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (issue_id) DO UPDATE SET 
                        project_key = EXCLUDED.project_key,
                        property_key = EXCLUDED.property_key,
                        reporter_key = EXCLUDED.reporter_key,
                        issue_type_key = EXCLUDED.issue_type_key,
                        severity_key = EXCLUDED.severity_key,
                        priority_key = EXCLUDED.priority_key,
                        status_key = EXCLUDED.status_key,
                        updated_date_key = EXCLUDED.updated_date_key
                """, (
                    issue_id, project_key, property_key, reporter_key,
                    issue_type_key, severity_key, priority_key, status_key,
                    updated_date_key
                ))
            conn.commit()

def create_tickets_dag():
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }

    dag = DAG(
        dag_id='tickets_analyt',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        description='ETL to populate fact_tickets in analytics_core'
    )

    with dag:
        run_etl = PythonOperator(
            task_id='upsert_tickets',
            python_callable=upsert_tickets
        )

    return dag

globals()['tickets_analyt'] = create_tickets_dag()
