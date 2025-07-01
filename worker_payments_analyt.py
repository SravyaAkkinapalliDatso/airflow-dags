from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

TARGET_SCHEMA = "analytics_core"

def upsert_worker_payments():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='analytics_postgres')

    data = src.get_records("""
        SELECT
            wp.payment_id, wp.worker_id, wp.worker_name, wp.worker_type,
            wp.work_duration_type, wp.total_hours_worked, wp.total_sqft_completed,
            wp.total_days, wp.gross_amount, wp.tds_percent, wp.tds_amount,
            wp.net_amount, wp.payment_mode, wp.status, wp.created_by, wp.created_at
        FROM payments.worker_payments wp
        WHERE wp.payment_id IS NOT NULL
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                (
                    payment_id, worker_id, worker_name, worker_type,
                    wtype, hours, sqft, days, gross, tds_pct, tds_amt,
                    net, paymode, status, created_by, created_at
                ) = row

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_worker (worker_id, worker_name, worker_type)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (worker_id) DO UPDATE SET 
                        worker_name = EXCLUDED.worker_name,
                        worker_type = EXCLUDED.worker_type
                    RETURNING worker_key
                """, (worker_id, worker_name, worker_type))
                worker_key = cur.fetchone()[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_status (status_name)
                    VALUES (%s)
                    ON CONFLICT (status_name) DO NOTHING
                    RETURNING status_key
                """, (status,))
                status_key = cur.fetchone()[0] if cur.rowcount else dest.get_first(f"""
                    SELECT status_key FROM {TARGET_SCHEMA}.dim_status WHERE status_name = %s
                """, parameters=(status,))[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_user (user_email)
                    VALUES (%s)
                    ON CONFLICT (user_email) DO NOTHING
                    RETURNING user_key
                """, (created_by,))
                created_by_key = cur.fetchone()[0] if cur.rowcount else dest.get_first(f"""
                    SELECT user_key FROM {TARGET_SCHEMA}.dim_user WHERE user_email = %s
                """, parameters=(created_by,))[0]

                cur.execute(f"""
                    SELECT date_key FROM {TARGET_SCHEMA}.dim_date WHERE full_date = %s
                """, (created_at.date(),))
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"No date_key found for full_date = {created_at.date()}")
                created_date_key = result[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.fact_worker_payments (
                        payment_id, worker_key, work_duration_type,
                        total_hours_worked, total_sqft_completed, total_days,
                        gross_amount, tds_percent, tds_amount, net_amount,
                        payment_mode, status_key, created_by_key, created_date_key
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (payment_id) DO UPDATE SET
                        worker_key = EXCLUDED.worker_key,
                        work_duration_type = EXCLUDED.work_duration_type,
                        total_hours_worked = EXCLUDED.total_hours_worked,
                        total_sqft_completed = EXCLUDED.total_sqft_completed,
                        total_days = EXCLUDED.total_days,
                        gross_amount = EXCLUDED.gross_amount,
                        tds_percent = EXCLUDED.tds_percent,
                        tds_amount = EXCLUDED.tds_amount,
                        net_amount = EXCLUDED.net_amount,
                        payment_mode = EXCLUDED.payment_mode,
                        status_key = EXCLUDED.status_key,
                        created_by_key = EXCLUDED.created_by_key,
                        created_date_key = EXCLUDED.created_date_key
                """, (
                    payment_id, worker_key, wtype, hours, sqft, days,
                    gross, tds_pct, tds_amt, net, paymode, status_key,
                    created_by_key, created_date_key
                ))
            conn.commit()

def create_worker_payments_dag():
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }

    dag = DAG(
        dag_id='worker_payments_analyt',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        description='ETL to populate fact_worker_payments in analytics_core'
    )

    with dag:
        run_etl = PythonOperator(
            task_id='upsert_worker_payments',
            python_callable=upsert_worker_payments
        )

    return dag

globals()['worker_payments_analyt'] = create_worker_payments_dag()
