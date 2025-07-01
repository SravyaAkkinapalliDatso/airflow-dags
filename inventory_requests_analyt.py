from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

TARGET_SCHEMA = "analytics_core"

def upsert_inventory_requests():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='analytics_postgres')

    data = src.get_records("""
        SELECT 
            request_id, item_name, item_type, project_name, property_name,
            warehouse, engineer_id, status, requested_quantity,
            deli_date, updated_at
        FROM inventory_management.inventory_requests
        WHERE request_id IS NOT NULL AND item_name IS NOT NULL
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                (
                    request_id, item_name, item_type, project_name, property_name,
                    warehouse, engineer_id, status, requested_quantity,
                    deli_date, updated_at
                ) = row

                # Item dimension
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_item (item_name, item_type)
                    VALUES (%s, %s)
                    ON CONFLICT (item_name) DO UPDATE SET item_type = EXCLUDED.item_type
                    RETURNING item_key
                """, (item_name, item_type))
                item_key = cur.fetchone()[0]

                # Project dimension
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_project (project_name)
                    VALUES (%s)
                    ON CONFLICT (project_name) DO NOTHING
                    RETURNING project_key
                """, (project_name,))
                if cur.rowcount:
                    project_key = cur.fetchone()[0]
                else:
                    cur.execute(f"SELECT project_key FROM {TARGET_SCHEMA}.dim_project WHERE project_name = %s", (project_name,))
                    project_key = cur.fetchone()[0]

                # Property dimension
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_property (property_name, project_name)
                    VALUES (%s, %s)
                    ON CONFLICT (property_name) DO UPDATE SET project_name = EXCLUDED.project_name
                    RETURNING property_key
                """, (property_name, project_name))
                property_key = cur.fetchone()[0]

                # Warehouse dimension
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_warehouse (warehouse_name)
                    VALUES (%s)
                    ON CONFLICT (warehouse_name) DO NOTHING
                    RETURNING warehouse_key
                """, (warehouse,))
                if cur.rowcount:
                    warehouse_key = cur.fetchone()[0]
                else:
                    cur.execute(f"SELECT warehouse_key FROM {TARGET_SCHEMA}.dim_warehouse WHERE warehouse_name = %s", (warehouse,))
                    warehouse_key = cur.fetchone()[0]

                # Engineer dimension
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_engineer (engineer_id)
                    VALUES (%s)
                    ON CONFLICT (engineer_id) DO NOTHING
                    RETURNING engineer_key
                """, (engineer_id,))
                if cur.rowcount:
                    engineer_key = cur.fetchone()[0]
                else:
                    cur.execute(f"SELECT engineer_key FROM {TARGET_SCHEMA}.dim_engineer WHERE engineer_id = %s", (engineer_id,))
                    engineer_key = cur.fetchone()[0]

                # Status dimension
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_status (status_name)
                    VALUES (%s)
                    ON CONFLICT (status_name) DO NOTHING
                    RETURNING status_key
                """, (status,))
                if cur.rowcount:
                    status_key = cur.fetchone()[0]
                else:
                    cur.execute(f"SELECT status_key FROM {TARGET_SCHEMA}.dim_status WHERE status_name = %s", (status,))
                    status_key = cur.fetchone()[0]

                # Date dimension
                cur.execute(f"SELECT date_key FROM {TARGET_SCHEMA}.dim_date WHERE full_date = %s", (deli_date.date(),))
                delivery_date_key = cur.fetchone()[0]

                # Insert into fact table
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.fact_inventory_requests (
                        request_id, item_key, project_key, property_key, warehouse_key,
                        engineer_key, status_key, requested_quantity, delivery_date_key, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (request_id) DO UPDATE SET
                        item_key = EXCLUDED.item_key,
                        project_key = EXCLUDED.project_key,
                        property_key = EXCLUDED.property_key,
                        warehouse_key = EXCLUDED.warehouse_key,
                        engineer_key = EXCLUDED.engineer_key,
                        status_key = EXCLUDED.status_key,
                        requested_quantity = EXCLUDED.requested_quantity,
                        delivery_date_key = EXCLUDED.delivery_date_key,
                        updated_at = EXCLUDED.updated_at
                """, (
                    request_id, item_key, project_key, property_key, warehouse_key,
                    engineer_key, status_key, requested_quantity, delivery_date_key, updated_at
                ))
            conn.commit()

def create_inventory_requests_dag():
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }

    dag = DAG(
        dag_id='inventory_requests_analyt',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        description='ETL to populate fact_inventory_requests in analytics_core'
    )

    with dag:
        run_etl = PythonOperator(
            task_id='upsert_inventory_requests',
            python_callable=upsert_inventory_requests
        )

    return dag

globals()['inventory_requests_analyt'] = create_inventory_requests_dag()
