from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime
import sys
import duckdb
import glob, os

# import utils
sys.path.append("/opt/airflow/scripts")
from utils import run_sql_file, DB_PATH, init_etl_log, log_status, get_loaded_files

with DAG(
    dag_id="etl_duckdb",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # tránh 2 run cùng lúc giữ lock DB
    tags=["duckdb", "etl"],
) as dag:
    
    # ---------------- VALIDATE FILES ----------------
    @task(task_id="validate_files")
    def validate_files():
        """
        - Tạo bảng log nếu chưa có
        - Lấy danh sách file mới hoặc chưa success
        - Đánh dấu 'pending' cho các file mới/chưa success
        - Trả về list đường dẫn file để staging
        """
        init_etl_log()
        all_files = glob.glob("/opt/airflow/data/raw/*.parquet")
        loaded_files = get_loaded_files()  # chỉ các file status='success'
        new_files = [f for f in all_files if os.path.basename(f) not in loaded_files]

        for fpath in new_files:
            # mark as pending data
            log_status(os.path.basename(fpath), status="pending")

        return new_files

    # ---------------- GATE (SKIP nếu không có file mới) ----------------
    @task.short_circuit(task_id="has_new_files")
    def has_new_files(files: list[str]) -> bool:
        return bool(files)

    # ---------------- STAGING (LOOP TUẦN TỰ) ----------------
    @task_group(group_id="staging")
    def staging_tasks(files):
        @task(task_id="stg_v_context_loop")
        def stg_v_context_loop(files: list[str]):
            if not files:
                print("✅ No new files to load")
                return
            errors = []
            print("✅ Start loading files")
            for fpath in files:
                try:
                    run_sql_file(
                        "/opt/airflow/sql/staging/stg_v_context.sql",
                        data=fpath
                    )
                    print(f"files {fpath} has been loaded")
                    log_status(os.path.basename(fpath), status="success")
                except Exception as e:
                    # log failed nhưng vẫn cố gắng xử lý các file còn lại
                    log_status(os.path.basename(fpath), status="failed")
                    errors.append(f"{os.path.basename(fpath)}: {e}")
            if errors:
                # Fail task sau khi đã thử hết các file
                raise Exception("Staging failed for some files:\n" + "\n".join(errors))

        stg_v_context_loop(files)

    # ---------------- SILVER ----------------
    @task_group(group_id="silver")
    def silver_tasks():
        @task
        def dim_articles():
            run_sql_file("/opt/airflow/sql/silver/dim_articles.sql")

        @task
        def dim_camp():
            run_sql_file(
                "/opt/airflow/sql/silver/dim_camp.sql",
                data="/opt/airflow/data/raw/dim_campaign.csv"
            )

        @task
        def dim_date():
            run_sql_file("/opt/airflow/sql/silver/dim_date.sql")

        @task
        def dim_device():
            run_sql_file("/opt/airflow/sql/silver/dim_device.sql")

        @task
        def dim_prod():
            run_sql_file(
                "/opt/airflow/sql/silver/dim_prod.sql",
                data="/opt/airflow/data/raw/dim_product.csv"
            )

        @task
        def fct_event_articles():
            run_sql_file("/opt/airflow/sql/silver/fct_event_articles.sql")

        @task
        def fct_event_impression():
            run_sql_file("/opt/airflow/sql/silver/fct_event_impression.sql")

        @task
        def fct_product_click():
            run_sql_file("/opt/airflow/sql/silver/fct_product_click.sql")

        dim_articles() >> dim_camp() >> dim_date() >> dim_device() >> dim_prod() >> \
            fct_event_articles() >> fct_event_impression() >> fct_product_click()

    # ---------------- GOLD ----------------
    @task_group(group_id="gold")
    def gold_tasks():
        @task(task_id="m_article_traffic")
        def m_article_traffic():
            run_sql_file("/opt/airflow/sql/gold/m_article_traffic.sql")

        @task(task_id="m_campaign_impressions")
        def m_campaign_impressions():
            run_sql_file("/opt/airflow/sql/gold/m_campaign_impressions.sql")

        @task(task_id="m_domain_fill_rate")
        def m_domain_fill_rate():
            run_sql_file("/opt/airflow/sql/gold/m_domain_fill_rate.sql")

        @task(task_id="m_product_clicks_weekly")
        def m_product_clicks_weekly():
            run_sql_file("/opt/airflow/sql/gold/m_product_clicks_weekly.sql")

        @task(task_id="m_user_reach")
        def m_user_reach():
            run_sql_file("/opt/airflow/sql/gold/m_user_reach.sql")

        m_article_traffic() >> m_campaign_impressions() >> m_domain_fill_rate() >> \
            m_product_clicks_weekly() >> m_user_reach()

    # ---------------- DAG FLOW ----------------
    files = validate_files()
    gate = has_new_files(files)      # If False -> skip stg/silver/gold
    stg = staging_tasks(files)
    silver = silver_tasks()
    gold = gold_tasks()

    gate >> stg >> silver >> gold
