import duckdb
from datetime import datetime

DB_PATH = "/opt/airflow/db/event_prod.db"

def run_sql_file( file_path: str, **kwargs):
    """
    read SQL files, kwargs (ex: data='data/*.parquet'),
    Then execute on Duckdb.
    """

    con = duckdb.connect(DB_PATH)
    with open(file_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    if kwargs:
        sql_text = sql_text.format(**kwargs)


    con.execute(sql_text)
    con.close()

def get_connection():
    return duckdb.connect(DB_PATH)

def init_etl_log():
    """create log if not exists"""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            timestamp TIMESTAMP,
            file_name STRING,
            status STRING,
            type_log STRING
        )
    """)
    conn.close()

def log_status(file_name, status, type_log="check_incremental_load_stg"):
    """Insert/update log """
    conn = get_connection()
    existing = conn.execute(
        "SELECT COUNT(*) FROM etl_log WHERE file_name = ?", [file_name]
    ).fetchone()[0]

    if existing:
        conn.execute("""
            UPDATE etl_log
            SET status = ?, timestamp = ?
            WHERE file_name = ?
        """, [status, datetime.now(), file_name])
    else:
        conn.execute("""
            INSERT INTO etl_log VALUES (?, ?, ?, ?)
        """, [datetime.now(), file_name, status, type_log])

    conn.close()

def get_loaded_files():
    
    conn = get_connection()
    files = {
        row[0] for row in conn.execute(
            "SELECT file_name FROM etl_log WHERE status = 'success'"
        ).fetchall()
    }
    conn.close()
    return files

