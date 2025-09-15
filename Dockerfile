FROM apache/airflow:3.0.6

# Cài provider Postgres
RUN pip install --no-cache-dir apache-airflow-providers-postgres

# (tuỳ chọn) thêm thư viện xử lý file parquet
RUN pip install --no-cache-dir pandas pyarrow fastparquet

RUN pip install duckdb

RUN pip install astronomer-cosmos
