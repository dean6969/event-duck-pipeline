# Event Duck Pipeline

## Introduction*

This project demonstrates a data engineering pipeline that parses raw JSON event data, enriches it with product and campaign dimensions, and transforms it into analysis-ready tables. The pipeline is orchestrated by **Apache Airflow** and uses **DuckDB** as the transformation engine.

The goal is to enable analysis such as:

- Top articles by traffic per domain
- Top clicked products per brand in the last week
- Most impressed product per campaign
- Fill rate (mounts/tagloads) per domain
- Total unique users reached

**Folder Structure**

- dags/: Contains the Airflow DAG (etl_duckdb.py) for orchestrating the pipeline.
- sql/staging/: SQL scripts to parse raw JSON into staging tables (tagloads, impressions, clicks).
- sql/silver/: SQL scripts to build fact and dimension tables.
- sql/gold/: SQL scripts to build analysis-ready marts (traffic, impressions, clicks, fill rate, reach).
- data/raw/: Partitioned raw JSON event files.
- data/dims/: Dimension CSVs (dim_products.csv, dim_campaigns.csv).
- result.ipynb: Jupyter Notebook to validate results from the gold layer.
- setup_pipeline.sh: Script to bootstrap Docker containers and Airflow environment.
- remove_pipeline.sh: Script to clean up resources.

**Getting Started**

**Prerequisites**

- Docker Deskstop & Docker Compose installed
- Python 3.x installed (for Jupyter Notebook)
- Unix-like shell (Linux/macOS or WSL for Windows)

**Setup and Deployment**

1. **Environment Setup**

Run the provided shell script to set up the environment:

./setup_pipeline.sh

This will:

- - Start **Airflow 3.0** inside Docker
    - Initialize a **DuckDB** warehouse file (for transformations)
    - Register and deploy the ETL DAG etl_duckdb into Airflow

Default Airflow credentials:  
Username: airflow  
Password: airflow

1. **Airflow Access**
    - Open the Airflow web UI at: <http://localhost:8080>
    - Locate the DAG **etl_duckdb**
    - Switch it **On**, then click **Trigger DAG** to run the pipeline
    - The pipeline executes the flow: **staging → silver → gold → data mart**
    - Monitor DAG runs via the Graph or Tree view
2. **Data Validation**

After the pipeline runs successfully:

- - Open **result.ipynb**
    - Run the notebook cells to validate the Gold layer queries

**Data Architecture**

The project uses a **Star Schema** with 3 fact tables and 5 dimensions.

- **Fact tables**:
  - fct_event_articles (TagLoads & Mounts)
  - fct_event_impression (Product Impressions)
  - fct_product_click (Product Clicks)
- **Dimension tables**:
  - dim_articles
  - dim_camp
  - dim_date
  - dim_device
  - dim_prod

The **Gold Layer** aggregates data into marts (m_article_traffic, m_product_clicks_weekly, m_campaign_impressions, m_domain_fill_rate, m_user_reach) to directly answer analysis questions.



**Remove Resources**

When finished, remove all resources with:
```bash
./remove_pipeline.sh

This stops and removes Docker containers, networks, and volumes created for the project.

**Known Limitations & Improvements**

- **DuckDB limitation**: Cannot handle concurrent writes, so not suitable for heavy production workloads. Works fine for this demo.
- **Possible improvements**:
  - Incremental loads & idempotent DAG runs
  - Stronger validation (e.g. Great Expectations, dbt tests)
  - Cloud data warehouse migration (Snowflake/BigQuery/Redshift) for scalability
  - Add monitoring and alerting for better observability