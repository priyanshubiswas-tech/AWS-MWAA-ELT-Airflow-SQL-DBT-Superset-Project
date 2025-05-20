# 📊 AWS MWAA ETL Pipeline with dbt  

**Automated Data Pipeline for Ticket Analytics**  
*Extract ticket data from S3 → Load to PostgreSQL → Transform with dbt → Schedule with Airflow*  

---

## 🌟 Key Features  
✅ **End-to-End Automation** - From raw CSV to analytics-ready tables  
✅ **Modern Data Stack** - Airflow + dbt + PostgreSQL on AWS  
✅ **Production-Ready** - Error handling, retries, and validation  
✅ **Scalable Architecture** - Built on AWS MWAA (Managed Workflows for Apache Airflow)  

---

## 🏗️ Architecture  
```mermaid  
graph TD  
    A[S3 Bucket] -->|CSV File| B(Airflow DAG)  
    B --> C[(PostgreSQL RDS)]  
    C --> D[dbt Transformations]  
    D --> E[Analytics Dashboards]  
```  

---

## 📂 Project Structure  
```bash
s3://your-bucket-name/
├── dags/                          # Airflow DAGs
│   ├── requirements.txt           # Python dependencies
│   ├── etl_pipeline.py            # Main DAG file
│   └── dbt/
│       └── demo/                  # dbt project
│           ├── dbt_project.yml    # dbt config
│           ├── profiles.yml       # DB credentials
│           ├── schema.yml         # Documentation
│           └── models/
│               ├── clean_incidents.sql
│               ├── avg_resolution_time.sql
│               └── ...
└── ticket_dump.csv                # Source data
```

---

## 🛠️ How It Works  

### 1️⃣ Extract: Get Data from S3  
```python
def download_from_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    file_obj = s3_hook.get_key(bucket_name="my-bucket", key="ticket_dump.csv")
    file_obj.download_fileobj('/tmp/data.csv')
```
- Uses Airflow's `S3Hook` to fetch CSV  
- Saves to local `/tmp/` for processing  

### 2️⃣ Load: Import to PostgreSQL  
```python
def load_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_rds')
    with open('/tmp/data.csv', 'r') as f:
        pg_hook.copy_expert("COPY tickets FROM STDIN WITH CSV", f)
```
- Bulk loads data using PostgreSQL's `COPY` command  
- Handles large datasets efficiently  

### 3️⃣ Transform: dbt Magic  
```bash
# Runs these dbt models:
models/
├── clean_incidents.sql          # Standardize data
├── avg_resolution_time.sql      # KPI calculation
└── monthly_summary.sql          # Aggregations
```
- Cleans raw data  
- Calculates business metrics  
- Creates analytics-ready tables  

### 4️⃣ Orchestrate: Airflow DAG  
```python
with DAG(dag_id='ticket_analytics', schedule_interval='@daily') as dag:
    extract = PythonOperator(task_id='extract', python_callable=download_from_s3)
    load = PythonOperator(task_id='load', python_callable=load_to_postgres)
    transform = BashOperator(task_id='transform', bash_command="dbt run")
    
    extract >> load >> transform
```
- Runs daily at midnight  
- Automatic retries on failure  
- Full dependency management  

---

## 🚀 Deployment  

### AWS MWAA Setup  
1. Upload to S3:  
   ```bash
   aws s3 sync . s3://your-bucket-name/dags/
   ```
2. Ensure `requirements.txt` has:  
   ```text
   dbt-core==1.5.0
   dbt-postgres==1.5.0
   apache-airflow-providers-amazon==8.7.0
   ```
3. Reboot MWAA environment after upload  

---

## 💡 Why This Matters  

### Business Impact  
📈 **Data-Driven Decisions** - Clean, reliable metrics for ticket management  
⏱️ **Time Savings** - Eliminates manual CSV processing  
🔍 **Data Quality** - dbt tests catch anomalies  

### Technical Value  
🐍 **Pythonic** - Clean, maintainable code  
🔗 **Modular** - Easy to add new data sources  
☁️ **Cloud-Native** - Fully managed AWS services  

---

## 🛠 Troubleshooting  

| Error | Solution |
|-------|----------|
| `No module named dbt` | Check `requirements.txt` is in `/dags/` |
| `S3 file not found` | Verify CSV is in bucket root (not `/dags/`) |
| `PostgreSQL connection failed` | Check `profiles.yml` credentials |

---


