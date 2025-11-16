# ETL Transaction Data Pipeline (Apache Airflow)

This project contains an automated ETL pipeline built using **Apache Airflow**.  
The pipeline extracts data from CSV and PostgreSQL sources, transforms it, and loads it into the final PostgreSQL database for analytics.

### 📁 Project Structure
```
├── dags/
│   └── etl_transaction_data_dag.py
│
├── scripts/
│   ├── extract_data.py
│   ├── transform_data.py
│   └── load_data.py
│
├── data/
│   ├── input/
│   │   - (input_data.csv)
│   └── output/
│       - (output_data.csv)
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```
### 📝 Dependencies Required
```
apache-airflow==2.7.1
apache-airflow-providers-postgres

pandas
pendulum

psycopg2-binary
SQLAlchemy
```


### 🔄 Pipeline Overview

![alt text](img-pipeline-overview.png)

### **Extract**
- Reads 5 CSV files as raw input.
- Converts 2 CSV tables (`dummy_transaksi_bus`, `dummy_transaksi_halte`) into PostgreSQL tables.
- Produces 3 CSV-based sources and 2 PostgreSQL-based sources.

### **Transform**
- Cleans and preprocesses all extracted datasets.
- Applies data quality rules and transformations.
- Generates unified, analytics-ready dataset.

### **Load**
- Exports the final transformed tables into:

    - PostgreSQL (fact & aggregations tables)

    - CSV report files in ```/data/output/```


### ⏰ Scheduling

The pipeline runs **every day at 07:00 (Asia/Jakarta)**.


```
schedule_interval="0 7 * * *"
```

### 🐳 Running with Docker
#### 1. Clone the Repository
```
git clone https://github.com/widiarsaf/airflow-etl-pipeline-widiareta.git
cd airflow-etl-pipeline-widiareta
```

#### 2. Setup and Start Airflow
- Clean any old resources (optional but recommended)
```
docker compose down --volumes --remove-orphans
```

- Build and Initialize Airflow
```
docker compose build
docker compose run airflow-init
```

- Start Airflow
```
docker-compose up -d
```

#### 3. Access Airflow Web UI 
http://localhost:8080

Login (default):
- **Username:** admin  
- **Password:** admin  

#### 4. Setting Up PostgreSQL Connection in Airflow
To enable the ETL pipeline to communicate with the PostgreSQL database, you need to configure a connection in the Airflow UI. 

- Open Airflow Web UI.

- Navigate to Admin → Connections.

- Search for “postgres_default” connection.

- Fill in the following details:

| Field         | Value              |
| ------------- | ------------------ |
| **Conn Id**   | `postgres_default` |
| **Conn Type** | Postgres           |
| **Host**      | `postgres`         |
| **Schema**    | `airflow`          |
| **Login**     | `airflow`          |
| **Password**  | `airflow`          |
| **Port**      | `5432`             |
| **Extra**     | `(leave completely empty)`    |

- For connection setup, please remove `{}` in Extra column, leave completely empty. This is important step to make sure airflow can connect to postgres database.

    - ✔️ Correct: (leave blank)
    - ❌ Incorrect: {} or any JSON content
![alt text](img-pg-conn.png)

- Click ***Save*** button.

📌 Note:

- The host postgres comes from ```docker-compose.yml```.

- The DAG expects the connection ID ```postgres_default```.



#### 5. Start the Pipeline
![alt text](img-start-etl-pipeline.png)

Trigger the DAG using the Play (►) button in Airflow.
Airflow will run all tasks in sequence—from ```start``` → CSV loading → extraction → transformation → data loading → ```end```.
Each step follows task dependencies automatically.

