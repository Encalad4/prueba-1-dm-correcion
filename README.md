
# Megaline Data Architecture - Practical Exam

## Context
Megaline is a telecommunications company with:
- 25 million users
- ~10 TB of data

Goal:
> Determine which plan is most profitable and why.

---

# 1. Docker Infrastructure (docker-compose.yml)

```yaml
services:
  postgres:
    image: postgres:15
    container_name: megaline_postgres
    environment:
      POSTGRES_USER: root
      POSTGRES_PASSWORD: root
      POSTGRES_DB: megaline_db
    ports:
      - "5432:5432"
    volumes:
      - postgres_volume_megaline:/var/lib/postgresql/data

  mage_ai:
    image: mageai/mageai:latest
    container_name: megaline_mage
    environment:
      DATABASE_CONNECTION_URL: postgresql://root:root@postgres:5432/megaline_db
    ports:
      - "6789:6789"
    volumes:
      - mage_ai_volume_megaline:/home/src
    depends_on:
      - postgres

volumes:
  postgres_volume_megaline:
  mage_ai_volume_megaline:
````

Archivo: [`./docker-compose.yaml`](./docker-compose.yaml)

---

# 2. Data Loader (Retry + Chunking)

```python
import pandas as pd
import time

mg_urls = [
    mg_users_url,
    mg_plans_url,
    mg_calls_url,
    mg_messages_url,
    mg_internet_url
]

mg_data_tables = []

MAX_RETRIES = 5
CHUNK_SIZE = 10000

for url in mg_urls:
    print(f"Cargando: {url}")

    for attempt in range(MAX_RETRIES):
        try:
            chunk_iterator = pd.read_csv(url, chunksize=CHUNK_SIZE)

            for chunk in chunk_iterator:
                mg_data_tables.append(chunk)

            print(f"Carga exitosa: {url}")
            break

        except Exception as e:
            wait_time = 2 ** attempt
            print(f"Error en intento {attempt + 1}: {e}")
            print(f"Reintentando en {wait_time}s...")

            time.sleep(wait_time)

            if attempt == MAX_RETRIES - 1:
                raise Exception(f"Fallo definitivo cargando {url}")
```

Archivo: [`./data-loader.py`](./data-loader.py)

---

# 3. ERD - Dimensional Model (Gold Layer)

## DBML Definition

```dbml
Table fct_table {
 usage_id integer [primary key]
 plan_id integer [ref: > dim_plan.plan_id]
 user_id integer [ref: > dim_user.user_id]

 date_id integer [ref: > dim_date.date_id]
 service_type string
 usage_amount float
 revenue float 
}

Table dim_date {
  date_id integer [primary key]
  date date
  year integer
  month integer
  day integer
}

Table dim_plan {
  plan_id integer [primary key]
  plan_name string
  usd_monthy_pay float
  minutes_included integer
  messages_included integer
  mb_per_month_included float
  usd_per_gb float
  usd_per_message float
  usd_per_minute float
}

Table dim_user {
  user_id integer [primary key]
  age integer
  city string
  reg_date date
  churn_date date
}
```

Archivo: [`./erd.dbml`](./erd.dbml)

## ERD Diagram

![ERD Diagram](./Diagrama%20ERD.png)

---

# 4. DBT - Fact Table Construction (FROM + JOINS)

```sql
FROM {{ ref('stg_calls') }} c
LEFT JOIN {{ source('megaline', 'users') }} u
    ON c.user_id = u.user_id
LEFT JOIN {{ source('megaline', 'plans') }} p
    ON u.plan = p.plan_name

UNION ALL

SELECT *
FROM {{ ref('stg_messages') }} m
LEFT JOIN {{ source('megaline', 'users') }} u
    ON m.user_id = u.user_id
LEFT JOIN {{ source('megaline', 'plans') }} p
    ON u.plan = p.plan_name

UNION ALL

SELECT *
FROM {{ ref('stg_internet') }} i
LEFT JOIN {{ source('megaline', 'users') }} u
    ON i.user_id = u.user_id
LEFT JOIN {{ source('megaline', 'plans') }} p
    ON u.plan = p.plan_name
```

Archivo: [`./dbt.sql`](./dbt.sql)

---

# 5. PySpark - Average Revenue per Plan (2025)

```python
from pyspark.sql import functions as F

df = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "fact_usage") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

df_plan = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "dim_plan") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

df_date = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "dim_date") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

result = df \
    .join(df_plan, "plan_id") \
    .join(df_date, "date_id") \
    .filter(F.year(F.col("date")) == 2025) \
    .groupBy("plan_name") \
    .agg(F.avg("revenue").alias("avg_revenue"))

result.show()
```

Archivo: [`./pyspark.py`](./pyspark.py)

---

# Extra: SQL - Fact Table + Partitioning

```sql
CREATE TABLE fact_usage (
    usage_id SERIAL PRIMARY KEY,
    user_id INT,
    plan_id INT,
    date_id DATE,
    service_type VARCHAR(20),
    usage_amount FLOAT,
    revenue FLOAT
)
PARTITION BY RANGE (date_id);

CREATE TABLE fact_usage_2025_01 PARTITION OF fact_usage
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE fact_usage_2025_02 PARTITION OF fact_usage
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
```

Archivo: [`./extra.sql`](./extra.sql)



