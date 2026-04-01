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