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