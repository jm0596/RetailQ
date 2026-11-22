from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="retail_q.gold.dim_customer",
    comment="Customer dimension (gold) - one row per active customer_id",
    schema="""
        customer_id STRING,
        customer_name STRING,
        customer_type STRING,
        city STRING,
        state STRING,
        country STRING,
        industry STRING,
        PRIMARY KEY (customer_id)
    """
)

@dp.expect_or_drop("customer_id IS NOT NULL", "customer_id is not null")
def dim_customer():

    return (
        spark.read.table("retail_q.retail_silver.account")
        .where(F.col("id").isNotNull() & (F.col("is_active") == True))
     .select(
        F.col("id").alias("customer_id"),
        F.col("customer_name").alias("customer_name"),
        F.col("type").alias("customer_type"),
        F.col("billing_city").alias("city"),
        F.col("billing_state").alias("state"),
        F.col("billing_country").alias("country"),
        F.col("industry").alias("industry"),
         )
     .dropDuplicates(["customer_id"]) 
    )

