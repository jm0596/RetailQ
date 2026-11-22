from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.gold.fact_sales",
    comment="Sales fact table (gold) - transactions joined to the gold dimensions",
    schema="""
        transaction_id string,
        date_key int,
        customer_id string,
        product_id string,
        store_id string,
        quantity int,
        selling_price decimal(10,2),
        discount_amount decimal(10,2),
        net_amount decimal(12,2),
        payment_mode string,
        sales_channel string"""
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_date_key", "date_key IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def fact_sales():
    return spark.sql("""
        SELECT
          t.transaction_id,
          dc.date_key,
          c.customer_id,
          t.product_id,
          t.store_id,
          t.quantity,
          t.selling_price,
          t.discount_amount,
          t.net_amount,
          t.payment_mode,
          t.sales_channel
        FROM retail_q.retail_silver.transactions t
        LEFT JOIN retail_q.gold.dim_calendar dc
          ON dc.full_date = CAST(t.transaction_timestamp AS DATE)  -- date_key looked up from transaction_timestamp
        LEFT JOIN retail_q.retail_silver.opportunity o
          ON LOWER(t.opportunity_name) = LOWER(o.name)
        LEFT JOIN retail_q.gold.dim_customer c
          ON o.account_id = c.customer_id
        LEFT JOIN retail_q.gold.dim_product dpp
          ON dpp.product_id = t.product_id
    """)
