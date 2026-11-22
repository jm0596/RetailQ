from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_q.retail_silver.transactions",
    comment="Retail sales transactions fact table with cleaned, typed fields and data quality checks"
)
@dp.expect_or_drop("valid transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid quantity", "quantity > 0")
@dp.expect("valid selling_price", "selling_price >= 0")
@dp.expect("valid discount_amount", "discount_amount >= 0 AND discount_amount <= selling_price")
@dp.expect("non-null product_id", "product_id IS NOT NULL")
@dp.expect("valid payment_mode", "payment_mode IN ('UPI', 'Card', 'Cash', 'Net Banking')")

def transactions_clean():
    # Read source streaming table
    source_df = spark.readStream.table("retail_q.blob_bronze.transactions")

    # Clean and standardise: trim identifiers, cast numerics, parse Oracle-style
    # timestamp (dd-Mon-yyyy hh.mm.ss AM/PM), derive net and total amounts.
    # _rescued_data is dropped by not selecting it.
    return source_df.select(
        F.trim("transaction_id").alias("transaction_id"),
        F.trim("opportunity_name").alias("opportunity_name"),
        F.trim("product_id").alias("product_id"),
        F.trim("store_id").alias("store_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("selling_price").cast("decimal(10,2)").alias("selling_price"),
        (F.col("quantity") * F.col("selling_price")).cast("decimal(12,2)").alias("gross_amount"),
        F.col("discount_amount").cast("decimal(10,2)").alias("discount_amount"),
        (F.col("selling_price") - F.col("discount_amount")).cast("decimal(12,2)").alias("net_amount"),
        ((F.col("selling_price") - F.col("discount_amount")) * F.col("quantity")).cast("decimal(12,2)").alias("total_amount"),
        F.to_timestamp("transaction_timestamp", "dd-MMM-yyyy hh.mm.ss a").alias("transaction_timestamp"),
        F.trim("payment_mode").alias("payment_mode"),
        F.trim("sales_channel").alias("sales_channel")
    )
