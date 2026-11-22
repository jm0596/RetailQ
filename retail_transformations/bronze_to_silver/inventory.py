from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_q.retail_silver.inventory",
    comment=(
        "Silver inventory: standardised copy of the bronze streaming table with the "
        "warehouse location reduced to its first token and stock/reorder levels "
        "categorised into logical bands."
    ),
)
# Core data quality rules - invalid records are dropped from the target
@dp.expect_or_drop("valid_inventory_id", "inventory_id IS NOT NULL AND LENGTH(TRIM(inventory_id)) > 0")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
# Advisory data quality rules - violations are logged but records are kept
@dp.expect("valid_store_id", "store_id IS NOT NULL")
@dp.expect("valid_stock_quantity", "stock_quantity >= 0")
def inventory():
    """Read the bronze streaming table incrementally and standardise its columns."""
    return (
        spark.readStream
        # The bronze table is an Auto CDC target and may contain change commits
        .option("skipChangeCommits", "true")
        .table("retail_q.bronze.inventory")
        .select(
            # Trim and standardise identifiers to uppercase
            F.upper(F.trim(F.col("inventory_id"))).alias("inventory_id"),
            F.upper(F.trim(F.col("product_id"))).alias("product_id"),
            F.upper(F.trim(F.col("store_id"))).alias("store_id"),
            F.col("stock_quantity").cast("int").alias("stock_quantity"),
            F.col("reorder_level").cast("int").alias("reorder_level"),
            F.when(F.col("stock_quantity") < F.col("reorder_level"), F.lit("LOW_STOCK")).otherwise(F.lit("HEALTHY")).alias("inventory_status"),
            # Keep only the first token of the warehouse location
            # (e.g. "Mumbai Central Warehouse" -> "Mumbai")
            F.element_at(F.split(F.trim(F.col("warehouse_location")), "\\s+"), 1).alias("warehouse_location"),
            F.col("last_stock_update").cast("timestamp").alias("last_stock_update"),
            
            # Preserve bronze SCD2 validity columns for downstream lineage
            F.col("__START_AT").alias("__START_AT"),
            F.col("__END_AT").alias("__END_AT"),
            # An inventory record is current if its SCD2 validity window is still open
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
           
            # Capture when the record was processed into the silver table
            F.current_timestamp().alias("processed_at"),
        )
    )
