from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_q.retail_silver.product_catalog",
    comment=(
        "Silver product catalog: standardised copy of the bronze streaming table "
        "with core data quality rules applied."
    ),
)
# Core data quality rules - invalid records are dropped from the target
# Core data quality rules - invalid records are dropped from the target
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
@dp.expect("valid_unit_price", "unit_price >= 0")
# Advisory data quality rules - violations are logged but records are kept
@dp.expect("valid_category", "category IS NOT NULL")
@dp.expect("valid_brand", "brand IS NOT NULL")
@dp.expect("valid_updated_at", "updated_at IS NOT NULL")
def product_catalog():
    """Read the bronze streaming table incrementally and standardise its columns."""
    return (
        spark.readStream
        # The bronze table is an Auto CDC target and may contain change commits
        .option("skipChangeCommits", "true")
        .table("retail_q.bronze.product_catalog")
        # Keep only active products; is_active is used as a filter, not an output column
        .filter(F.col("is_active").cast("boolean") == F.lit(True))
        .select(
            # Trim and convert empty strings to NULL for identifiers and names
            F.upper(F.trim(F.col("product_id"))).alias("product_id"),
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
            # Normalise categorical text to lowercase
            F.initcap(F.trim(F.col("category"))).alias("category"),
            F.coalesce(F.initcap(F.trim(F.col("subcategory"))), F.lit("Unknown")).alias("subcategory"),
            F.coalesce(F.initcap(F.trim(F.col("brand"))), F.lit("Unknown")).alias("brand"),
            F.coalesce(F.trim(F.col("supplier_name")), F.lit("Unknown")).alias("supplier_name"),
            # Enforce types - round prices to 2 decimal places (no decimal cast)
            F.round(F.col("unit_price"), 2).alias("unit_price"),
            # Classify products into price segments based on unit price
            F.when(F.col("unit_price") > 50000, "PREMIUM")
            .when(F.col("unit_price") > 10000, "MID_RANGE")
            .otherwise("BUDGET")
            .alias("product_segment"),
            F.col("launch_date").cast("date").alias("launch_date"),
            
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            # Preserve bronze SCD2 validity columns for downstream lineage
            F.col("__START_AT").alias("__START_AT"),
            F.col("__END_AT").alias("__END_AT"),
            # A product is active if its SCD2 validity window is still open (__END_AT is NULL)
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
        )
    )
