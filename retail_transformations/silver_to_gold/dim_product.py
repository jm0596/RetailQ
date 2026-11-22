from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_q.gold.dim_product",
    comment="Product dimension (gold) - one row per active product_id",
    schema="""
        product_id STRING,
        product_name STRING,
        category STRING,
        subcategory STRING,
        brand STRING,
        product_segment STRING,
        unit_price DECIMAL(11,2),
        supplier_name STRING,
        CONSTRAINT dim_product_pk PRIMARY KEY (product_id)
    """,
)

@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def dim_product():

    return (
        spark.readStream.table("retail_q.retail_silver.product_catalog")
        .where(F.col("product_id").isNotNull() & (F.col("is_active") == True))
        .select(
            "product_id",  # primary key (PK constraint declared in schema above)
            "product_name",
            "category",
            "subcategory",
            "brand",
            "product_segment",
            "unit_price",
            "supplier_name"
        )
        .dropDuplicates(["product_id"])   
    )