
from pyspark import pipelines as dp

@dp.table(
    name="retail_q.gold.dim_calendar",
    schema="""
      date_key   INT PRIMARY KEY,
      full_date  DATE,
      day        INT,
      day_name   STRING,
      week_number  INT,
      month      INT,
      month_name STRING,
      quarter    INT,
      year       INT,
      is_weekend BOOLEAN
    """,
    comment="Calendar dimension (smart key date_key = yyyymmdd) for the retail_q gold layer"
)
def dim_calendar():

    return spark.sql(
        """
        SELECT
          cast(date_format(full_date, 'yyyyMMdd') as int) AS date_key,
          full_date,
          dayofmonth(full_date)                          AS day,
          date_format(full_date, 'EEEE')                 AS day_name,
          weekofyear(full_date)                          AS week_number,
          month(full_date)                               AS month,
          date_format(full_date, 'MMMM')                 AS month_name,
          quarter(full_date)                              AS quarter,
          year(full_date)                                AS year,
          dayofweek(full_date) IN (1, 7)                 AS is_weekend
        FROM (
          SELECT explode(sequence(date '2020-01-01', date_add(current_date(), 1000), interval 1 day)) AS full_date
        )
        """
    )
