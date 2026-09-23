# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 — Silver Products
# MAGIC
# MAGIC ## Purpose
# MAGIC Transform the Bronze `products` table into a curated Silver product table.
# MAGIC
# MAGIC ### Silver responsibilities
# MAGIC - Standardize column names
# MAGIC - Preserve the source product grain
# MAGIC - Identify missing product attributes
# MAGIC - Identify non-positive product weights
# MAGIC - Preserve suspicious records rather than silently deleting them
# MAGIC - Validate the product business key
# MAGIC - Persist the result as a Delta table

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    sum,
    when,
    count,
    countDistinct
)

products_df = spark.table("ecommerce.bronze.products")

print("Bronze row count:", products_df.count())

products_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate product key
# MAGIC
# MAGIC The expected grain is one row per `product_id`.

# COMMAND ----------

products_df.select(
    count("*").alias("total_rows"),
    countDistinct("product_id").alias("distinct_product_id")
).show()

products_df.groupBy("product_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Profile missing values
# MAGIC
# MAGIC First, determine which product attributes contain missing values.

# COMMAND ----------

products_df.select(
    sum(
        when(col("product_category_name").isNull(), 1)
        .otherwise(0)
    ).alias("category_nulls"),

    sum(
        when(col("product_name_lenght").isNull(), 1)
        .otherwise(0)
    ).alias("name_length_nulls"),

    sum(
        when(col("product_description_lenght").isNull(), 1)
        .otherwise(0)
    ).alias("description_length_nulls"),

    sum(
        when(col("product_photos_qty").isNull(), 1)
        .otherwise(0)
    ).alias("photos_qty_nulls"),

    sum(
        when(col("product_weight_g").isNull(), 1)
        .otherwise(0)
    ).alias("weight_nulls"),

    sum(
        when(col("product_length_cm").isNull(), 1)
        .otherwise(0)
    ).alias("length_nulls"),

    sum(
        when(col("product_height_cm").isNull(), 1)
        .otherwise(0)
    ).alias("height_nulls"),

    sum(
        when(col("product_width_cm").isNull(), 1)
        .otherwise(0)
    ).alias("width_nulls")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect products with missing catalog metadata
# MAGIC
# MAGIC Check whether the missing fields appear to represent a meaningful pattern.

# COMMAND ----------

products_df.filter(
    col("product_category_name").isNull()
).select(
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize the Silver schema
# MAGIC
# MAGIC Correct the source spelling mistakes in the column names without changing the underlying values.

# COMMAND ----------

products_silver = products_df.select(
    trim("product_id").alias("product_id"),
    trim("product_category_name").alias("product_category_name"),

    col("product_name_lenght").alias("product_name_length"),
    col("product_description_lenght").alias(
        "product_description_length"
    ),

    col("product_photos_qty").alias("product_photos_qty"),
    col("product_weight_g").alias("product_weight_g"),
    col("product_length_cm").alias("product_length_cm"),
    col("product_height_cm").alias("product_height_cm"),
    col("product_width_cm").alias("product_width_cm")
)

products_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add missing-value quality flags
# MAGIC
# MAGIC Missing values are preserved. The flags make the data-quality conditions explicit.

# COMMAND ----------

products_silver = (
    products_silver

    # Catalog metadata
    .withColumn(
        "missing_product_category",
        col("product_category_name").isNull() |
        (col("product_category_name") == "")
    )
    .withColumn(
        "missing_product_name_length",
        col("product_name_length").isNull()
    )
    .withColumn(
        "missing_product_description_length",
        col("product_description_length").isNull()
    )
    .withColumn(
        "missing_product_photos_qty",
        col("product_photos_qty").isNull()
    )

    # Physical attributes
    .withColumn(
        "missing_product_weight",
        col("product_weight_g").isNull()
    )
    .withColumn(
        "missing_product_length",
        col("product_length_cm").isNull()
    )
    .withColumn(
        "missing_product_height",
        col("product_height_cm").isNull()
    )
    .withColumn(
        "missing_product_width",
        col("product_width_cm").isNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Profile numeric validity
# MAGIC
# MAGIC Check whether numeric attributes contain impossible negative or zero values.

# COMMAND ----------

products_silver.select(
    sum(
        when(col("product_name_length") < 0, 1)
        .otherwise(0)
    ).alias("negative_name_length"),

    sum(
        when(col("product_description_length") < 0, 1)
        .otherwise(0)
    ).alias("negative_description_length"),

    sum(
        when(col("product_photos_qty") < 0, 1)
        .otherwise(0)
    ).alias("negative_photos_qty"),

    sum(
        when(col("product_weight_g") <= 0, 1)
        .otherwise(0)
    ).alias("non_positive_weight"),

    sum(
        when(col("product_length_cm") <= 0, 1)
        .otherwise(0)
    ).alias("non_positive_length"),

    sum(
        when(col("product_height_cm") <= 0, 1)
        .otherwise(0)
    ).alias("non_positive_height"),

    sum(
        when(col("product_width_cm") <= 0, 1)
        .otherwise(0)
    ).alias("non_positive_width")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inspect non-positive product weights
# MAGIC
# MAGIC The profiling identified four products with zero weight. Inspect them before defining the Silver rule.

# COMMAND ----------

products_silver.filter(
    col("product_weight_g") <= 0
).select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "product_photos_qty"
).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Add weight-quality flag
# MAGIC
# MAGIC Preserve the records but explicitly flag non-positive weights.

# COMMAND ----------

products_silver = products_silver.withColumn(
    "non_positive_weight",
    col("product_weight_g").isNotNull() &
    (col("product_weight_g") <= 0)
)

products_silver.groupBy(
    "non_positive_weight"
).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Inspect numeric distributions
# MAGIC
# MAGIC Check the overall ranges and quartiles before introducing arbitrary extreme-value thresholds.

# COMMAND ----------

products_silver.select(
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).summary(
    "count",
    "min",
    "25%",
    "50%",
    "75%",
    "max"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Final Silver quality validation
# MAGIC
# MAGIC Validate the quality flags discovered during profiling.

# COMMAND ----------

product_quality_flags = [
    "missing_product_category",
    "missing_product_name_length",
    "missing_product_description_length",
    "missing_product_photos_qty",
    "missing_product_weight",
    "missing_product_length",
    "missing_product_height",
    "missing_product_width",
    "non_positive_weight"
]

products_silver.select(
    *[
        sum(
            when(col(flag), 1)
            .otherwise(0)
        ).alias(flag)
        for flag in product_quality_flags
    ]
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate Silver product grain
# MAGIC
# MAGIC The Silver table must retain one row per `product_id`.

# COMMAND ----------

products_silver.select(
    count("*").alias("total_rows"),
    countDistinct("product_id").alias("distinct_product_id")
).show()

products_silver.groupBy("product_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Persist Silver products

# COMMAND ----------

(
    products_silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.silver.products")
)

print("Silver products table written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Validate persisted Silver table

# COMMAND ----------

products_check = spark.table(
    "ecommerce.silver.products"
)

print(
    "Silver row count:",
    products_check.count()
)

products_check.printSchema()

# COMMAND ----------

products_check.select(
    count("*").alias("total_rows"),
    countDistinct("product_id").alias("distinct_product_id")
).show()

products_check.select(
    *[
        sum(
            when(col(flag), 1)
            .otherwise(0)
        ).alias(flag)
        for flag in product_quality_flags
    ]
).show()
