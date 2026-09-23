# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — Sellers
# MAGIC
# MAGIC This notebook transforms `ecommerce.bronze.sellers` into
# MAGIC `ecommerce.silver.sellers`.
# MAGIC
# MAGIC ## Silver objectives
# MAGIC
# MAGIC - Standardize seller identifier and descriptive fields
# MAGIC - Validate seller state codes
# MAGIC - Validate seller ZIP-code prefix structure
# MAGIC - Preserve source records rather than silently dropping anomalies
# MAGIC - Add explicit data-quality flags
# MAGIC - Validate the expected seller grain
# MAGIC - Persist the curated table as Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and shared configuration
# MAGIC
# MAGIC `BRAZIL_STATE_CODES` is maintained in the shared Workspace Python file so
# MAGIC the same validation rule can be reused by customers and sellers.

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    count,
    countDistinct,
    sum,
    when
)

from shared.constants import BRAZIL_STATE_CODES

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Bronze sellers

# COMMAND ----------

sellers_df = spark.table("ecommerce.bronze.sellers")

print("Bronze row count:", sellers_df.count())

sellers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate seller grain
# MAGIC
# MAGIC The expected grain is:
# MAGIC
# MAGIC **1 row = 1 seller**
# MAGIC
# MAGIC Therefore `seller_id` should be unique.

# COMMAND ----------

seller_key_validation = sellers_df.select(
    count("*").alias("total_rows"),
    countDistinct("seller_id").alias("distinct_seller_id")
)

seller_key_validation.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for duplicate seller IDs

# COMMAND ----------

duplicate_sellers = (
    sellers_df
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_sellers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Profile missing values
# MAGIC
# MAGIC Before transformation, check whether any seller attributes are null.

# COMMAND ----------

sellers_df.select(
    sum(when(col("seller_id").isNull(), 1).otherwise(0)).alias("seller_id_nulls"),
    sum(when(col("seller_zip_code_prefix").isNull(), 1).otherwise(0)).alias("zip_code_nulls"),
    sum(when(col("seller_city").isNull(), 1).otherwise(0)).alias("city_nulls"),
    sum(when(col("seller_state").isNull(), 1).otherwise(0)).alias("state_nulls")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Profile seller states
# MAGIC
# MAGIC We validate against all 27 valid Brazilian state/DF codes.
# MAGIC
# MAGIC The fact that only some states appear in the current dataset is not a
# MAGIC data-quality problem. The validation rule defines what is valid, not what
# MAGIC must necessarily appear.

# COMMAND ----------

sellers_df.select(
    "seller_state"
).distinct().orderBy(
    "seller_state"
).show(100, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Standardize seller fields
# MAGIC
# MAGIC Trim string fields and preserve the original numeric ZIP prefix.

# COMMAND ----------

sellers_silver = sellers_df.select(
    trim("seller_id").alias("seller_id"),
    col("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
    trim("seller_city").alias("seller_city"),
    trim("seller_state").alias("seller_state")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate seller state
# MAGIC
# MAGIC A non-null seller state must be one of the valid Brazilian state/DF codes.

# COMMAND ----------

sellers_silver = sellers_silver.withColumn(
    "invalid_seller_state",
    when(
        col("seller_state").isNotNull() &
        ~col("seller_state").isin(BRAZIL_STATE_CODES),
        True
    ).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate state results

# COMMAND ----------

sellers_silver.groupBy(
    "invalid_seller_state"
).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect invalid states, if any

# COMMAND ----------

sellers_silver.filter(
    col("invalid_seller_state")
).select(
    "seller_id",
    "seller_state"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate seller ZIP-code prefix
# MAGIC
# MAGIC The Olist ZIP field is a prefix rather than a complete postal code.
# MAGIC
# MAGIC We therefore validate its structural range only:
# MAGIC
# MAGIC **1000 through 99999**
# MAGIC
# MAGIC We do not attempt to validate whether a ZIP prefix actually belongs to
# MAGIC the stated city/state because that would require an external geographic
# MAGIC reference dataset.

# COMMAND ----------

sellers_df.select(
    count("*").alias("total_rows"),
    sum(
        when(col("seller_zip_code_prefix") < 1000, 1)
        .otherwise(0)
    ).alias("zip_below_1000"),
    sum(
        when(col("seller_zip_code_prefix") > 99999, 1)
        .otherwise(0)
    ).alias("zip_above_99999")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Add ZIP-code quality flag
# MAGIC
# MAGIC Preserve the seller record and flag structurally invalid ZIP prefixes
# MAGIC rather than dropping them.

# COMMAND ----------

sellers_silver = sellers_silver.withColumn(
    "invalid_seller_zip_code",
    when(
        col("seller_zip_code_prefix").isNotNull() &
        (
            (col("seller_zip_code_prefix") < 1000) |
            (col("seller_zip_code_prefix") > 99999)
        ),
        True
    ).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Check for blank string values
# MAGIC
# MAGIC Null checks alone do not catch empty strings, so check the standardized
# MAGIC string fields explicitly.

# COMMAND ----------

sellers_silver.select(
    sum(
        when(
            col("seller_id").isNull() |
            (col("seller_id") == ""),
            1
        ).otherwise(0)
    ).alias("missing_seller_id"),

    sum(
        when(
            col("seller_city").isNull() |
            (col("seller_city") == ""),
            1
        ).otherwise(0)
    ).alias("missing_seller_city"),

    sum(
        when(
            col("seller_state").isNull() |
            (col("seller_state") == ""),
            1
        ).otherwise(0)
    ).alias("missing_seller_state")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Add missing-value quality flags
# MAGIC
# MAGIC These flags make the Silver contract explicit and protect the pipeline
# MAGIC if future source files contain missing values.

# COMMAND ----------

sellers_silver = (
    sellers_silver
    .withColumn(
        "missing_seller_id",
        col("seller_id").isNull() | (col("seller_id") == "")
    )
    .withColumn(
        "missing_seller_zip_code",
        col("seller_zip_code_prefix").isNull()
    )
    .withColumn(
        "missing_seller_city",
        col("seller_city").isNull() | (col("seller_city") == "")
    )
    .withColumn(
        "missing_seller_state",
        col("seller_state").isNull() | (col("seller_state") == "")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final quality validation
# MAGIC
# MAGIC Check all seller-level quality flags together.

# COMMAND ----------

seller_quality_validation = sellers_silver.select(
    sum(
        when(col("missing_seller_id"), 1).otherwise(0)
    ).alias("missing_seller_id"),

    sum(
        when(col("missing_seller_zip_code"), 1).otherwise(0)
    ).alias("missing_seller_zip_code"),

    sum(
        when(col("missing_seller_city"), 1).otherwise(0)
    ).alias("missing_seller_city"),

    sum(
        when(col("missing_seller_state"), 1).otherwise(0)
    ).alias("missing_seller_state"),

    sum(
        when(col("invalid_seller_state"), 1).otherwise(0)
    ).alias("invalid_seller_state"),

    sum(
        when(col("invalid_seller_zip_code"), 1).otherwise(0)
    ).alias("invalid_seller_zip_code")
)

seller_quality_validation.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Validate seller grain after transformation
# MAGIC
# MAGIC The Silver table should still contain exactly one row per `seller_id`.

# COMMAND ----------

seller_grain_validation = sellers_silver.select(
    count("*").alias("total_rows"),
    countDistinct("seller_id").alias("distinct_seller_id")
)

seller_grain_validation.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for duplicate seller IDs

# COMMAND ----------

duplicate_sellers_silver = (
    sellers_silver
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_sellers_silver.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Review Silver schema

# COMMAND ----------

sellers_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Write Silver table
# MAGIC
# MAGIC The curated seller entity is persisted as a Delta table.

# COMMAND ----------

(
    sellers_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.silver.sellers")
)

print("Silver sellers table written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Validate persisted Silver table
# MAGIC
# MAGIC Reload the table after writing to make sure the persisted result has the
# MAGIC expected row count and grain.

# COMMAND ----------

sellers_silver_persisted = spark.table(
    "ecommerce.silver.sellers"
)

sellers_silver_persisted.select(
    count("*").alias("total_rows"),
    countDistinct("seller_id").alias("distinct_seller_id")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Validate persisted quality flags

# COMMAND ----------

sellers_silver_persisted.select(
    sum(
        when(col("missing_seller_id"), 1).otherwise(0)
    ).alias("missing_seller_id"),

    sum(
        when(col("missing_seller_zip_code"), 1).otherwise(0)
    ).alias("missing_seller_zip_code"),

    sum(
        when(col("missing_seller_city"), 1).otherwise(0)
    ).alias("missing_seller_city"),

    sum(
        when(col("missing_seller_state"), 1).otherwise(0)
    ).alias("missing_seller_state"),

    sum(
        when(col("invalid_seller_state"), 1).otherwise(0)
    ).alias("invalid_seller_state"),

    sum(
        when(col("invalid_seller_zip_code"), 1).otherwise(0)
    ).alias("invalid_seller_zip_code")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Sample final Silver data

# COMMAND ----------

display(
    sellers_silver_persisted.limit(20)
)
