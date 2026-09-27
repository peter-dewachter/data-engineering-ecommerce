# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 — Silver Customers
# MAGIC
# MAGIC **Grain:** 1 row = 1 `customer_id`
# MAGIC
# MAGIC **Process:**
# MAGIC 1. Load Bronze
# MAGIC 2. Standardize customer fields
# MAGIC 3. Add future-proof quality rules
# MAGIC 4. Validate quality rules
# MAGIC 5. Validate Silver grain
# MAGIC 6. Write Silver table
# MAGIC 7. Read back and validate persisted table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    sum,
    when
)

customers_df = spark.table("ecommerce.bronze.customers")

print("Bronze row count:", customers_df.count())

customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Standardize customer fields
# MAGIC
# MAGIC Trim string fields while preserving the source values and table grain.

# COMMAND ----------

customers_silver = (
    customers_df
    .select(
        trim("customer_id").alias("customer_id"),
        trim("customer_unique_id").alias("customer_unique_id"),
        col("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
        trim("customer_city").alias("customer_city"),
        trim("customer_state").alias("customer_state")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Required-field quality rules
# MAGIC
# MAGIC These rules are future-proof.
# MAGIC
# MAGIC Today's dataset has no violations, but future bad records will be flagged.

# COMMAND ----------

customers_silver = (
    customers_silver
    .withColumn(
        "missing_customer_id",
        col("customer_id").isNull() |
        (col("customer_id") == "")
    )
    .withColumn(
        "missing_customer_unique_id",
        col("customer_unique_id").isNull() |
        (col("customer_unique_id") == "")
    )
    .withColumn(
        "missing_customer_zip_code",
        col("customer_zip_code_prefix").isNull()
    )
    .withColumn(
        "missing_customer_city",
        col("customer_city").isNull() |
        (col("customer_city") == "")
    )
    .withColumn(
        "missing_customer_state",
        col("customer_state").isNull() |
        (col("customer_state") == "")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Brazilian state codes
# MAGIC
# MAGIC The Olist dataset contains the 26 Brazilian states plus Distrito Federal (`DF`).
# MAGIC
# MAGIC A missing state is handled separately by `missing_customer_state`.
# MAGIC Therefore, `invalid_customer_state` only identifies non-null values outside the allowed domain.

# COMMAND ----------

from shared.constants import BRAZIL_STATE_CODES

customers_silver = customers_silver.withColumn(
    "invalid_customer_state",
    col("customer_state").isNotNull() &
    (~col("customer_state").isin(BRAZIL_STATE_CODES))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate ZIP-code structure
# MAGIC
# MAGIC We are only checking the structural numeric range of the CEP prefix.
# MAGIC
# MAGIC We are **not** validating whether a ZIP actually belongs to the supplied city/state.
# MAGIC That would require a trusted external geographic reference dataset.

# COMMAND ----------

customers_silver = customers_silver.withColumn(
    "invalid_customer_zip_code",
    col("customer_zip_code_prefix").isNotNull() &
    (
        (col("customer_zip_code_prefix") < 1000) |
        (col("customer_zip_code_prefix") > 99999)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Define quality flags

# COMMAND ----------

quality_flags = [
    "missing_customer_id",
    "missing_customer_unique_id",
    "missing_customer_zip_code",
    "missing_customer_city",
    "missing_customer_state",
    "invalid_customer_state",
    "invalid_customer_zip_code"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate all quality rules
# MAGIC
# MAGIC All flags should currently be zero.

# COMMAND ----------

customers_silver.select(
    *[
        sum(
            when(col(flag), 1).otherwise(0)
        ).alias(flag)
        for flag in quality_flags
    ]
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Silver grain
# MAGIC
# MAGIC Expected grain:
# MAGIC
# MAGIC **1 row = 1 `customer_id`**

# COMMAND ----------

duplicate_customer_ids = (
    customers_silver
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_customer_ids.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate row count and distinct customer IDs

# COMMAND ----------

print("Rows:", customers_silver.count())

print(
    "Distinct customer_id:",
    customers_silver
        .select("customer_id")
        .distinct()
        .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Write Silver table

# COMMAND ----------

(
    customers_silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.silver.customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Read back persisted Silver table

# COMMAND ----------

customers_silver_check = spark.table(
    "ecommerce.silver.customers"
)

print(
    "Persisted rows:",
    customers_silver_check.count()
)

customers_silver_check.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validate persisted quality flags

# COMMAND ----------

customers_silver_check.select(
    *[
        sum(
            when(col(flag), 1).otherwise(0)
        ).alias(flag)
        for flag in quality_flags
    ]
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Validate persisted grain

# COMMAND ----------

customers_silver_check \
    .groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Final validation summary

# COMMAND ----------

print("==========================================")
print("Silver Customers Validation")
print("==========================================")

print(
    "Bronze rows:",
    customers_df.count()
)

print(
    "Silver rows:",
    customers_silver_check.count()
)

print(
    "Distinct customer_id:",
    customers_silver_check
        .select("customer_id")
        .distinct()
        .count()
)

print("==========================================")
print("Quality flag counts")
print("==========================================")

customers_silver_check.select(
    *[
        sum(
            when(col(flag), 1).otherwise(0)
        ).alias(flag)
        for flag in quality_flags
    ]
).show()

print("==========================================")
print("Duplicate customer_id check")
print("==========================================")

customers_silver_check \
    .groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Silver Customer Table
# MAGIC
# MAGIC The curated table contains:
# MAGIC
# MAGIC - `customer_id`
# MAGIC - `customer_unique_id`
# MAGIC - `customer_zip_code_prefix`
# MAGIC - `customer_city`
# MAGIC - `customer_state`
# MAGIC - `missing_customer_id`
# MAGIC - `missing_customer_unique_id`
# MAGIC - `missing_customer_zip_code`
# MAGIC - `missing_customer_city`
# MAGIC - `missing_customer_state`
# MAGIC - `invalid_customer_state`
# MAGIC - `invalid_customer_zip_code`
# MAGIC
# MAGIC Geographic validation of ZIP → city → state is intentionally out of scope.