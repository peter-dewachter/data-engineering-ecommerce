# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — Order Payments
# MAGIC
# MAGIC This notebook transforms `ecommerce.bronze.order_payments` into
# MAGIC `ecommerce.silver.order_payments`.
# MAGIC
# MAGIC ## Grain
# MAGIC One row represents one payment record identified by:
# MAGIC `(order_id, payment_sequential)`
# MAGIC
# MAGIC ## Silver objectives
# MAGIC - Standardize string fields
# MAGIC - Preserve source payment states such as `not_defined`
# MAGIC - Validate payment types
# MAGIC - Validate payment installments
# MAGIC - Validate payment values
# MAGIC - Validate order references
# MAGIC - Preserve source anomalies through explicit quality flags
# MAGIC - Validate the final Silver table before and after persistence
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    count,
    countDistinct,
    sum,
    when
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Bronze payments

# COMMAND ----------

payments_df = spark.table("ecommerce.bronze.order_payments")

print("Bronze row count:", payments_df.count())

payments_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define expected payment types
# MAGIC
# MAGIC `not_defined` is a known source value in the Olist dataset.
# MAGIC It is preserved rather than removed.
# MAGIC

# COMMAND ----------

expected_payment_types = [
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined"
]


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize payment columns
# MAGIC
# MAGIC String identifiers and payment types are trimmed.
# MAGIC Numeric fields retain their source values.
# MAGIC

# COMMAND ----------

payments_silver = payments_df.select(
    trim("order_id").alias("order_id"),
    col("payment_sequential"),
    trim("payment_type").alias("payment_type"),
    col("payment_installments"),
    col("payment_value")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add intrinsic data-quality flags
# MAGIC
# MAGIC These flags describe issues that can be identified from the payment
# MAGIC record itself.
# MAGIC
# MAGIC Zero payment values are not considered invalid because the source
# MAGIC contains legitimate zero-value voucher and `not_defined` records.
# MAGIC
# MAGIC Non-positive installments are flagged because the profiling showed
# MAGIC two credit-card records with `payment_installments = 0`.
# MAGIC

# COMMAND ----------

payments_silver = (
    payments_silver
    .withColumn(
        "missing_order_id",
        col("order_id").isNull() | (col("order_id") == "")
    )
    .withColumn(
        "missing_payment_sequential",
        col("payment_sequential").isNull()
    )
    .withColumn(
        "missing_payment_type",
        col("payment_type").isNull() | (col("payment_type") == "")
    )
    .withColumn(
        "missing_payment_installments",
        col("payment_installments").isNull()
    )
    .withColumn(
        "missing_payment_value",
        col("payment_value").isNull()
    )
    .withColumn(
        "invalid_payment_type",
        col("payment_type").isNotNull()
        & ~col("payment_type").isin(expected_payment_types)
    )
    .withColumn(
        "invalid_payment_installments",
        col("payment_installments").isNotNull()
        & (col("payment_installments") <= 0)
    )
    .withColumn(
        "negative_payment_value",
        col("payment_value").isNotNull()
        & (col("payment_value") < 0)
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate payment → order references
# MAGIC
# MAGIC Every payment should belong to an existing order.
# MAGIC
# MAGIC We use a left anti join to identify orphaned payment records.
# MAGIC

# COMMAND ----------

order_ids = (
    spark.table("ecommerce.silver.orders")
    .select("order_id")
    .dropDuplicates()
)

invalid_payment_orders = (
    payments_silver
    .join(order_ids, on="order_id", how="left_anti")
)

invalid_payment_order_count = invalid_payment_orders.count()

print(
    "Payments with invalid order references:",
    invalid_payment_order_count
)

invalid_payment_orders.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Add referential-integrity flag
# MAGIC

# COMMAND ----------

payments_silver = payments_silver.withColumn(
    "invalid_order_reference",
    ~col("order_id").isin(
        [row["order_id"] for row in order_ids.collect()]
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate quality flags
# MAGIC
# MAGIC This summarizes the number of records affected by each Silver rule.
# MAGIC

# COMMAND ----------

payments_silver.select(
    sum(when(col("missing_order_id"), 1).otherwise(0)).alias(
        "missing_order_id"
    ),
    sum(when(col("missing_payment_sequential"), 1).otherwise(0)).alias(
        "missing_payment_sequential"
    ),
    sum(when(col("missing_payment_type"), 1).otherwise(0)).alias(
        "missing_payment_type"
    ),
    sum(when(col("missing_payment_installments"), 1).otherwise(0)).alias(
        "missing_payment_installments"
    ),
    sum(when(col("missing_payment_value"), 1).otherwise(0)).alias(
        "missing_payment_value"
    ),
    sum(when(col("invalid_payment_type"), 1).otherwise(0)).alias(
        "invalid_payment_type"
    ),
    sum(when(col("invalid_payment_installments"), 1).otherwise(0)).alias(
        "invalid_payment_installments"
    ),
    sum(when(col("negative_payment_value"), 1).otherwise(0)).alias(
        "negative_payment_value"
    ),
    sum(when(col("invalid_order_reference"), 1).otherwise(0)).alias(
        "invalid_order_reference"
    )
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Inspect invalid installment records
# MAGIC
# MAGIC These are the two records discovered during profiling.
# MAGIC

# COMMAND ----------

payments_silver.filter(
    col("invalid_payment_installments")
).select(
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value"
).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate natural key
# MAGIC
# MAGIC The expected grain is one row per:
# MAGIC `(order_id, payment_sequential)`.
# MAGIC

# COMMAND ----------

grain_profile = payments_silver.select(
    count("*").alias("total_rows"),
    countDistinct(
        "order_id",
        "payment_sequential"
    ).alias("distinct_payment_keys")
)

grain_profile.show()


# COMMAND ----------

duplicate_payment_keys = (
    payments_silver
    .groupBy(
        "order_id",
        "payment_sequential"
    )
    .count()
    .filter(col("count") > 1)
)

print(
    "Duplicate payment keys:",
    duplicate_payment_keys.count()
)

duplicate_payment_keys.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Final Silver validation
# MAGIC
# MAGIC The current dataset should have:
# MAGIC - no missing required fields
# MAGIC - no invalid payment types
# MAGIC - 2 non-positive installment records
# MAGIC - no negative payment values
# MAGIC - no invalid order references
# MAGIC - unique payment grain
# MAGIC

# COMMAND ----------

final_validation = payments_silver.select(
    count("*").alias("total_rows"),

    countDistinct("order_id").alias("distinct_orders"),

    countDistinct(
        "order_id",
        "payment_sequential"
    ).alias("distinct_payment_keys"),

    sum(
        when(col("missing_order_id"), 1).otherwise(0)
    ).alias("missing_order_id"),

    sum(
        when(col("missing_payment_sequential"), 1).otherwise(0)
    ).alias("missing_payment_sequential"),

    sum(
        when(col("missing_payment_type"), 1).otherwise(0)
    ).alias("missing_payment_type"),

    sum(
        when(col("missing_payment_installments"), 1).otherwise(0)
    ).alias("missing_payment_installments"),

    sum(
        when(col("missing_payment_value"), 1).otherwise(0)
    ).alias("missing_payment_value"),

    sum(
        when(col("invalid_payment_type"), 1).otherwise(0)
    ).alias("invalid_payment_type"),

    sum(
        when(col("invalid_payment_installments"), 1).otherwise(0)
    ).alias("invalid_payment_installments"),

    sum(
        when(col("negative_payment_value"), 1).otherwise(0)
    ).alias("negative_payment_value"),

    sum(
        when(col("invalid_order_reference"), 1).otherwise(0)
    ).alias("invalid_order_reference")
)

final_validation.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Persist Silver table
# MAGIC

# COMMAND ----------

(
    payments_silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.silver.order_payments")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Reload persisted Silver table
# MAGIC

# COMMAND ----------

payments_silver_final = spark.table(
    "ecommerce.silver.order_payments"
)

print(
    "Persisted Silver row count:",
    payments_silver_final.count()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Validate persisted table
# MAGIC

# COMMAND ----------

payments_silver_final.select(
    count("*").alias("total_rows"),

    countDistinct(
        "order_id",
        "payment_sequential"
    ).alias("distinct_payment_keys"),

    sum(
        when(col("invalid_payment_type"), 1).otherwise(0)
    ).alias("invalid_payment_type"),

    sum(
        when(col("invalid_payment_installments"), 1).otherwise(0)
    ).alias("invalid_payment_installments"),

    sum(
        when(col("negative_payment_value"), 1).otherwise(0)
    ).alias("negative_payment_value"),

    sum(
        when(col("invalid_order_reference"), 1).otherwise(0)
    ).alias("invalid_order_reference")
).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Inspect final schema
# MAGIC

# COMMAND ----------

payments_silver_final.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Sample Silver records
# MAGIC

# COMMAND ----------

payments_silver_final.orderBy(
    "order_id",
    "payment_sequential"
).show(20, truncate=False)