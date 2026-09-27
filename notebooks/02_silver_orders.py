# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — Orders
# MAGIC
# MAGIC Transform the Bronze orders table into a validated Silver Delta table.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC - Standardize string fields
# MAGIC - Validate order status
# MAGIC - Flag timestamp chronology issues
# MAGIC - Flag missing lifecycle timestamps
# MAGIC - Add useful delivery metrics
# MAGIC - Validate the transformed dataset
# MAGIC - Write `ecommerce.silver.orders`

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    when,
    timestamp_diff,
    min,
    max,
    sum
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze

# COMMAND ----------

orders_df = spark.table("ecommerce.bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Standardize string fields
# MAGIC
# MAGIC The source schema already contains the correct data types.
# MAGIC We therefore preserve the timestamp types and defensively trim string fields.

# COMMAND ----------

orders_silver = orders_df.select(
    trim("order_id").alias("order_id"),
    trim("customer_id").alias("customer_id"),
    trim("order_status").alias("order_status"),
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate order status
# MAGIC
# MAGIC Keep all records, but flag unexpected status values rather than silently
# MAGIC filtering them out.

# COMMAND ----------

valid_statuses = [
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved"
]

orders_silver = orders_silver.withColumn(
    "is_valid_status",
    when(
        col("order_status").isin(valid_statuses),
        True
    ).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate timestamp chronology
# MAGIC
# MAGIC Preserve the original timestamps and flag lifecycle inconsistencies.
# MAGIC
# MAGIC Expected order:
# MAGIC
# MAGIC `purchase → approval → carrier handoff → customer delivery`

# COMMAND ----------

orders_silver = (
    orders_silver
    .withColumn(
        "approval_before_purchase",
        when(
            col("order_approved_at").isNotNull() &
            (
                col("order_approved_at")
                < col("order_purchase_timestamp")
            ),
            True
        ).otherwise(False)
    )
    .withColumn(
        "carrier_before_approval",
        when(
            col("order_delivered_carrier_date").isNotNull() &
            col("order_approved_at").isNotNull() &
            (
                col("order_delivered_carrier_date")
                < col("order_approved_at")
            ),
            True
        ).otherwise(False)
    )
    .withColumn(
        "customer_before_carrier",
        when(
            col("order_delivered_customer_date").isNotNull() &
            col("order_delivered_carrier_date").isNotNull() &
            (
                col("order_delivered_customer_date")
                < col("order_delivered_carrier_date")
            ),
            True
        ).otherwise(False)
    )
)

orders_silver = orders_silver.withColumn(
    "has_timestamp_anomaly",
    col("approval_before_purchase") |
    col("carrier_before_approval") |
    col("customer_before_carrier")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Flag missing lifecycle timestamps
# MAGIC
# MAGIC Missing values are not automatically invalid because timestamps depend
# MAGIC on the order lifecycle.
# MAGIC
# MAGIC Delivered orders, however, should normally have delivery and carrier
# MAGIC timestamps. The estimated delivery date is treated as a required field
# MAGIC for the source order record.

# COMMAND ----------

orders_silver = (
    orders_silver
    .withColumn(
        "missing_delivery_timestamp",
        (
            (col("order_status") == "delivered") &
            col("order_delivered_customer_date").isNull()
        )
    )
    .withColumn(
        "missing_carrier_timestamp",
        (
            (col("order_status") == "delivered") &
            col("order_delivered_carrier_date").isNull()
        )
    )
    .withColumn(
        "missing_estimated_delivery_date",
        col("order_estimated_delivery_date").isNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate required identifiers
# MAGIC
# MAGIC IDs are required for reliable downstream joins and order-level analysis.
# MAGIC Records are retained and flagged rather than dropped.

# COMMAND ----------

orders_silver = (
    orders_silver
    .withColumn(
        "missing_order_id",
        col("order_id").isNull()
    )
    .withColumn(
        "missing_customer_id",
        col("customer_id").isNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Add delivery metrics
# MAGIC
# MAGIC `delivery_days` measures the elapsed days from purchase to customer delivery.
# MAGIC
# MAGIC `days_late` measures the difference between actual and estimated delivery:
# MAGIC - positive = late
# MAGIC - zero = on time
# MAGIC - negative = early
# MAGIC
# MAGIC Metrics remain NULL when the required timestamps are unavailable.

# COMMAND ----------

orders_silver = (
    orders_silver
    .withColumn(
        "delivery_days",
        when(
            col("order_delivered_customer_date").isNull() |
            col("order_purchase_timestamp").isNull(),
            None
        ).otherwise(
            timestamp_diff(
                "day",
                "order_purchase_timestamp",
                "order_delivered_customer_date"
            )
        )
    )
    .withColumn(
        "days_late",
        when(
            col("order_delivered_customer_date").isNull() |
            col("order_estimated_delivery_date").isNull(),
            None
        ).otherwise(
            timestamp_diff(
                "day",
                "order_estimated_delivery_date",
                "order_delivered_customer_date"
            )
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate transformed data
# MAGIC
# MAGIC Check derived metrics and data-quality flags before persisting the
# MAGIC Silver table.

# COMMAND ----------

display(
    orders_silver.select(
        min("delivery_days").alias("min_delivery_days"),
        max("delivery_days").alias("max_delivery_days"),
        sum(
            when(col("delivery_days").isNull(), 1).otherwise(0)
        ).alias("delivery_days_nulls"),

        min("days_late").alias("min_days_late"),
        max("days_late").alias("max_days_late"),
        sum(
            when(col("days_late").isNull(), 1).otherwise(0)
        ).alias("days_late_nulls"),

        sum(
            when(col("has_timestamp_anomaly"), 1).otherwise(0)
        ).alias("timestamp_anomalies"),

        sum(
            when(col("missing_delivery_timestamp"), 1).otherwise(0)
        ).alias("missing_delivery_timestamp"),

        sum(
            when(col("missing_carrier_timestamp"), 1).otherwise(0)
        ).alias("missing_carrier_timestamp"),

        sum(
            when(col("missing_estimated_delivery_date"), 1).otherwise(0)
        ).alias("missing_estimated_delivery_date"),

        sum(
            when(col("missing_order_id"), 1).otherwise(0)
        ).alias("missing_order_id"),

        sum(
            when(col("missing_customer_id"), 1).otherwise(0)
        ).alias("missing_customer_id")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;

# COMMAND ----------

(
    orders_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.silver.orders")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate persisted Silver table

# COMMAND ----------

silver_orders = spark.table("ecommerce.silver.orders")

print(f"Silver row count: {silver_orders.count()}")

display(silver_orders.limit(10))
