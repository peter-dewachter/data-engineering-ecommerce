# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold Deliveries
# MAGIC
# MAGIC **Grain:** 1 row per `order_id`
# MAGIC
# MAGIC **Business purpose:** Create a business-facing delivery performance table containing order timing, delivery duration, estimated delivery performance, and relevant delivery quality indicators.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Schema

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS ecommerce.gold
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Orders

# COMMAND ----------

orders = (
    spark.table("ecommerce.silver.orders")
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "days_late",
        "has_timestamp_anomaly",
        "missing_delivery_timestamp",
        "missing_carrier_timestamp",
        "missing_estimated_delivery_date"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Deliveries Table
# MAGIC
# MAGIC The Silver orders table already contains the validated delivery metrics and quality indicators.
# MAGIC Gold exposes these fields in a business-facing deliveries table.

# COMMAND ----------

gold_deliveries = orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "delivery_days",
    "days_late",
    "has_timestamp_anomaly",
    "missing_delivery_timestamp",
    "missing_carrier_timestamp",
    "missing_estimated_delivery_date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Deliveries Table

# COMMAND ----------

(
    gold_deliveries.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.gold.deliveries")
)