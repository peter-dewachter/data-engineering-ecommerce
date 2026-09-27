# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold Customers
# MAGIC
# MAGIC **Grain:** 1 row per `customer_unique_id`
# MAGIC
# MAGIC **Business purpose:** Create a customer-level analytical table containing customer identity, purchasing behavior, spending, payment activity, and review metrics.

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    min,
    max,
    count,
    countDistinct,
    sum,
    avg,
    round
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Schema

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS ecommerce.gold
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Customers

# COMMAND ----------

customers = (
    spark.table("ecommerce.silver.customers")
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Orders

# COMMAND ----------

orders = (
    spark.table("ecommerce.silver.orders")
    .select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "order_status"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Customer-Order Base
# MAGIC
# MAGIC `customer_unique_id` represents the real customer.
# MAGIC `customer_id` is used to connect customer records to orders.

# COMMAND ----------

customer_orders = (
    customers.alias("c")
    .join(
        orders.alias("o"),
        on=col("c.customer_id") == col("o.customer_id"),
        how="left"
    )
    .select(
        col("c.customer_unique_id"),
        col("c.customer_city"),
        col("c.customer_state"),
        col("o.order_id"),
        col("o.order_purchase_timestamp"),
        col("o.order_status")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Order Items

# COMMAND ----------

order_items = (
    spark.table("ecommerce.silver.order_items")
    .select(
        "order_id",
        "order_item_id",
        "price",
        "freight_value"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach Order Items
# MAGIC
# MAGIC This temporarily creates an order-item-level dataset so customer purchasing metrics can be calculated.

# COMMAND ----------

customer_order_items = (
    customer_orders.alias("co")
    .join(
        order_items.alias("oi"),
        on=col("co.order_id") == col("oi.order_id"),
        how="left"
    )
    .select(
        col("co.customer_unique_id"),
        col("co.customer_city"),
        col("co.customer_state"),
        col("co.order_id"),
        col("co.order_purchase_timestamp"),
        col("co.order_status"),
        col("oi.order_item_id"),
        col("oi.price"),
        col("oi.freight_value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Customer Sales Metrics

# COMMAND ----------

customer_sales = (
    customer_order_items
    .groupBy(
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
    .agg(
        min("order_purchase_timestamp").alias("first_order_date"),
        max("order_purchase_timestamp").alias("last_order_date"),
        countDistinct("order_id").alias("total_orders"),
        count("order_item_id").alias("total_items"),
        sum("price").alias("total_product_value"),
        sum("freight_value").alias("total_freight_value")
    )
    .withColumn(
        "total_spend",
        col("total_product_value") + col("total_freight_value")
    )
    .withColumn(
        "average_order_value",
        round(
            col("total_spend") / col("total_orders"),
            2
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Aggregate Payments
# MAGIC
# MAGIC Payments are first aggregated to the order level to avoid multiplying customer sales rows.

# COMMAND ----------

order_payments = (
    spark.table("ecommerce.silver.order_payments")
    .groupBy("order_id")
    .agg(
        count("*").alias("payment_record_count")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Payment Metrics to Customer Level

# COMMAND ----------

customer_payment_metrics = (
    customer_orders
    .join(
        order_payments,
        on="order_id",
        how="left"
    )
    .groupBy("customer_unique_id")
    .agg(
        sum("payment_record_count").alias("total_payments")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Payment Metrics

# COMMAND ----------

customer_sales = (
    customer_sales
    .join(
        customer_payment_metrics,
        on="customer_unique_id",
        how="left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Aggregate Reviews
# MAGIC
# MAGIC Only valid review scores are used for customer satisfaction metrics.

# COMMAND ----------

order_reviews = (
    spark.table("ecommerce.silver.order_reviews")
    .filter(
        col("review_score_int").isNotNull()
    )
    .groupBy("order_id")
    .agg(
        count("*").alias("review_count"),
        avg("review_score_int").alias("average_review_score")
    )
    .withColumn(
        "average_review_score",
        round(col("average_review_score"), 2)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Review Metrics to Customer Level

# COMMAND ----------

customer_review_metrics = (
    customer_orders
    .join(
        order_reviews,
        on="order_id",
        how="left"
    )
    .groupBy("customer_unique_id")
    .agg(
        sum("review_count").alias("total_reviews"),
        round(
            avg("average_review_score"),
            2
        ).alias("average_review_score")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Review Metrics

# COMMAND ----------

customer_sales = (
    customer_sales
    .join(
        customer_review_metrics,
        on="customer_unique_id",
        how="left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Final Gold Columns

# COMMAND ----------

gold_customers = customer_sales.select(
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "first_order_date",
    "last_order_date",
    "total_orders",
    "total_items",
    "total_product_value",
    "total_freight_value",
    "total_spend",
    "average_order_value",
    "total_payments",
    "total_reviews",
    "average_review_score"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Customers Table

# COMMAND ----------

(
    gold_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.gold.customers")
)