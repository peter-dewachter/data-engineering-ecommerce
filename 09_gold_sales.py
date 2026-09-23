# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 09 - Gold Sales
# MAGIC
# MAGIC **Grain:** 1 row per order item `(order_id, order_item_id)`
# MAGIC
# MAGIC **Business purpose:** Create a business-facing sales fact table combining order, customer, product, and seller information with item-level financial measures.

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
# MAGIC ## Load Silver Tables

# COMMAND ----------

order_items = (
    spark.table("ecommerce.silver.order_items")
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value"
    )
)

orders = (
    spark.table("ecommerce.silver.orders")
    .select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "order_status"
    )
)

products = (
    spark.table("ecommerce.silver.products")
    .select(
        "product_id",
        "product_category_name"
    )
)

sellers = (
    spark.table("ecommerce.silver.sellers")
    .select(
        "seller_id",
        "seller_state"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Orders
# MAGIC
# MAGIC Keep `order_items` as the driving table so the Gold grain remains one row per order item.

# COMMAND ----------

sales_with_orders = (
    order_items.alias("oi")
    .join(
        orders.alias("o"),
        on=col("oi.order_id") == col("o.order_id"),
        how="left"
    )
    .select(
        col("oi.order_id"),
        col("oi.order_item_id"),
        col("oi.product_id"),
        col("oi.seller_id"),
        col("o.customer_id"),
        col("o.order_purchase_timestamp"),
        col("o.order_status"),
        col("oi.price"),
        col("oi.freight_value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Products

# COMMAND ----------

sales_with_products = (
    sales_with_orders.alias("s")
    .join(
        products.alias("p"),
        on=col("s.product_id") == col("p.product_id"),
        how="left"
    )
    .select(
        col("s.order_id"),
        col("s.order_item_id"),
        col("s.product_id"),
        col("s.seller_id"),
        col("s.customer_id"),
        col("s.order_purchase_timestamp"),
        col("s.order_status"),
        col("p.product_category_name"),
        col("s.price"),
        col("s.freight_value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Sellers

# COMMAND ----------

sales_with_sellers = (
    sales_with_products.alias("s")
    .join(
        sellers.alias("se"),
        on=col("s.seller_id") == col("se.seller_id"),
        how="left"
    )
    .select(
        col("s.order_id"),
        col("s.order_item_id"),
        col("s.product_id"),
        col("s.seller_id"),
        col("s.customer_id"),
        col("s.order_purchase_timestamp"),
        col("s.order_status"),
        col("s.product_category_name"),
        col("se.seller_state"),
        col("s.price"),
        col("s.freight_value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Business Measure
# MAGIC
# MAGIC `item_total_value` represents the product price plus the freight charge associated with that order item.

# COMMAND ----------

sales_final = (
    sales_with_sellers
    .withColumn(
        "item_total_value",
        col("price") + col("freight_value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Gold Columns

# COMMAND ----------

gold_sales = sales_final.select(
    "order_id",
    "order_item_id",
    "customer_id",
    "product_id",
    "seller_id",
    "order_purchase_timestamp",
    "order_status",
    "product_category_name",
    "seller_state",
    "price",
    "freight_value",
    "item_total_value"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Sales Table

# COMMAND ----------

(
    gold_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ecommerce.gold.sales")
)