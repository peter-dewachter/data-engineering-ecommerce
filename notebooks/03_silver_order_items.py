# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — Order Items
# MAGIC
# MAGIC Transform the Bronze order items table into a validated Silver Delta table.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC - Standardize identifier fields
# MAGIC - Preserve the order-item grain
# MAGIC - Flag missing identifiers
# MAGIC - Flag invalid numeric values
# MAGIC - Flag broken foreign-key references
# MAGIC - Flag shipping-date quality issues
# MAGIC - Validate the transformed dataset
# MAGIC - Write `ecommerce.silver.order_items`

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    when,
    count_if
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze

# COMMAND ----------

order_items_df = spark.table(
    "ecommerce.bronze.order_items"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Standardize identifier fields
# MAGIC
# MAGIC The Bronze schema already contains appropriate data types.
# MAGIC We trim identifier fields while preserving all other source columns.

# COMMAND ----------

order_items_silver = order_items_df.select(
    trim("order_id").alias("order_id"),
    trim("product_id").alias("product_id"),
    trim("seller_id").alias("seller_id"),
    "order_item_id",
    "shipping_limit_date",
    "price",
    "freight_value"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add intrinsic data-quality flags
# MAGIC
# MAGIC Keep all records and explicitly flag potential quality issues.
# MAGIC
# MAGIC Zero freight is allowed because it can represent legitimate
# MAGIC free-shipping or business scenarios.

# COMMAND ----------

order_items_silver = (
    order_items_silver
    .withColumn(
        "missing_order_id",
        col("order_id").isNull()
    )
    .withColumn(
        "missing_order_item_id",
        col("order_item_id").isNull()
    )
    .withColumn(
        "missing_product_id",
        col("product_id").isNull()
    )
    .withColumn(
        "missing_seller_id",
        col("seller_id").isNull()
    )
    .withColumn(
        "negative_price",
        when(
            col("price").isNotNull() &
            (col("price") < 0),
            True
        ).otherwise(False)
    )
    .withColumn(
        "negative_freight_value",
        when(
            col("freight_value").isNotNull() &
            (col("freight_value") < 0),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate foreign-key references
# MAGIC
# MAGIC An order item should reference an existing order, product, and seller.
# MAGIC
# MAGIC Missing IDs and invalid references are treated separately:
# MAGIC - missing ID = the identifier itself is NULL
# MAGIC - invalid reference = an identifier exists but has no matching parent

# COMMAND ----------

orders = spark.table(
    "ecommerce.silver.orders"
).select(
    "order_id"
)

products = spark.table(
    "ecommerce.bronze.products"
).select(
    "product_id"
)

sellers = spark.table(
    "ecommerce.bronze.sellers"
).select(
    "seller_id"
)

order_items_silver = (
    order_items_silver
    .join(
        orders.withColumn(
            "order_exists",
            col("order_id")
        ),
        on="order_id",
        how="left"
    )
    .join(
        products.withColumn(
            "product_exists",
            col("product_id")
        ),
        on="product_id",
        how="left"
    )
    .join(
        sellers.withColumn(
            "seller_exists",
            col("seller_id")
        ),
        on="seller_id",
        how="left"
    )
    .withColumn(
        "invalid_order_reference",
        col("order_exists").isNull() &
        col("order_id").isNotNull()
    )
    .withColumn(
        "invalid_product_reference",
        col("product_exists").isNull() &
        col("product_id").isNotNull()
    )
    .withColumn(
        "invalid_seller_reference",
        col("seller_exists").isNull() &
        col("seller_id").isNotNull()
    )
    .drop(
        "order_exists",
        "product_exists",
        "seller_exists"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate shipping dates
# MAGIC
# MAGIC `shipping_limit_date` should normally be present and should not occur
# MAGIC before the order purchase timestamp.
# MAGIC
# MAGIC The purchase timestamp is used only to evaluate the rule and is not
# MAGIC retained as part of the order-item Silver entity.

# COMMAND ----------

orders_purchase = spark.table(
    "ecommerce.silver.orders"
).select(
    "order_id",
    "order_purchase_timestamp"
)

order_items_silver = (
    order_items_silver
    .join(
        orders_purchase,
        on="order_id",
        how="left"
    )
    .withColumn(
        "missing_shipping_limit_date",
        col("shipping_limit_date").isNull()
    )
    .withColumn(
        "shipping_limit_before_purchase",
        when(
            col("shipping_limit_date").isNotNull() &
            col("order_purchase_timestamp").isNotNull() &
            (
                col("shipping_limit_date") <
                col("order_purchase_timestamp")
            ),
            True
        ).otherwise(False)
    )
    .drop(
        "order_purchase_timestamp"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate transformed data
# MAGIC
# MAGIC The current dataset should have zero violations for these rules.
# MAGIC The flags remain part of Silver so future problematic records can be
# MAGIC identified without silently removing them.

# COMMAND ----------

display(
    order_items_silver.select(
        count_if(
            col("missing_order_id")
        ).alias("missing_order_id"),

        count_if(
            col("missing_order_item_id")
        ).alias("missing_order_item_id"),

        count_if(
            col("missing_product_id")
        ).alias("missing_product_id"),

        count_if(
            col("missing_seller_id")
        ).alias("missing_seller_id"),

        count_if(
            col("negative_price")
        ).alias("negative_price"),

        count_if(
            col("negative_freight_value")
        ).alias("negative_freight_value"),

        count_if(
            col("invalid_order_reference")
        ).alias("invalid_order_reference"),

        count_if(
            col("invalid_product_reference")
        ).alias("invalid_product_reference"),

        count_if(
            col("invalid_seller_reference")
        ).alias("invalid_seller_reference"),

        count_if(
            col("missing_shipping_limit_date")
        ).alias("missing_shipping_limit_date"),

        count_if(
            col("shipping_limit_before_purchase")
        ).alias("shipping_limit_before_purchase")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate Silver grain
# MAGIC
# MAGIC The natural key is `(order_id, order_item_id)`.
# MAGIC There should be at most one row for each combination.

# COMMAND ----------

display(
    order_items_silver
    .groupBy(
        "order_id",
        "order_item_id"
    )
    .count()
    .filter(
        col("count") > 1
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Silver table

# COMMAND ----------

(
    order_items_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "ecommerce.silver.order_items"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate persisted Silver table

# COMMAND ----------

silver_order_items = spark.table(
    "ecommerce.silver.order_items"
)

print(
    f"Silver row count: {silver_order_items.count()}"
)

display(
    silver_order_items.limit(10)
)