# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # 00 — Data Profiling
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook profiles the Bronze datasets before implementing Silver transformations.
# MAGIC
# MAGIC The objective is to understand:
# MAGIC - Dataset structure and data types
# MAGIC - Row counts and completeness
# MAGIC - Key uniqueness and duplicates
# MAGIC - Business value distributions
# MAGIC - Relationships between datasets
# MAGIC - Potential data-quality issues
# MAGIC
# MAGIC No source data is modified in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN ecommerce.bronze
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Orders Profiling

# COMMAND ----------

orders_df = spark.table("ecommerce.bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row Count

# COMMAND ----------

print(f"Orders: {orders_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sample Records

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Timestamp Completeness

# COMMAND ----------

from pyspark.sql.functions import col, count, when, min, max

display(
    orders_df.select(
        count("*").alias("total_orders"),
        count("order_approved_at").alias("approved_timestamp"),
        count("order_delivered_carrier_date").alias("carrier_timestamp"),
        count("order_delivered_customer_date").alias("customer_delivery_timestamp")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC - Total orders: 99,441
# MAGIC - Approval timestamp missing: 160
# MAGIC - Carrier timestamp missing: 1,783
# MAGIC - Customer delivery timestamp missing: 2,965
# MAGIC
# MAGIC Missing timestamps are not automatically invalid because availability depends on order lifecycle status.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Order Status Distribution

# COMMAND ----------

display(
    orders_df
    .groupBy("order_status")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC The dataset contains 8 expected order statuses:
# MAGIC `delivered`, `shipped`, `canceled`, `unavailable`, `invoiced`,
# MAGIC `processing`, `created`, and `approved`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Duplicate Order IDs

# COMMAND ----------

duplicate_order_ids_df = (
    orders_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_order_ids_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC No duplicate `order_id` values were found.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Order Date Range

# COMMAND ----------

display(
    orders_df.select(
        min("order_purchase_timestamp").alias("earliest_order"),
        max("order_purchase_timestamp").alias("latest_order")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC Orders range from September 2016 through October 2018.

# COMMAND ----------

# MAGIC %md
# MAGIC # Order Lifecycle Profiling
# MAGIC
# MAGIC Expected lifecycle:
# MAGIC
# MAGIC `Purchase → Approval → Carrier Handoff → Customer Delivery`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Timestamp Ordering

# COMMAND ----------

timestamp_anomalies = orders_df.filter(
    (col("order_approved_at") < col("order_purchase_timestamp")) |
    (col("order_delivered_carrier_date") < col("order_approved_at")) |
    (col("order_delivered_customer_date") < col("order_delivered_carrier_date"))
)

print(f"Timestamp ordering anomalies: {timestamp_anomalies.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC Some records contain timestamps that do not follow the expected chronological sequence.
# MAGIC
# MAGIC Preliminary decision:
# MAGIC - Preserve the original timestamps.
# MAGIC - Do not invent corrected timestamps.
# MAGIC - Consider data-quality flags in Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Delivered Orders Missing Customer Delivery Date

# COMMAND ----------

delivered_orders = orders_df.filter(
    col("order_status") == "delivered"
)

delivered_count = delivered_orders.count()

delivered_missing_date = (
    delivered_orders
    .filter(col("order_delivered_customer_date").isNull())
    .count()
)

print(f"Delivered orders: {delivered_count:,}")
print(f"Missing delivery date: {delivered_missing_date:,}")
print(
    f"Percentage: "
    f"{delivered_missing_date / delivered_count * 100:.2f}%"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC 8 delivered orders are missing `order_delivered_customer_date` (0.01%).
# MAGIC These records should be preserved.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Non-Delivered Orders With Delivery Date

# COMMAND ----------

non_delivered_count = (
    orders_df
    .filter(col("order_status") != "delivered")
    .count()
)

non_delivered_with_date = (
    orders_df
    .filter(
        (col("order_status") != "delivered") &
        col("order_delivered_customer_date").isNotNull()
    )
    .count()
)

print(f"Non-delivered orders: {non_delivered_count:,}")
print(f"With delivery date: {non_delivered_with_date:,}")
print(
    f"Percentage: "
    f"{non_delivered_with_date / non_delivered_count * 100:.2f}%"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC 6 non-delivered orders have a customer delivery timestamp (0.20%).
# MAGIC These records should be preserved pending further investigation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Delivery Timestamp Completeness by Status

# COMMAND ----------

display(
    orders_df
    .groupBy("order_status")
    .agg(
        count("*").alias("order_count"),
        count(
            when(
                col("order_delivered_customer_date").isNull(),
                True
            )
        ).alias("missing_delivery_date")
    )
    .orderBy("order_count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC Missing delivery timestamps are largely explained by order lifecycle status.
# MAGIC The main exception is 8 delivered orders without a delivery timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Approval Timestamp Completeness by Status

# COMMAND ----------

display(
    orders_df
    .groupBy("order_status")
    .agg(
        count("*").alias("order_count"),
        count(
            when(
                col("order_approved_at").isNull(),
                True
            )
        ).alias("missing_approved_at")
    )
    .orderBy("order_count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC Missing approval timestamps are concentrated primarily in canceled orders.
# MAGIC There are also 14 delivered orders and 5 created orders with missing approval timestamps.
# MAGIC
# MAGIC A missing approval timestamp therefore does not necessarily prevent an order from reaching delivered status.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Carrier Timestamp Completeness by Status

# COMMAND ----------

display(
    orders_df
    .groupBy("order_status")
    .agg(
        count("*").alias("order_count"),
        count(
            when(
                col("order_delivered_carrier_date").isNull(),
                True
            )
        ).alias("missing_carrier_date")
    )
    .orderBy("order_count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC Missing carrier timestamps are largely explained by lifecycle status.
# MAGIC Only 2 delivered orders have a missing carrier timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Referential Integrity

# COMMAND ----------

customers_df = spark.table("ecommerce.bronze.customers")

missing_customers_df = (
    orders_df
    .select("customer_id")
    .distinct()
    .join(
        customers_df.select("customer_id").distinct(),
        on="customer_id",
        how="left_anti"
    )
)

print(
    f"Orphan customer IDs: "
    f"{missing_customers_df.count():,}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding**
# MAGIC
# MAGIC No orphan `customer_id` values were found.
# MAGIC Every distinct `customer_id` in `orders` has a corresponding record in `customers`.

# COMMAND ----------

# MAGIC %md
# MAGIC # Orders Profiling Summary
# MAGIC
# MAGIC | Check | Finding | Silver consideration |
# MAGIC |---|---|---|
# MAGIC | Row count | 99,441 | Preserve source records |
# MAGIC | `order_id` uniqueness | No duplicates | No deduplication required |
# MAGIC | Status values | 8 expected statuses | Validate status domain |
# MAGIC | Date range | Sep 2016 – Oct 2018 | Preserve timestamps |
# MAGIC | Approval nulls | 160 | Status-dependent |
# MAGIC | Carrier nulls | 1,783 | Mostly lifecycle-dependent |
# MAGIC | Delivery nulls | 2,965 | Mostly lifecycle-dependent |
# MAGIC | Delivered + missing delivery | 8 (0.01%) | Preserve; consider quality flag |
# MAGIC | Non-delivered + delivery date | 6 (0.20%) | Preserve; consider quality flag |
# MAGIC | Timestamp ordering | Some anomalies | Do not invent corrections |
# MAGIC | Customer integrity | No orphan IDs | Relationship appears valid |

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial Silver Design Considerations
# MAGIC
# MAGIC 1. Preserve source order records.
# MAGIC 2. Maintain `order_id` as the business key.
# MAGIC 3. Validate `order_status` against the expected domain.
# MAGIC 4. Preserve legitimate lifecycle nulls.
# MAGIC 5. Do not replace missing timestamps with artificial values.
# MAGIC 6. Do not automatically delete timestamp-ordering anomalies.
# MAGIC 7. Preserve source timestamps and optionally add quality flags.
# MAGIC 8. Validate the `customer_id` relationship.
# MAGIC 9. Standardize Silver data types and naming conventions.
# MAGIC
# MAGIC Actual transformations will be implemented in `02_silver_orders`.

# COMMAND ----------

# MAGIC %md
# MAGIC # Order Items Profiling
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Profile the Bronze `order_items` table to understand:
# MAGIC
# MAGIC - Table schema and grain
# MAGIC - Natural key and uniqueness
# MAGIC - Item numbering behavior
# MAGIC - Null completeness
# MAGIC - Price and freight-value quality
# MAGIC - Extreme values
# MAGIC - Referential integrity with parent tables
# MAGIC
# MAGIC The goal is to understand the source data before defining Silver transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Table

# COMMAND ----------

order_items_df = spark.table("ecommerce.bronze.order_items")

display(order_items_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema

# COMMAND ----------

order_items_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC The table contains:
# MAGIC
# MAGIC - `order_id` - identifies the order
# MAGIC - `order_item_id` - identifies the item/line within an order
# MAGIC - `product_id` - identifies the product
# MAGIC - `seller_id` - identifies the seller
# MAGIC - `shipping_limit_date` - seller shipping deadline
# MAGIC - `price` - item price
# MAGIC - `freight_value` - freight/shipping charge
# MAGIC
# MAGIC The expected grain is **one row per item line within an order**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Row Count

# COMMAND ----------

print(f"Total rows: {order_items_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC The table contains **112,650 item rows**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Natural Key Uniqueness
# MAGIC
# MAGIC The expected natural key is `(order_id, order_item_id)`.
# MAGIC
# MAGIC `order_item_id` is not globally unique. It identifies the item position within a specific order.

# COMMAND ----------

from pyspark.sql.functions import col

duplicate_items_df = (
    order_items_df
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_items_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC No duplicate `(order_id, order_item_id)` combinations were found.
# MAGIC
# MAGIC This supports `(order_id, order_item_id)` as the natural key at the source grain.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Item Numbering Behavior
# MAGIC
# MAGIC Check whether `order_item_id` is sequential within each order.

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            order_id,
            COUNT(*) AS item_rows,
            MAX(order_item_id) AS max_order_item_id
        FROM ecommerce.bronze.order_items
        GROUP BY order_id
        ORDER BY item_rows DESC
        LIMIT 10
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            order_id,
            COUNT(*) AS item_rows,
            MAX(order_item_id) AS max_order_item_id
        FROM ecommerce.bronze.order_items
        GROUP BY order_id
        HAVING COUNT(*) <> MAX(order_item_id)
        ORDER BY item_rows DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC No orders violated the expected relationship:
# MAGIC
# MAGIC `COUNT(*) = MAX(order_item_id)`
# MAGIC
# MAGIC Combined with the uniqueness check, this indicates that `order_item_id` behaves as a **1-based sequential line number within each order**.
# MAGIC
# MAGIC Therefore:
# MAGIC
# MAGIC - `order_id` identifies the parent order
# MAGIC - `order_item_id` identifies the line within that order
# MAGIC - `product_id` identifies the product associated with that line

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Null Completeness

# COMMAND ----------

from pyspark.sql.functions import count, when

display(
    order_items_df.select(
        count("*").alias("total_rows"),
        *[
            count(
                when(col(c).isNull(), True)
            ).alias(f"{c}_nulls")
            for c in order_items_df.columns
        ]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC No null values were found in any column.
# MAGIC
# MAGIC All item identifiers, seller/product references, shipping dates, prices, and freight values are populated in the Bronze source.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Price and Freight Sanity Checks
# MAGIC
# MAGIC Check for negative and zero monetary values.

# COMMAND ----------

display(
    order_items_df.select(
        (col("price") < 0).alias("negative_price"),
        (col("price") == 0).alias("zero_price"),
        (col("freight_value") < 0).alias("negative_freight"),
        (col("freight_value") == 0).alias("zero_freight")
    )
    .groupBy(
        "negative_price",
        "zero_price",
        "negative_freight",
        "zero_freight"
    )
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC - No negative prices
# MAGIC - No zero prices
# MAGIC - No negative freight values
# MAGIC - 383 rows have zero freight
# MAGIC - 112,267 rows have positive price and positive freight
# MAGIC
# MAGIC The zero-freight records were investigated and were concentrated among a small number of sellers. They appear structured rather than random, so they were **not classified as invalid**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Price and Freight Distribution

# COMMAND ----------

display(
    order_items_df.select(
        "price",
        "freight_value"
    ).summary(
        "count",
        "min",
        "25%",
        "50%",
        "75%",
        "max"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC Price:
# MAGIC
# MAGIC - Minimum: `0.85`
# MAGIC - 25th percentile: `39.90`
# MAGIC - Median: `74.99`
# MAGIC - 75th percentile: `134.90`
# MAGIC - Maximum: `6735.00`
# MAGIC
# MAGIC Freight:
# MAGIC
# MAGIC - Minimum: `0.00`
# MAGIC - 25th percentile: `13.08`
# MAGIC - Median: `16.26`
# MAGIC - 75th percentile: `21.15`
# MAGIC - Maximum: `409.68`
# MAGIC
# MAGIC The extreme price and freight values were inspected individually. They appear structured and were not classified as invalid.
# MAGIC
# MAGIC No value-based filtering rule was justified from this profiling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Referential Integrity: Orders
# MAGIC
# MAGIC Check whether every order referenced by `order_items` exists in the Bronze `orders` table.

# COMMAND ----------

orders_df = spark.table("ecommerce.bronze.orders")

missing_orders_df = (
    order_items_df
    .select("order_id")
    .distinct()
    .join(
        orders_df.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
)

display(missing_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC No orphaned `order_id` values were found.
# MAGIC
# MAGIC Every distinct `order_id` referenced by `order_items` exists in the Bronze `orders` table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Profiling Summary
# MAGIC
# MAGIC ### Grain
# MAGIC
# MAGIC One row represents one item line within an order.
# MAGIC
# MAGIC ### Key
# MAGIC
# MAGIC
# MAGIC `(order_id, order_item_id)` behaves as the natural key.
# MAGIC
# MAGIC ### Item numbering
# MAGIC
# MAGIC `order_item_id` is a 1-based sequential item position within each order.
# MAGIC
# MAGIC ### Completeness
# MAGIC
# MAGIC No nulls were found in any column.
# MAGIC
# MAGIC ### Monetary values
# MAGIC
# MAGIC No negative prices or freight values were found.
# MAGIC
# MAGIC 383 rows have zero freight. These values appear structured and were not classified as invalid.
# MAGIC
# MAGIC Extreme price and freight values were inspected and did not provide sufficient evidence for filtering or correction.
# MAGIC
# MAGIC ### Referential integrity
# MAGIC
# MAGIC Every `order_id` in `order_items` matches an existing order in the Bronze `orders` table.
# MAGIC
# MAGIC ### Silver implications
# MAGIC
# MAGIC Based on the profiling performed so far:
# MAGIC
# MAGIC - Preserve the natural grain `(order_id, order_item_id)`.
# MAGIC - Do not remove rows based on price or freight extremes.
# MAGIC - Do not treat zero freight as automatically invalid.
# MAGIC - Preserve source monetary values unless a later business rule provides evidence for transformation.
# MAGIC - Validate `product_id` and `seller_id` against their parent tables before finalizing the Silver model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Referential Integrity: Products
# MAGIC
# MAGIC Check whether every product referenced by `order_items` exists in the Bronze `products` table.

# COMMAND ----------

products_df = spark.table("ecommerce.bronze.products")

missing_products_df = (
    order_items_df
    .select("product_id")
    .distinct()
    .join(
        products_df.select("product_id").distinct(),
        on="product_id",
        how="left_anti"
    )
)

display(missing_products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC No orphaned `product_id` values were found.
# MAGIC
# MAGIC Every distinct `product_id` referenced by `order_items` exists in the Bronze `products` table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Referential Integrity: Sellers
# MAGIC
# MAGIC Check whether every seller referenced by `order_items` exists in the Bronze `sellers` table.

# COMMAND ----------

sellers_df = spark.table("ecommerce.bronze.sellers")

missing_sellers_df = (
    order_items_df
    .select("seller_id")
    .distinct()
    .join(
        sellers_df.select("seller_id").distinct(),
        on="seller_id",
        how="left_anti"
    )
)

display(missing_sellers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding
# MAGIC
# MAGIC No orphaned `seller_id` values were found.
# MAGIC
# MAGIC Every distinct `seller_id` referenced by `order_items` exists in the Bronze `sellers` table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Final Profiling Summary
# MAGIC
# MAGIC ### Grain
# MAGIC One row represents one item line within an order.
# MAGIC
# MAGIC ### Key
# MAGIC `(order_id, order_item_id)` behaves as the natural key.
# MAGIC
# MAGIC ### Item numbering
# MAGIC `order_item_id` is a 1-based sequential item position within each order.
# MAGIC
# MAGIC ### Completeness
# MAGIC No null values were found in any column.
# MAGIC
# MAGIC ### Monetary values
# MAGIC No negative prices or freight values were found.
# MAGIC
# MAGIC 383 rows have zero freight. These values appear structured and were not classified as invalid.
# MAGIC
# MAGIC Extreme price and freight values were inspected and did not provide sufficient evidence for filtering or correction.
# MAGIC
# MAGIC ### Referential integrity
# MAGIC All `order_id`, `product_id`, and `seller_id` references were found in their corresponding Bronze parent tables.
# MAGIC
# MAGIC ### Silver implications
# MAGIC
# MAGIC - Preserve the natural grain `(order_id, order_item_id)`.
# MAGIC - Preserve source price and freight values.
# MAGIC - Do not remove zero-freight rows based on profiling alone.
# MAGIC - Do not remove extreme monetary values based on profiling alone.
# MAGIC - Retain valid relationships to orders, products, and sellers.
# MAGIC - Silver transformations should focus on validation, standardization, and explicit data-quality flags rather than arbitrary row removal.

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Profiling Summary
# MAGIC
# MAGIC ### Table
# MAGIC `ecommerce.bronze.customers`
# MAGIC
# MAGIC ### Findings
# MAGIC
# MAGIC - `customer_id` is unique.
# MAGIC - No null values were found.
# MAGIC - `customer_id` can be used as the primary/natural key for the customer table.
# MAGIC - No immediate data-quality issue was identified that requires additional profiling.
# MAGIC
# MAGIC ### Silver implications
# MAGIC
# MAGIC - Preserve `customer_id` as the customer key.
# MAGIC - Retain customer attributes.
# MAGIC - No null-handling rule is required for the current columns based on profiling.

# COMMAND ----------

# MAGIC %md
# MAGIC # Product Profiling Summary
# MAGIC
# MAGIC ### Table
# MAGIC `ecommerce.bronze.products`
# MAGIC
# MAGIC ### Findings
# MAGIC
# MAGIC - `product_id` is unique.
# MAGIC - No null `product_id` values were found.
# MAGIC - `product_category_name`: 610 nulls.
# MAGIC - `product_name_lenght`: 610 nulls.
# MAGIC - `product_description_lenght`: 610 nulls.
# MAGIC - `product_photos_qty`: 610 nulls.
# MAGIC - `product_weight_g`: 2 nulls.
# MAGIC - `product_length_cm`: 2 nulls.
# MAGIC - `product_height_cm`: 2 nulls.
# MAGIC - `product_width_cm`: 2 nulls.
# MAGIC
# MAGIC The missing product attributes appear to represent incomplete source records rather than duplicate products.
# MAGIC
# MAGIC ### Silver implications
# MAGIC
# MAGIC - Preserve `product_id` as the product key.
# MAGIC - Do not remove products because optional attributes are missing.
# MAGIC - Preserve missing attributes as null unless a specific business rule requires otherwise.
# MAGIC - Consider standardizing the source column names during Silver transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC # Seller Profiling Summary
# MAGIC
# MAGIC ### Table
# MAGIC `ecommerce.bronze.sellers`
# MAGIC
# MAGIC ### Findings
# MAGIC
# MAGIC - `seller_id` is unique.
# MAGIC - No null values were found.
# MAGIC - No immediate data-quality issue was identified.
# MAGIC
# MAGIC ### Silver implications
# MAGIC
# MAGIC - Preserve `seller_id` as the seller key.
# MAGIC - Retain seller location attributes.
# MAGIC - No additional null-handling rule is required based on profiling.

# COMMAND ----------

# MAGIC %md
# MAGIC # Order Payments Profiling Summary
# MAGIC
# MAGIC ### Table
# MAGIC `ecommerce.bronze.order_payments`
# MAGIC
# MAGIC ### Findings
# MAGIC
# MAGIC - `(order_id, payment_sequential)` is unique.
# MAGIC - No null values were found.
# MAGIC - Multiple payment records can exist for the same order.
# MAGIC - `payment_sequential` therefore represents the payment sequence within an order.
# MAGIC
# MAGIC ### Silver implications
# MAGIC
# MAGIC - Preserve the grain as one row per payment within an order.
# MAGIC - Use `(order_id, payment_sequential)` as the natural key.
# MAGIC - Do not aggregate payments at the Silver layer; retain individual payment records.

# COMMAND ----------

# MAGIC %md
# MAGIC # Order Reviews Profiling Summary
# MAGIC
# MAGIC ### Table
# MAGIC `ecommerce.bronze.order_reviews`
# MAGIC
# MAGIC ### Findings
# MAGIC
# MAGIC - 104,162 review records were found.
# MAGIC - `review_id`: 1 null.
# MAGIC - `order_id`: 2,236 nulls.
# MAGIC - `review_score`: 2,380 nulls.
# MAGIC - `review_comment_title`: 92,157 nulls.
# MAGIC - `review_comment_message`: 63,079 nulls.
# MAGIC - `review_creation_date`: 8,764 nulls.
# MAGIC - `review_answer_timestamp`: 8,785 nulls.
# MAGIC
# MAGIC The large number of missing comment fields is not necessarily invalid because customers can submit a review score without providing written comments.
# MAGIC
# MAGIC The review identifier relationship also showed source-quality issues, so uniqueness should not be assumed without additional validation.
# MAGIC
# MAGIC ### Silver implications
# MAGIC
# MAGIC - Preserve review records rather than dropping rows solely because comment fields are null.
# MAGIC - Preserve null review attributes where the source provides no value.
# MAGIC - Treat missing `order_id` and `review_score` as data-quality issues that may require explicit Silver quality flags.
# MAGIC - Avoid assuming `review_id` alone, or `(review_id, order_id)`, is a clean natural key without further validation.