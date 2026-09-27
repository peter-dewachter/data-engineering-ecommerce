from __future__ import annotations

from datetime import date, datetime
from typing import Iterable

import pandas as pd

from ecommerce_dashboard.db import run_query

SALES_TABLE = "ecommerce.gold.sales"
CUSTOMERS_TABLE = "ecommerce.gold.customers"
DELIVERIES_TABLE = "ecommerce.gold.deliveries"


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        normalized = value.date().isoformat()
    elif isinstance(value, date):
        normalized = value.isoformat()
    else:
        normalized = date.fromisoformat(str(value)).isoformat()
    return f"DATE('{normalized}')"


def _in_clause(column_name: str, values: Iterable[str] | None) -> str | None:
    cleaned_values = []
    for value in values or []:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            cleaned_values.append(_quote_string(text))

    if not cleaned_values:
        return None

    return f"{column_name} IN ({', '.join(cleaned_values)})"


def _build_where_clause(clauses: list[str]) -> str:
    active_clauses = [clause for clause in clauses if clause]
    if not active_clauses:
        return ""
    return "WHERE " + " AND ".join(active_clauses)


def build_sales_where_clause(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
) -> str:
    clauses: list[str] = []

    if start_date:
        clauses.append(
            f"DATE(order_purchase_timestamp) >= {_quote_date(start_date)}"
        )
    if end_date:
        clauses.append(
            f"DATE(order_purchase_timestamp) <= {_quote_date(end_date)}"
        )

    for clause in (
        _in_clause("order_status", order_statuses),
        _in_clause("seller_state", seller_states),
        _in_clause("product_category_name", product_categories),
    ):
        if clause:
            clauses.append(clause)

    return _build_where_clause(clauses)


def build_customer_where_clause(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
) -> str:
    clauses: list[str] = []

    state_clause = _in_clause("customer_state", customer_states)
    if state_clause:
        clauses.append(state_clause)

    if minimum_total_spend not in (None, ""):
        clauses.append(f"total_spend >= {float(minimum_total_spend)}")

    if minimum_total_orders not in (None, ""):
        clauses.append(f"total_orders >= {int(minimum_total_orders)}")

    return _build_where_clause(clauses)


def build_delivery_where_clause(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> str:
    clauses: list[str] = []

    if start_date:
        clauses.append(
            f"DATE(order_purchase_timestamp) >= {_quote_date(start_date)}"
        )
    if end_date:
        clauses.append(
            f"DATE(order_purchase_timestamp) <= {_quote_date(end_date)}"
        )

    status_clause = _in_clause("order_status", order_statuses)
    if status_clause:
        clauses.append(status_clause)

    return _build_where_clause(clauses)


def get_sales_date_bounds() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            MIN(DATE(order_purchase_timestamp)) AS min_date,
            MAX(DATE(order_purchase_timestamp)) AS max_date
        FROM {SALES_TABLE}
        """
    )


def get_sales_filter_options() -> dict[str, list[str]]:
    status_df = run_query(
        f"""
        SELECT DISTINCT order_status
        FROM {SALES_TABLE}
        WHERE order_status IS NOT NULL
        ORDER BY order_status
        """
    )
    state_df = run_query(
        f"""
        SELECT DISTINCT seller_state
        FROM {SALES_TABLE}
        WHERE seller_state IS NOT NULL
        ORDER BY seller_state
        """
    )
    category_df = run_query(
        f"""
        SELECT DISTINCT product_category_name
        FROM {SALES_TABLE}
        WHERE product_category_name IS NOT NULL
        ORDER BY product_category_name
        """
    )
    return {
        "order_statuses": status_df["order_status"].tolist(),
        "seller_states": state_df["seller_state"].tolist(),
        "product_categories": category_df["product_category_name"].tolist(),
    }


def get_overview_kpis() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            (SELECT COALESCE(SUM(item_total_value), 0) FROM {SALES_TABLE}) AS total_revenue,
            (SELECT COUNT(DISTINCT order_id) FROM {SALES_TABLE}) AS total_orders,
            (SELECT COUNT(*) FROM {SALES_TABLE}) AS total_items,
            (SELECT COUNT(*) FROM {CUSTOMERS_TABLE}) AS total_customers,
            (
                SELECT COALESCE(AVG(order_total), 0)
                FROM (
                    SELECT order_id, SUM(item_total_value) AS order_total
                    FROM {SALES_TABLE}
                    GROUP BY order_id
                ) order_totals
            ) AS average_order_value,
            (
                SELECT COALESCE(AVG(delivery_days), 0)
                FROM {DELIVERIES_TABLE}
                WHERE delivery_days IS NOT NULL
            ) AS average_delivery_days
        """
    )


def get_overview_revenue_over_time() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            DATE_TRUNC('month', order_purchase_timestamp) AS purchase_month,
            SUM(item_total_value) AS revenue
        FROM {SALES_TABLE}
        GROUP BY purchase_month
        ORDER BY purchase_month
        """
    )


def get_overview_revenue_by_category(limit: int = 10) -> pd.DataFrame:
    limit = max(1, int(limit))
    return run_query(
        f"""
        SELECT
            product_category_name,
            SUM(item_total_value) AS revenue
        FROM {SALES_TABLE}
        WHERE product_category_name IS NOT NULL
        GROUP BY product_category_name
        ORDER BY revenue DESC
        LIMIT {limit}
        """
    )


def get_overview_uncategorized_sales() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            COUNT(*) AS uncategorized_items,
            COUNT(DISTINCT order_id) AS affected_orders,
            COALESCE(SUM(item_total_value), 0) AS uncategorized_revenue
        FROM {SALES_TABLE}
        WHERE product_category_name IS NULL
        """
    )


def get_overview_orders_by_status() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            order_status,
            COUNT(DISTINCT order_id) AS order_count
        FROM {SALES_TABLE}
        GROUP BY order_status
        ORDER BY order_count DESC
        """
    )


def get_overview_revenue_by_seller_state() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            seller_state,
            SUM(item_total_value) AS revenue
        FROM {SALES_TABLE}
        WHERE seller_state IS NOT NULL
        GROUP BY seller_state
        ORDER BY revenue DESC
        """
    )


def get_sales_kpis(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )

    return run_query(
        f"""
        WITH filtered_sales AS (
            SELECT order_id, item_total_value, freight_value
            FROM {SALES_TABLE}
            {where_clause}
        ),
        order_totals AS (
            SELECT order_id, SUM(item_total_value) AS order_total
            FROM filtered_sales
            GROUP BY order_id
        )
        SELECT
            (SELECT COALESCE(SUM(item_total_value), 0) FROM filtered_sales) AS revenue,
            (SELECT COUNT(DISTINCT order_id) FROM filtered_sales) AS orders,
            (SELECT COUNT(*) FROM filtered_sales) AS items,
            (SELECT COALESCE(AVG(order_total), 0) FROM order_totals) AS average_order_value,
            (SELECT COALESCE(AVG(item_total_value), 0) FROM filtered_sales) AS average_item_value,
            (SELECT COALESCE(SUM(freight_value), 0) FROM filtered_sales) AS freight_revenue
        """
    )


def get_sales_revenue_over_time(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )
    return run_query(
        f"""
        SELECT
            DATE_TRUNC('month', order_purchase_timestamp) AS purchase_month,
            SUM(item_total_value) AS revenue
        FROM {SALES_TABLE}
        {where_clause}
        GROUP BY purchase_month
        ORDER BY purchase_month
        """
    )


def get_sales_revenue_by_category(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
    limit: int = 15,
) -> pd.DataFrame:
    limit = max(1, int(limit))
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )

    category_filter = "product_category_name IS NOT NULL"
    if where_clause:
        where_clause = where_clause + f" AND {category_filter}"
    else:
        where_clause = f"WHERE {category_filter}"

    return run_query(
        f"""
        SELECT
            product_category_name,
            SUM(item_total_value) AS revenue
        FROM {SALES_TABLE}
        {where_clause}
        GROUP BY product_category_name
        ORDER BY revenue DESC
        LIMIT {limit}
        """
    )


def get_sales_uncategorized_summary(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )

    if where_clause:
        where_clause = where_clause + " AND product_category_name IS NULL"
    else:
        where_clause = "WHERE product_category_name IS NULL"

    return run_query(
        f"""
        SELECT
            COUNT(*) AS uncategorized_items,
            COUNT(DISTINCT order_id) AS affected_orders,
            COALESCE(SUM(item_total_value), 0) AS uncategorized_revenue
        FROM {SALES_TABLE}
        {where_clause}
        """
    )


def get_sales_revenue_by_seller_state(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )

    state_filter = "seller_state IS NOT NULL"
    if where_clause:
        where_clause = where_clause + f" AND {state_filter}"
    else:
        where_clause = f"WHERE {state_filter}"

    return run_query(
        f"""
        SELECT
            seller_state,
            SUM(item_total_value) AS revenue
        FROM {SALES_TABLE}
        {where_clause}
        GROUP BY seller_state
        ORDER BY revenue DESC
        """
    )


def get_sales_top_sellers(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
    limit: int = 10,
) -> pd.DataFrame:
    limit = max(1, int(limit))
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )

    return run_query(
        f"""
        SELECT
            seller_id,
            SUM(item_total_value) AS revenue,
            COUNT(DISTINCT order_id) AS orders
        FROM {SALES_TABLE}
        {where_clause}
        GROUP BY seller_id
        ORDER BY revenue DESC
        LIMIT {limit}
        """
    )


def get_sales_order_status_distribution(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )

    return run_query(
        f"""
        SELECT
            order_status,
            COUNT(DISTINCT order_id) AS order_count
        FROM {SALES_TABLE}
        {where_clause}
        GROUP BY order_status
        ORDER BY order_count DESC
        """
    )


def get_sales_detail(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
    seller_states: Iterable[str] | None = None,
    product_categories: Iterable[str] | None = None,
    limit: int = 25,
    offset: int = 0,
) -> pd.DataFrame:
    limit = max(1, int(limit))
    offset = max(0, int(offset))
    where_clause = build_sales_where_clause(
        start_date,
        end_date,
        order_statuses,
        seller_states,
        product_categories,
    )

    return run_query(
        f"""
        SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            order_purchase_timestamp,
            order_status,
            product_category_name,
            seller_state,
            price,
            freight_value,
            item_total_value
        FROM {SALES_TABLE}
        {where_clause}
        ORDER BY order_purchase_timestamp DESC, order_id, order_item_id
        LIMIT {limit}
        OFFSET {offset}
        """
    )


def get_customer_filter_options() -> dict[str, list[str]]:
    states_df = run_query(
        f"""
        SELECT DISTINCT customer_state
        FROM {CUSTOMERS_TABLE}
        WHERE customer_state IS NOT NULL
        ORDER BY customer_state
        """
    )
    return {"customer_states": states_df["customer_state"].tolist()}


def get_customer_kpis(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
) -> pd.DataFrame:
    where_clause = build_customer_where_clause(
        customer_states,
        minimum_total_spend,
        minimum_total_orders,
    )

    return run_query(
        f"""
        WITH filtered_customers AS (
            SELECT total_spend, total_orders, average_order_value, average_review_score
            FROM {CUSTOMERS_TABLE}
            {where_clause}
        )
        SELECT
            COUNT(*) AS total_customers,
            COALESCE(SUM(total_spend), 0) AS total_customer_spend,
            COALESCE(AVG(total_spend), 0) AS average_customer_spend,
            COALESCE(AVG(total_orders), 0) AS average_orders_per_customer,
            COALESCE(AVG(average_order_value), 0) AS average_customer_order_value,
            COALESCE(AVG(average_review_score), 0) AS average_review_score
        FROM filtered_customers
        """
    )


def get_customer_count_by_state(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
) -> pd.DataFrame:
    where_clause = build_customer_where_clause(
        customer_states,
        minimum_total_spend,
        minimum_total_orders,
    )
    return run_query(
        f"""
        SELECT
            customer_state,
            COUNT(*) AS customer_count
        FROM {CUSTOMERS_TABLE}
        {where_clause}
        GROUP BY customer_state
        ORDER BY customer_count DESC
        """
    )


def get_customer_spend_by_state(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
) -> pd.DataFrame:
    where_clause = build_customer_where_clause(
        customer_states,
        minimum_total_spend,
        minimum_total_orders,
    )
    return run_query(
        f"""
        SELECT
            customer_state,
            SUM(total_spend) AS total_spend
        FROM {CUSTOMERS_TABLE}
        {where_clause}
        GROUP BY customer_state
        ORDER BY total_spend DESC
        """
    )


def get_customer_order_frequency_distribution(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
) -> pd.DataFrame:
    where_clause = build_customer_where_clause(
        customer_states,
        minimum_total_spend,
        minimum_total_orders,
    )
    return run_query(
        f"""
        SELECT
            total_orders,
            COUNT(*) AS customer_count
        FROM {CUSTOMERS_TABLE}
        {where_clause}
        GROUP BY total_orders
        ORDER BY total_orders
        """
    )


def get_customer_spending_distribution(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
) -> pd.DataFrame:
    where_clause = build_customer_where_clause(
        customer_states,
        minimum_total_spend,
        minimum_total_orders,
    )
    return run_query(
        f"""
        WITH filtered_customers AS (
            SELECT total_spend
            FROM {CUSTOMERS_TABLE}
            {where_clause}
        ),
        bucketed_customers AS (
            SELECT
                CASE
                    WHEN total_spend < 100 THEN '0-99'
                    WHEN total_spend < 250 THEN '100-249'
                    WHEN total_spend < 500 THEN '250-499'
                    WHEN total_spend < 1000 THEN '500-999'
                    WHEN total_spend < 2000 THEN '1,000-1,999'
                    ELSE '2,000+'
                END AS spend_bucket,
                CASE
                    WHEN total_spend < 100 THEN 1
                    WHEN total_spend < 250 THEN 2
                    WHEN total_spend < 500 THEN 3
                    WHEN total_spend < 1000 THEN 4
                    WHEN total_spend < 2000 THEN 5
                    ELSE 6
                END AS bucket_order
            FROM filtered_customers
        )
        SELECT
            spend_bucket,
            COUNT(*) AS customer_count,
            bucket_order
        FROM bucketed_customers
        GROUP BY spend_bucket, bucket_order
        ORDER BY bucket_order
        """
    )


def get_new_customers_over_time(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
) -> pd.DataFrame:
    where_clause = build_customer_where_clause(
        customer_states,
        minimum_total_spend,
        minimum_total_orders,
    )
    if where_clause:
        where_clause = where_clause + " AND first_order_date IS NOT NULL"
    else:
        where_clause = "WHERE first_order_date IS NOT NULL"

    return run_query(
        f"""
        SELECT
            DATE_TRUNC('month', first_order_date) AS first_order_month,
            COUNT(*) AS new_customers
        FROM {CUSTOMERS_TABLE}
        {where_clause}
        GROUP BY first_order_month
        ORDER BY first_order_month
        """
    )


def get_customer_detail(
    customer_states: Iterable[str] | None = None,
    minimum_total_spend: float | int | None = None,
    minimum_total_orders: int | None = None,
    limit: int = 25,
    offset: int = 0,
) -> pd.DataFrame:
    limit = max(1, int(limit))
    offset = max(0, int(offset))
    where_clause = build_customer_where_clause(
        customer_states,
        minimum_total_spend,
        minimum_total_orders,
    )
    return run_query(
        f"""
        SELECT
            customer_unique_id,
            customer_city,
            customer_state,
            first_order_date,
            last_order_date,
            total_orders,
            total_items,
            total_spend,
            average_order_value,
            total_reviews,
            average_review_score
        FROM {CUSTOMERS_TABLE}
        {where_clause}
        ORDER BY total_spend DESC, total_orders DESC
        LIMIT {limit}
        OFFSET {offset}
        """
    )


def get_delivery_date_bounds() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            MIN(DATE(order_purchase_timestamp)) AS min_date,
            MAX(DATE(order_purchase_timestamp)) AS max_date
        FROM {DELIVERIES_TABLE}
        """
    )


def get_delivery_filter_options() -> dict[str, list[str]]:
    status_df = run_query(
        f"""
        SELECT DISTINCT order_status
        FROM {DELIVERIES_TABLE}
        WHERE order_status IS NOT NULL
        ORDER BY order_status
        """
    )
    return {"order_statuses": status_df["order_status"].tolist()}


def get_delivery_kpis(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_delivery_where_clause(start_date, end_date, order_statuses)
    return run_query(
        f"""
        WITH filtered_deliveries AS (
            SELECT delivery_days, days_late
            FROM {DELIVERIES_TABLE}
            {where_clause}
        )
        SELECT
            COUNT(*) AS total_orders,
            COALESCE(AVG(delivery_days), 0) AS average_delivery_days,
            PERCENTILE_APPROX(delivery_days, 0.5) AS median_delivery_days,
            COALESCE(AVG(days_late), 0) AS average_days_late,
            SUM(CASE WHEN days_late > 0 THEN 1 ELSE 0 END) AS late_orders,
            SUM(CASE WHEN days_late = 0 THEN 1 ELSE 0 END) AS on_time_orders,
            SUM(CASE WHEN days_late < 0 THEN 1 ELSE 0 END) AS early_orders
        FROM filtered_deliveries
        """
    )


def get_delivery_time_distribution(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_delivery_where_clause(start_date, end_date, order_statuses)
    return run_query(
        f"""
        WITH filtered_deliveries AS (
            SELECT delivery_days
            FROM {DELIVERIES_TABLE}
            {where_clause}
        )
        SELECT
            CAST(FLOOR(delivery_days / 2) * 2 AS INT) AS bucket_start,
            CAST(FLOOR(delivery_days / 2) * 2 + 1 AS INT) AS bucket_end,
            COUNT(*) AS order_count
        FROM filtered_deliveries
        WHERE delivery_days IS NOT NULL
        GROUP BY bucket_start, bucket_end
        ORDER BY bucket_start
        """
    )


def get_delivery_status_distribution(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_delivery_where_clause(start_date, end_date, order_statuses)
    return run_query(
        f"""
        WITH filtered_deliveries AS (
            SELECT days_late
            FROM {DELIVERIES_TABLE}
            {where_clause}
        )
        SELECT
            delivery_timing,
            order_count,
            timing_order
        FROM (
            SELECT
                CASE
                    WHEN days_late < 0 THEN 'Early'
                    WHEN days_late = 0 THEN 'On time'
                    WHEN days_late > 0 THEN 'Late'
                END AS delivery_timing,
                COUNT(*) AS order_count,
                CASE
                    WHEN days_late < 0 THEN 1
                    WHEN days_late = 0 THEN 2
                    WHEN days_late > 0 THEN 3
                END AS timing_order
            FROM filtered_deliveries
            WHERE days_late IS NOT NULL
            GROUP BY
                CASE
                    WHEN days_late < 0 THEN 'Early'
                    WHEN days_late = 0 THEN 'On time'
                    WHEN days_late > 0 THEN 'Late'
                END,
                CASE
                    WHEN days_late < 0 THEN 1
                    WHEN days_late = 0 THEN 2
                    WHEN days_late > 0 THEN 3
                END
        ) distribution
        ORDER BY timing_order
        """
    )


def get_average_delivery_days_over_time(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_delivery_where_clause(start_date, end_date, order_statuses)
    if where_clause:
        where_clause = where_clause + " AND delivery_days IS NOT NULL"
    else:
        where_clause = "WHERE delivery_days IS NOT NULL"

    return run_query(
        f"""
        SELECT
            DATE_TRUNC('month', order_purchase_timestamp) AS purchase_month,
            AVG(delivery_days) AS average_delivery_days
        FROM {DELIVERIES_TABLE}
        {where_clause}
        GROUP BY purchase_month
        ORDER BY purchase_month
        """
    )


def get_average_days_late_by_seller_state(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_delivery_where_clause(start_date, end_date, order_statuses)
    return run_query(
        f"""
        WITH filtered_deliveries AS (
            SELECT order_id, days_late
            FROM {DELIVERIES_TABLE}
            {where_clause}
        ),
        seller_orders AS (
            SELECT DISTINCT order_id, seller_state
            FROM {SALES_TABLE}
            WHERE seller_state IS NOT NULL
        )
        SELECT
            seller_orders.seller_state,
            AVG(filtered_deliveries.days_late) AS average_days_late,
            COUNT(*) AS order_seller_pairs
        FROM filtered_deliveries
        INNER JOIN seller_orders
            ON filtered_deliveries.order_id = seller_orders.order_id
        WHERE filtered_deliveries.days_late IS NOT NULL
        GROUP BY seller_orders.seller_state
        ORDER BY average_days_late DESC
        """
    )


def get_late_orders_by_month(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_delivery_where_clause(start_date, end_date, order_statuses)
    if where_clause:
        where_clause = where_clause + " AND days_late > 0 AND order_delivered_customer_date IS NOT NULL"
    else:
        where_clause = "WHERE days_late > 0 AND order_delivered_customer_date IS NOT NULL"

    return run_query(
        f"""
        SELECT
            DATE_TRUNC('month', order_delivered_customer_date) AS delivered_month,
            COUNT(*) AS late_orders
        FROM {DELIVERIES_TABLE}
        {where_clause}
        GROUP BY delivered_month
        ORDER BY delivered_month
        """
    )


def get_delivery_quality_flags(
    start_date: date | None = None,
    end_date: date | None = None,
    order_statuses: Iterable[str] | None = None,
) -> pd.DataFrame:
    where_clause = build_delivery_where_clause(start_date, end_date, order_statuses)
    return run_query(
        f"""
        WITH filtered_deliveries AS (
            SELECT
                has_timestamp_anomaly,
                missing_delivery_timestamp,
                missing_carrier_timestamp,
                missing_estimated_delivery_date
            FROM {DELIVERIES_TABLE}
            {where_clause}
        )
        SELECT
            SUM(CASE WHEN has_timestamp_anomaly THEN 1 ELSE 0 END) AS timestamp_anomalies,
            SUM(CASE WHEN missing_delivery_timestamp THEN 1 ELSE 0 END) AS missing_delivery_timestamp,
            SUM(CASE WHEN missing_carrier_timestamp THEN 1 ELSE 0 END) AS missing_carrier_timestamp,
            SUM(CASE WHEN missing_estimated_delivery_date THEN 1 ELSE 0 END) AS missing_estimated_delivery_date
        FROM filtered_deliveries
        """
    )


def get_gold_table_counts() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT 'sales' AS table_name, COUNT(*) AS row_count FROM {SALES_TABLE}
        UNION ALL
        SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM {CUSTOMERS_TABLE}
        UNION ALL
        SELECT 'deliveries' AS table_name, COUNT(*) AS row_count FROM {DELIVERIES_TABLE}
        """
    )


def get_gold_freshness() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT 'sales' AS table_name, MAX(order_purchase_timestamp) AS latest_business_timestamp FROM {SALES_TABLE}
        UNION ALL
        SELECT 'customers' AS table_name, MAX(last_order_date) AS latest_business_timestamp FROM {CUSTOMERS_TABLE}
        UNION ALL
        SELECT 'deliveries' AS table_name, MAX(order_purchase_timestamp) AS latest_business_timestamp FROM {DELIVERIES_TABLE}
        """
    )


def get_gold_null_counts() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT 'sales' AS table_name, 'product_category_name' AS field_name, SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) AS null_count FROM {SALES_TABLE}
        UNION ALL
        SELECT 'sales' AS table_name, 'seller_state' AS field_name, SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) AS null_count FROM {SALES_TABLE}
        UNION ALL
        SELECT 'customers' AS table_name, 'customer_state' AS field_name, SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) AS null_count FROM {CUSTOMERS_TABLE}
        UNION ALL
        SELECT 'customers' AS table_name, 'first_order_date' AS field_name, SUM(CASE WHEN first_order_date IS NULL THEN 1 ELSE 0 END) AS null_count FROM {CUSTOMERS_TABLE}
        UNION ALL
        SELECT 'customers' AS table_name, 'average_review_score' AS field_name, SUM(CASE WHEN average_review_score IS NULL THEN 1 ELSE 0 END) AS null_count FROM {CUSTOMERS_TABLE}
        UNION ALL
        SELECT 'deliveries' AS table_name, 'order_delivered_customer_date' AS field_name, SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_count FROM {DELIVERIES_TABLE}
        UNION ALL
        SELECT 'deliveries' AS table_name, 'order_estimated_delivery_date' AS field_name, SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) AS null_count FROM {DELIVERIES_TABLE}
        UNION ALL
        SELECT 'deliveries' AS table_name, 'days_late' AS field_name, SUM(CASE WHEN days_late IS NULL THEN 1 ELSE 0 END) AS null_count FROM {DELIVERIES_TABLE}
        ORDER BY table_name, field_name
        """
    )


def get_pipeline_quality_indicators() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            (SELECT SUM(CASE WHEN has_timestamp_anomaly THEN 1 ELSE 0 END) FROM {DELIVERIES_TABLE}) AS timestamp_anomalies,
            (SELECT SUM(CASE WHEN missing_delivery_timestamp THEN 1 ELSE 0 END) FROM {DELIVERIES_TABLE}) AS missing_delivery_timestamp,
            (SELECT SUM(CASE WHEN missing_carrier_timestamp THEN 1 ELSE 0 END) FROM {DELIVERIES_TABLE}) AS missing_carrier_timestamp,
            (SELECT SUM(CASE WHEN missing_estimated_delivery_date THEN 1 ELSE 0 END) FROM {DELIVERIES_TABLE}) AS missing_estimated_delivery_date,
            (SELECT SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) FROM {SALES_TABLE}) AS uncategorized_items,
            (SELECT COUNT(DISTINCT order_id) FROM {SALES_TABLE} WHERE product_category_name IS NULL) AS uncategorized_orders
        """
    )


