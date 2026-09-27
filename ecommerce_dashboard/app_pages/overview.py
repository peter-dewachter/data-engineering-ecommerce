import streamlit as st
from ecommerce_dashboard.components import (
    format_currency,
    format_days,
    format_number,
    normalize_datetime_column,
    render_metric_grid,
    render_query_error,
)
from ecommerce_dashboard.db import DatabricksConfigurationError, DatabricksQueryError
from ecommerce_dashboard.queries import (
    get_overview_kpis,
    get_overview_orders_by_status,
    get_overview_revenue_by_category,
    get_overview_revenue_by_seller_state,
    get_overview_revenue_over_time,
    get_overview_uncategorized_sales,
)


def render():
    st.title("Overview")
    st.caption(
        "Business-facing summary built entirely on the Gold layer: "
        "`ecommerce.gold.sales`, `ecommerce.gold.customers`, and `ecommerce.gold.deliveries`."
    )

    with st.expander("Business definitions", expanded=False):
        st.markdown(
            """
            - **Revenue** = `SUM(item_total_value)`
            - **Orders** = `COUNT(DISTINCT order_id)`
            - **Items** = `COUNT(*)` because `gold.sales` is one row per order item
            - **Customers** = row count of `gold.customers`, where the grain is one row per `customer_unique_id`
            - **Average order value** is calculated at the order grain, not as an average item value
            - **Average delivery days** uses valid non-null `delivery_days` values from `gold.deliveries`
            """
        )

    try:
        kpi_df = get_overview_kpis()
        revenue_over_time = normalize_datetime_column(
            get_overview_revenue_over_time(), "purchase_month"
        )
        revenue_by_category = get_overview_revenue_by_category(limit=10)
        orders_by_status = get_overview_orders_by_status()
        revenue_by_seller_state = get_overview_revenue_by_seller_state()
        uncategorized_sales = get_overview_uncategorized_sales()
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    kpi_row = kpi_df.iloc[0]
    render_metric_grid(
        [
            (
                "Total revenue",
                format_currency(kpi_row["total_revenue"]),
                "Sum of item_total_value across the sales Gold table.",
            ),
            (
                "Total orders",
                format_number(kpi_row["total_orders"]),
                "Distinct order count from ecommerce.gold.sales.",
            ),
            (
                "Total items",
                format_number(kpi_row["total_items"]),
                "Row count from ecommerce.gold.sales at order-item grain.",
            ),
            (
                "Total customers",
                format_number(kpi_row["total_customers"]),
                "Customer count from ecommerce.gold.customers.",
            ),
            (
                "Average order value",
                format_currency(kpi_row["average_order_value"]),
                "Average of order-level totals after grouping sales by order_id.",
            ),
            (
                "Average delivery days",
                format_days(kpi_row["average_delivery_days"]),
                "Average delivery_days from ecommerce.gold.deliveries.",
            ),
        ],
        columns_per_row=3,
    )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Revenue over time")
            if revenue_over_time.empty:
                st.info("No revenue trend data is available.")
            else:
                st.line_chart(
                    revenue_over_time,
                    x="purchase_month",
                    y="revenue",
                    x_label="Purchase month",
                    y_label="Revenue",
                )

    with right_col:
        with st.container(border=True):
            st.subheader("Revenue by product category")
            if revenue_by_category.empty:
                st.info("No category revenue data is available.")
            else:
                st.bar_chart(
                    revenue_by_category,
                    x="product_category_name",
                    y="revenue",
                    horizontal=True,
                    x_label="Revenue",
                    y_label="Product category",
                )
                uncategorized_row = uncategorized_sales.iloc[0]
                if int(uncategorized_row["uncategorized_items"] or 0) > 0:
                    st.caption(
                        "Uncategorized records exist in the source Gold table: "
                        f"{format_number(uncategorized_row['uncategorized_items'])} items across "
                        f"{format_number(uncategorized_row['affected_orders'])} orders, worth "
                        f"{format_currency(uncategorized_row['uncategorized_revenue'])}."
                    )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Orders by order status")
            if orders_by_status.empty:
                st.info("No order status data is available.")
            else:
                st.bar_chart(
                    orders_by_status,
                    x="order_status",
                    y="order_count",
                    x_label="Order status",
                    y_label="Orders",
                )

    with right_col:
        with st.container(border=True):
            st.subheader("Revenue by seller state")
            if revenue_by_seller_state.empty:
                st.info("No seller geography data is available.")
            else:
                st.bar_chart(
                    revenue_by_seller_state,
                    x="seller_state",
                    y="revenue",
                    x_label="Seller state",
                    y_label="Revenue",
                )


