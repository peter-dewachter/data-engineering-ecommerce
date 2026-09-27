import pandas as pd
import streamlit as st
from ecommerce_dashboard.components import (
    dataframe_download_disabled_notice,
    format_currency,
    format_number,
    get_pagination_state,
    normalize_datetime_column,
    render_metric_grid,
    render_query_error,
)
from ecommerce_dashboard.db import DatabricksConfigurationError, DatabricksQueryError
from ecommerce_dashboard.queries import (
    get_sales_date_bounds,
    get_sales_detail,
    get_sales_filter_options,
    get_sales_kpis,
    get_sales_order_status_distribution,
    get_sales_revenue_by_category,
    get_sales_revenue_by_seller_state,
    get_sales_revenue_over_time,
    get_sales_top_sellers,
    get_sales_uncategorized_summary,
)

def render():
    st.title("Sales")
    st.caption(
        "Sales analytics from `ecommerce.gold.sales`, where each row represents one `(order_id, order_item_id)` record."
    )

    # Initialize session state for filter application
    if "sales_filters_applied" not in st.session_state:
        st.session_state.sales_filters_applied = False

    try:
        date_bounds = get_sales_date_bounds()
        filter_options = get_sales_filter_options()
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    bounds_row = date_bounds.iloc[0]
    default_start = pd.to_datetime(bounds_row["min_date"]).date()
    default_end = pd.to_datetime(bounds_row["max_date"]).date()

    # Display filters in main content
    st.subheader("Query filters")
    st.caption("Leave multiselects empty to include all values.")

    with st.form("sales_filters"):
        selected_date_range = st.date_input(
            "Purchase date range",
            value=(default_start, default_end),
            min_value=default_start,
            max_value=default_end,
        )
        selected_statuses = st.multiselect(
            "Order status",
            options=filter_options["order_statuses"],
        )
        selected_seller_states = st.multiselect(
            "Seller state",
            options=filter_options["seller_states"],
        )
        selected_categories = st.multiselect(
            "Product category",
            options=filter_options["product_categories"],
        )
        if st.form_submit_button("Apply filters", type="primary"):
            st.session_state.sales_filters_applied = True

    # Only show results if filters have been applied
    if not st.session_state.sales_filters_applied:
        st.info("Configure filters above and click **Apply filters** to view analytics.")
        st.stop()

    if isinstance(selected_date_range, (list, tuple)) and len(selected_date_range) == 2:
        start_date, end_date = selected_date_range
    else:
        start_date, end_date = default_start, default_end

    try:
        kpi_df = get_sales_kpis(
            start_date,
            end_date,
            selected_statuses,
            selected_seller_states,
            selected_categories,
        )
        revenue_over_time = normalize_datetime_column(
            get_sales_revenue_over_time(
                start_date,
                end_date,
                selected_statuses,
                selected_seller_states,
                selected_categories,
            ),
            "purchase_month",
        )
        revenue_by_category = get_sales_revenue_by_category(
            start_date,
            end_date,
            selected_statuses,
            selected_seller_states,
            selected_categories,
            limit=15,
        )
        uncategorized_summary = get_sales_uncategorized_summary(
            start_date,
            end_date,
            selected_statuses,
            selected_seller_states,
            selected_categories,
        )
        revenue_by_seller_state = get_sales_revenue_by_seller_state(
            start_date,
            end_date,
            selected_statuses,
            selected_seller_states,
            selected_categories,
        )
        top_sellers = get_sales_top_sellers(
            start_date,
            end_date,
            selected_statuses,
            selected_seller_states,
            selected_categories,
            limit=10,
        )
        order_status_distribution = get_sales_order_status_distribution(
            start_date,
            end_date,
            selected_statuses,
            selected_seller_states,
            selected_categories,
        )
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    kpi_row = kpi_df.iloc[0]
    render_metric_grid(
        [
            (
                "Revenue",
                format_currency(kpi_row["revenue"]),
                "Sum of item_total_value for the filtered sales records.",
            ),
            (
                "Orders",
                format_number(kpi_row["orders"]),
                "Distinct order count in the filtered sales slice.",
            ),
            (
                "Items",
                format_number(kpi_row["items"]),
                "Filtered row count at order-item grain.",
            ),
            (
                "Average order value",
                format_currency(kpi_row["average_order_value"]),
                "Average of order-level totals after grouping filtered rows by order_id.",
            ),
            (
                "Average item value",
                format_currency(kpi_row["average_item_value"]),
                "Average item_total_value across the filtered rows.",
            ),
            (
                "Freight revenue",
                format_currency(kpi_row["freight_revenue"]),
                "Sum of freight_value across the filtered sales records.",
            ),
        ],
        columns_per_row=3,
    )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Revenue over time")
            if revenue_over_time.empty:
                st.info("No filtered sales records matched the current selection.")
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
                st.info("No named product categories matched the current selection.")
            else:
                st.bar_chart(
                    revenue_by_category,
                    x="product_category_name",
                    y="revenue",
                    horizontal=True,
                    x_label="Revenue",
                    y_label="Product category",
                )
                uncategorized_row = uncategorized_summary.iloc[0]
                if int(uncategorized_row["uncategorized_items"] or 0) > 0:
                    st.caption(
                        "Uncategorized sales were excluded from this category chart only: "
                        f"{format_number(uncategorized_row['uncategorized_items'])} items across "
                        f"{format_number(uncategorized_row['affected_orders'])} orders, worth "
                        f"{format_currency(uncategorized_row['uncategorized_revenue'])}."
                    )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Revenue by seller state")
            if revenue_by_seller_state.empty:
                st.info("No seller state data matched the current filters.")
            else:
                st.bar_chart(
                    revenue_by_seller_state,
                    x="seller_state",
                    y="revenue",
                    x_label="Seller state",
                    y_label="Revenue",
                )

    with right_col:
        with st.container(border=True):
            st.subheader("Top sellers by revenue")
            if top_sellers.empty:
                st.info("No seller results matched the current filters.")
            else:
                st.bar_chart(
                    top_sellers,
                    x="seller_id",
                    y="revenue",
                    x_label="Seller ID",
                    y_label="Revenue",
                )

    with st.container(border=True):
        st.subheader("Order status distribution")
        if order_status_distribution.empty:
            st.info("No order status data matched the current filters.")
        else:
            st.bar_chart(
                order_status_distribution,
                x="order_status",
                y="order_count",
                x_label="Order status",
                y_label="Orders",
            )

    st.subheader("Sales detail table")
    show_details = st.toggle("Show sales detail table", value=False)

    if show_details:
        total_rows = int(kpi_row["items"] or 0)
        dataframe_download_disabled_notice()
        page_size, page_number, max_page, offset = get_pagination_state(
            total_rows,
            key_prefix="sales_detail",
        )

        try:
            detail_df = get_sales_detail(
                start_date,
                end_date,
                selected_statuses,
                selected_seller_states,
                selected_categories,
                limit=page_size,
                offset=offset,
            )
        except (DatabricksConfigurationError, DatabricksQueryError) as error:
            render_query_error(error)
            st.stop()

        st.caption(
            f"Showing page {page_number} of {max_page} "
            f"({format_number(total_rows)} filtered rows)."
        )

        if detail_df.empty:
            st.info("No detail rows matched the current filters.")
        else:
            detail_display = detail_df.copy()
            detail_display["order_purchase_timestamp"] = pd.to_datetime(
                detail_display["order_purchase_timestamp"]
            ).dt.strftime("%Y-%m-%d %H:%M:%S")
            for column_name in ["price", "freight_value", "item_total_value"]:
                detail_display[column_name] = detail_display[column_name].map(format_currency)

            st.dataframe(detail_display, hide_index=True)
