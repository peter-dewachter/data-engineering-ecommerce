import pandas as pd
import streamlit as st
from ecommerce_dashboard.components import (
    format_days,
    format_number,
    normalize_datetime_column,
    render_metric_grid,
    render_query_error,
)
from ecommerce_dashboard.db import DatabricksConfigurationError, DatabricksQueryError
from ecommerce_dashboard.queries import (
    get_average_days_late_by_seller_state,
    get_average_delivery_days_over_time,
    get_delivery_date_bounds,
    get_delivery_filter_options,
    get_delivery_kpis,
    get_delivery_quality_flags,
    get_delivery_status_distribution,
    get_delivery_time_distribution,
    get_late_orders_by_month,
)

def render():
    st.title("Deliveries")
    st.caption(
        "Operational delivery dashboard built on `ecommerce.gold.deliveries`, with seller-state context joined from `ecommerce.gold.sales` only where required."
    )

    with st.expander("Delivery definitions", expanded=False):
        st.markdown(
            """
            - **Early**: `days_late < 0`
            - **On time**: `days_late = 0`
            - **Late**: `days_late > 0`
            - Null `days_late` values are excluded from early/on-time/late classification.
            - Seller-state delivery analysis uses distinct `(order_id, seller_state)` pairs from `gold.sales` to associate delivery outcomes with seller geography.
            """
        )

    # Initialize session state for filter application
    if "delivery_filters_applied" not in st.session_state:
        st.session_state.delivery_filters_applied = False

    try:
        date_bounds = get_delivery_date_bounds()
        filter_options = get_delivery_filter_options()
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    bounds_row = date_bounds.iloc[0]
    default_start = pd.to_datetime(bounds_row["min_date"]).date()
    default_end = pd.to_datetime(bounds_row["max_date"]).date()

    # Display filters in main content
    st.subheader("Query filters")
    st.caption("Leave order status empty to include all statuses.")

    with st.form("delivery_filters"):
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
        if st.form_submit_button("Apply filters", type="primary"):
            st.session_state.delivery_filters_applied = True

    # Only show results if filters have been applied
    if not st.session_state.delivery_filters_applied:
        st.info("Configure filters above and click **Apply filters** to view analytics.")
        st.stop()

    if isinstance(selected_date_range, (list, tuple)) and len(selected_date_range) == 2:
        start_date, end_date = selected_date_range
    else:
        start_date, end_date = default_start, default_end

    try:
        kpi_df = get_delivery_kpis(start_date, end_date, selected_statuses)
        delivery_time_distribution = get_delivery_time_distribution(
            start_date,
            end_date,
            selected_statuses,
        )
        delivery_status_distribution = get_delivery_status_distribution(
            start_date,
            end_date,
            selected_statuses,
        )
        average_delivery_days_over_time = normalize_datetime_column(
            get_average_delivery_days_over_time(start_date, end_date, selected_statuses),
            "purchase_month",
        )
        average_days_late_by_seller_state = get_average_days_late_by_seller_state(
            start_date,
            end_date,
            selected_statuses,
        )
        late_orders_by_month = normalize_datetime_column(
            get_late_orders_by_month(start_date, end_date, selected_statuses),
            "delivered_month",
        )
        delivery_quality_flags = get_delivery_quality_flags(
            start_date,
            end_date,
            selected_statuses,
        )
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    kpi_row = kpi_df.iloc[0]
    render_metric_grid(
        [
            (
                "Total orders",
                format_number(kpi_row["total_orders"]),
                "Filtered order count from ecommerce.gold.deliveries.",
            ),
            (
                "Average delivery days",
                format_days(kpi_row["average_delivery_days"]),
                "Average non-null delivery_days for the filtered orders.",
            ),
            (
                "Median delivery days",
                format_days(kpi_row["median_delivery_days"]),
                "Median non-null delivery_days for the filtered orders.",
            ),
            (
                "Average days late",
                format_days(kpi_row["average_days_late"]),
                "Average days_late across filtered orders with valid timing data.",
            ),
            (
                "Late orders",
                format_number(kpi_row["late_orders"]),
                "Orders where days_late > 0.",
            ),
            (
                "On-time orders",
                format_number(kpi_row["on_time_orders"]),
                "Orders where days_late = 0.",
            ),
            (
                "Early orders",
                format_number(kpi_row["early_orders"]),
                "Orders where days_late < 0.",
            ),
        ],
        columns_per_row=3,
    )

    quality_row = delivery_quality_flags.iloc[0]
    with st.container(border=True):
        st.subheader("Operational data-quality indicators")
        render_metric_grid(
            [
                (
                    "Timestamp anomalies",
                    format_number(quality_row["timestamp_anomalies"]),
                    "Orders flagged by the pipeline with timestamp anomalies.",
                ),
                (
                    "Missing delivery timestamp",
                    format_number(quality_row["missing_delivery_timestamp"]),
                    "Orders where order_delivered_customer_date is missing.",
                ),
                (
                    "Missing carrier timestamp",
                    format_number(quality_row["missing_carrier_timestamp"]),
                    "Orders where order_delivered_carrier_date is missing.",
                ),
                (
                    "Missing estimated delivery date",
                    format_number(quality_row["missing_estimated_delivery_date"]),
                    "Orders where order_estimated_delivery_date is missing.",
                ),
            ],
            columns_per_row=4,
        )

    if not delivery_time_distribution.empty:
        delivery_time_distribution = delivery_time_distribution.copy()
        delivery_time_distribution["delivery_day_band"] = delivery_time_distribution.apply(
            lambda row: f"{int(row['bucket_start'])}-{int(row['bucket_end'])} days",
            axis=1,
        )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Delivery time distribution")
            if delivery_time_distribution.empty:
                st.info("No delivery duration values matched the current filters.")
            else:
                st.bar_chart(
                    delivery_time_distribution,
                    x="delivery_day_band",
                    y="order_count",
                    x_label="Delivery day band",
                    y_label="Orders",
                )

    with right_col:
        with st.container(border=True):
            st.subheader("Early, on-time, and late distribution")
            if delivery_status_distribution.empty:
                st.info("No days_late values matched the current filters.")
            else:
                st.bar_chart(
                    delivery_status_distribution,
                    x="delivery_timing",
                    y="order_count",
                    x_label="Delivery timing",
                    y_label="Orders",
                )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Average delivery days over time")
            if average_delivery_days_over_time.empty:
                st.info("No delivery trend data matched the current filters.")
            else:
                st.line_chart(
                    average_delivery_days_over_time,
                    x="purchase_month",
                    y="average_delivery_days",
                    x_label="Purchase month",
                    y_label="Average delivery days",
                )

    with right_col:
        with st.container(border=True):
            st.subheader("Average days late by seller state")
            if average_days_late_by_seller_state.empty:
                st.info("No seller-state delivery view matched the current filters.")
            else:
                st.bar_chart(
                    average_days_late_by_seller_state,
                    x="seller_state",
                    y="average_days_late",
                    x_label="Seller state",
                    y_label="Average days late",
                )
                st.caption(
                    "This view associates delivery records with distinct order/seller-state pairs from the sales Gold table."
                )

    with st.container(border=True):
        st.subheader("Late orders by month")
        if late_orders_by_month.empty:
            st.info("No late delivered orders matched the current filters.")
        else:
            st.line_chart(
                late_orders_by_month,
                x="delivered_month",
                y="late_orders",
                x_label="Delivered month",
                y_label="Late orders",
            )
