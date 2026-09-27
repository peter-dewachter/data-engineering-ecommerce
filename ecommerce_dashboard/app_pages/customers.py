import pandas as pd
import streamlit as st
from ecommerce_dashboard.components import (
    dataframe_download_disabled_notice,
    format_currency,
    format_decimal,
    format_number,
    get_pagination_state,
    normalize_datetime_column,
    render_metric_grid,
    render_query_error,
)
from ecommerce_dashboard.db import DatabricksConfigurationError, DatabricksQueryError
from ecommerce_dashboard.queries import (
    get_customer_count_by_state,
    get_customer_detail,
    get_customer_filter_options,
    get_customer_kpis,
    get_customer_order_frequency_distribution,
    get_customer_spend_by_state,
    get_customer_spending_distribution,
    get_new_customers_over_time,
)

def render():
    st.title("Customers")
    st.caption(
        "Customer analytics from `ecommerce.gold.customers`, where each row represents one `customer_unique_id`."
    )

    # Initialize session state for filter application
    if "customer_filters_applied" not in st.session_state:
        st.session_state.customer_filters_applied = False

    try:
        filter_options = get_customer_filter_options()
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    # Display filters in main content
    st.subheader("Query filters")
    st.caption("Leave state empty to include all customers.")

    with st.form("customer_filters"):
        selected_states = st.multiselect(
            "Customer state",
            options=filter_options["customer_states"],
        )
        minimum_total_spend = st.number_input(
            "Minimum total spend",
            min_value=0.0,
            value=0.0,
            step=50.0,
        )
        minimum_total_orders = st.number_input(
            "Minimum total orders",
            min_value=0,
            value=0,
            step=1,
        )
        if st.form_submit_button("Apply filters", type="primary"):
            st.session_state.customer_filters_applied = True

    # Only show results if filters have been applied
    if not st.session_state.customer_filters_applied:
        st.info("Configure filters above and click **Apply filters** to view analytics.")
        st.stop()

    try:
        kpi_df = get_customer_kpis(
            selected_states,
            minimum_total_spend,
            minimum_total_orders,
        )
        customer_count_by_state = get_customer_count_by_state(
            selected_states,
            minimum_total_spend,
            minimum_total_orders,
        )
        customer_spend_by_state = get_customer_spend_by_state(
            selected_states,
            minimum_total_spend,
            minimum_total_orders,
        )
        order_frequency_distribution = get_customer_order_frequency_distribution(
            selected_states,
            minimum_total_spend,
            minimum_total_orders,
        )
        customer_spending_distribution = get_customer_spending_distribution(
            selected_states,
            minimum_total_spend,
            minimum_total_orders,
        )
        new_customers_over_time = normalize_datetime_column(
            get_new_customers_over_time(
                selected_states,
                minimum_total_spend,
                minimum_total_orders,
            ),
            "first_order_month",
        )
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    kpi_row = kpi_df.iloc[0]
    render_metric_grid(
        [
            (
                "Total customers",
                format_number(kpi_row["total_customers"]),
                "Filtered customer count at customer_unique_id grain.",
            ),
            (
                "Total customer spend",
                format_currency(kpi_row["total_customer_spend"]),
                "Sum of total_spend across filtered customers.",
            ),
            (
                "Average customer spend",
                format_currency(kpi_row["average_customer_spend"]),
                "Average total_spend per filtered customer.",
            ),
            (
                "Average orders per customer",
                format_decimal(kpi_row["average_orders_per_customer"]),
                "Average total_orders per filtered customer.",
            ),
            (
                "Average customer order value",
                format_currency(kpi_row["average_customer_order_value"]),
                "Average of the customer-level average_order_value metric.",
            ),
            (
                "Average review score",
                format_decimal(kpi_row["average_review_score"]),
                "Average of customer-level average_review_score.",
            ),
        ],
        columns_per_row=3,
    )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Customer count by state")
            if customer_count_by_state.empty:
                st.info("No customers matched the current filters.")
            else:
                st.bar_chart(
                    customer_count_by_state,
                    x="customer_state",
                    y="customer_count",
                    x_label="Customer state",
                    y_label="Customers",
                )

    with right_col:
        with st.container(border=True):
            st.subheader("Customer spend by state")
            if customer_spend_by_state.empty:
                st.info("No customer spend matched the current filters.")
            else:
                st.bar_chart(
                    customer_spend_by_state,
                    x="customer_state",
                    y="total_spend",
                    x_label="Customer state",
                    y_label="Total spend",
                )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Customer order frequency distribution")
            if order_frequency_distribution.empty:
                st.info("No order frequency data matched the current filters.")
            else:
                st.bar_chart(
                    order_frequency_distribution,
                    x="total_orders",
                    y="customer_count",
                    x_label="Orders per customer",
                    y_label="Customers",
                )

    with right_col:
        with st.container(border=True):
            st.subheader("Customer spending distribution")
            if customer_spending_distribution.empty:
                st.info("No spending distribution is available for the current filters.")
            else:
                st.bar_chart(
                    customer_spending_distribution[["spend_bucket", "customer_count"]],
                    x="spend_bucket",
                    y="customer_count",
                    x_label="Total spend band",
                    y_label="Customers",
                )
                st.caption("Spend distribution uses readable spend bands for presentation only.")

    with st.container(border=True):
        st.subheader("New customers over time")
        if new_customers_over_time.empty:
            st.info("No first-order dates matched the current filters.")
        else:
            st.line_chart(
                new_customers_over_time,
                x="first_order_month",
                y="new_customers",
                x_label="First order month",
                y_label="New customers",
            )

    st.subheader("Customer table")
    show_customer_table = st.toggle("Show customer table", value=False)

    if show_customer_table:
        total_rows = int(kpi_row["total_customers"] or 0)
        dataframe_download_disabled_notice()
        page_size, page_number, max_page, offset = get_pagination_state(
            total_rows,
            key_prefix="customer_detail",
        )

        try:
            customer_detail = get_customer_detail(
                selected_states,
                minimum_total_spend,
                minimum_total_orders,
                limit=page_size,
                offset=offset,
            )
        except (DatabricksConfigurationError, DatabricksQueryError) as error:
            render_query_error(error)
            st.stop()

        st.caption(
            f"Showing page {page_number} of {max_page} "
            f"({format_number(total_rows)} filtered customers)."
        )

        if customer_detail.empty:
            st.info("No customers matched the current filters.")
        else:
            customer_display = customer_detail.copy()
            for date_column in ["first_order_date", "last_order_date"]:
                customer_display[date_column] = pd.to_datetime(
                    customer_display[date_column]
                ).dt.strftime("%Y-%m-%d")

            for currency_column in ["total_spend", "average_order_value"]:
                customer_display[currency_column] = customer_display[currency_column].map(
                    format_currency
                )

            customer_display["average_review_score"] = customer_display[
                "average_review_score"
            ].map(format_decimal)

            st.dataframe(customer_display, hide_index=True)
