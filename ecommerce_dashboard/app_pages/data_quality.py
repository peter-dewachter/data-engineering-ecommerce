import pandas as pd
import streamlit as st
from ecommerce_dashboard.components import (
    format_number,
    normalize_datetime_column,
    render_metric_grid,
    render_query_error,
)
from ecommerce_dashboard.db import DatabricksConfigurationError, DatabricksQueryError
from ecommerce_dashboard.queries import (
    get_gold_freshness,
    get_gold_null_counts,
    get_gold_table_counts,
    get_pipeline_quality_indicators,
)

def render():
    st.title("Data quality")
    st.caption(
        "This page highlights the data-engineering pipeline and the quality signals already exposed by the curated Gold layer."
    )

    st.markdown(
        """
        The application normally consumes **Gold** data only. This page surfaces quality indicators that were preserved by the pipeline so the portfolio demonstrates both analytics and engineering discipline.
        """
    )

    layer_overview = pd.DataFrame(
        [
            {"Layer": "Bronze", "Purpose": "Raw source data ingested from the Olist dataset."},
            {"Layer": "Silver", "Purpose": "Standardized and validated entities with quality checks applied."},
            {"Layer": "Gold", "Purpose": "Business-facing analytical tables used by the Streamlit application."},
            {"Layer": "Streamlit", "Purpose": "Consumption and visualization layer on top of the curated Gold tables."},
        ]
    )

    with st.container(border=True):
        st.subheader("Pipeline layers")
        st.table(layer_overview)

    try:
        table_counts = get_gold_table_counts()
        freshness = normalize_datetime_column(get_gold_freshness(), "latest_business_timestamp")
        null_counts = get_gold_null_counts()
        quality_indicators = get_pipeline_quality_indicators()
    except (DatabricksConfigurationError, DatabricksQueryError) as error:
        render_query_error(error)
        st.stop()

    quality_row = quality_indicators.iloc[0]
    render_metric_grid(
        [
            (
                "Timestamp anomalies",
                format_number(quality_row["timestamp_anomalies"]),
                "Orders flagged with anomalous timestamps in the deliveries Gold table.",
            ),
            (
                "Missing delivery timestamps",
                format_number(quality_row["missing_delivery_timestamp"]),
                "Orders missing order_delivered_customer_date in the deliveries Gold table.",
            ),
            (
                "Missing carrier timestamps",
                format_number(quality_row["missing_carrier_timestamp"]),
                "Orders missing order_delivered_carrier_date in the deliveries Gold table.",
            ),
            (
                "Uncategorized items",
                format_number(quality_row["uncategorized_items"]),
                "Sales records with null product_category_name in the sales Gold table.",
            ),
            (
                "Orders with uncategorized items",
                format_number(quality_row["uncategorized_orders"]),
                "Distinct orders affected by uncategorized products.",
            ),
            (
                "Missing estimated delivery dates",
                format_number(quality_row["missing_estimated_delivery_date"]),
                "Orders missing order_estimated_delivery_date in the deliveries Gold table.",
            ),
        ],
        columns_per_row=3,
    )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            st.subheader("Gold table row counts")
            st.dataframe(table_counts, hide_index=True)

    with right_col:
        with st.container(border=True):
            st.subheader("Gold freshness indicators")
            freshness_display = freshness.copy()
            freshness_display["latest_business_timestamp"] = pd.to_datetime(
                freshness_display["latest_business_timestamp"]
            ).dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(freshness_display, hide_index=True)
            st.caption(
                "Freshness is shown as the latest business timestamp available in each Gold table."
            )

    with st.container(border=True):
        st.subheader("Null counts for important business fields")
        st.dataframe(null_counts, hide_index=True)

    with st.container(border=True):
        st.subheader("What this shows about the project")
        st.markdown(
            """
            - The analytics app does **not** rebuild Bronze or Silver transformations.
            - Databricks remains the source of truth for business logic and quality checks.
            - Gold tables expose both business metrics and operational quality indicators.
            - Streamlit acts as a secure presentation layer over curated SQL outputs.
            """
        )
