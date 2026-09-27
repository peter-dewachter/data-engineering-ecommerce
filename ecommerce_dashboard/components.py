from __future__ import annotations

import math
from collections.abc import Sequence

import pandas as pd
import streamlit as st

from ecommerce_dashboard.db import DatabricksConfigurationError


MetricDefinition = tuple[str, str, str | None]


def format_number(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{int(round(float(value))):,}"


def format_decimal(value, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.{decimals}f}"


def format_currency(value, currency_symbol: str = "R$") -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{currency_symbol}{float(value):,.2f}"


def format_days(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.1f} days"


def normalize_datetime_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if column_name in df.columns and not df.empty:
        df = df.copy()
        df[column_name] = pd.to_datetime(df[column_name])
    return df


def render_metric_grid(metrics: Sequence[MetricDefinition], columns_per_row: int = 3) -> None:
    if not metrics:
        return

    for index in range(0, len(metrics), columns_per_row):
        row = metrics[index : index + columns_per_row]
        columns = st.columns(len(row))
        for column, (label, value, help_text) in zip(columns, row):
            column.metric(label, value, help=help_text, border=True)


def render_query_error(error: Exception) -> None:
    if isinstance(error, DatabricksConfigurationError):
        st.error(str(error))
    else:
        st.error("Unable to retrieve data from Databricks. Please try again later.")


def get_pagination_state(total_rows: int, key_prefix: str) -> tuple[int, int, int, int]:
    total_rows = max(0, int(total_rows))
    selector_col, page_col = st.columns(2)

    with selector_col:
        page_size = st.selectbox(
            "Rows per page",
            options=[25, 50, 100],
            index=0,
            key=f"{key_prefix}_page_size",
        )

    max_page = max(1, math.ceil(total_rows / page_size)) if total_rows else 1

    with page_col:
        page_number = st.number_input(
            "Page",
            min_value=1,
            max_value=max_page,
            value=1,
            step=1,
            key=f"{key_prefix}_page_number",
        )

    offset = (int(page_number) - 1) * int(page_size)
    return int(page_size), int(page_number), int(max_page), int(offset)


def dataframe_download_disabled_notice() -> None:
    st.caption(
        "Detail tables are intentionally paginated so the app only queries the slice you are viewing."
    )

