from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator

import pandas as pd
import requests
import streamlit as st
from databricks import sql

LOGGER = logging.getLogger(__name__)

REQUIRED_ENV_VARS = (
    "DATABRICKS_SERVER_HOSTNAME",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
)


class DatabricksConfigurationError(RuntimeError):
    """Raised when the Streamlit app is missing required Databricks configuration."""


class DatabricksQueryError(RuntimeError):
    """Raised when a Databricks query cannot be completed successfully."""


def get_missing_env_vars() -> list[str]:
    return [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]


def _require_env_vars() -> None:
    missing = get_missing_env_vars()
    if missing:
        missing_list = ", ".join(missing)
        raise DatabricksConfigurationError(
            f"Missing required Databricks environment variables: {missing_list}"
        )


def get_oauth_token() -> str:
    _require_env_vars()
    hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]

    response = requests.post(
        f"https://{hostname}/oidc/v1/token",
        auth=(
            os.environ["DATABRICKS_CLIENT_ID"],
            os.environ["DATABRICKS_CLIENT_SECRET"],
        ),
        data={
            "grant_type": "client_credentials",
            "scope": "sql",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


@contextmanager
def get_connection() -> Iterator[sql.client.Connection]:
    token = get_oauth_token()
    connection = sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=token,
    )
    try:
        yield connection
    finally:
        connection.close()


@st.cache_data(ttl=900, max_entries=128, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description or []]
        return pd.DataFrame(rows, columns=columns)
    except DatabricksConfigurationError:
        raise
    except Exception as exc:  # pragma: no cover - depends on live Databricks connectivity
        LOGGER.exception("Databricks query execution failed.")
        raise DatabricksQueryError("Unable to retrieve data from Databricks.") from exc


@st.cache_data(ttl=900, max_entries=128, show_spinner=False)
def run_scalar(query: str):
    result = run_query(query)
    if result.empty or result.shape[1] == 0:
        return None
    return result.iloc[0, 0]

