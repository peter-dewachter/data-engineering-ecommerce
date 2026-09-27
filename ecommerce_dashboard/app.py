import sys
from pathlib import Path

# Ensure repo root is on path so ecommerce_dashboard package can be imported
repo_root = Path(__file__).parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

import streamlit as st

from ecommerce_dashboard.app_pages import (
    customers,
    data_quality,
    deliveries,
    overview,
    sales,
)

st.set_page_config(
    page_title="E-commerce data platform",
    page_icon=":material/shopping_bag:",
    layout="wide",
)

pages = {
    "Analytics": [
        st.Page(
            overview.render,
            title="Overview",
            icon=":material/dashboard:",
            url_path="overview",
            default=True,
        ),
        st.Page(
            sales.render,
            title="Sales",
            icon=":material/monitoring:",
            url_path="sales",
        ),
        st.Page(
            customers.render,
            title="Customers",
            icon=":material/groups:",
            url_path="customers",
        ),
        st.Page(
            deliveries.render,
            title="Deliveries",
            icon=":material/local_shipping:",
            url_path="deliveries",
        ),
    ],
    "Engineering": [
        st.Page(
            data_quality.render,
            title="Data quality",
            icon=":material/verified:",
            url_path="data_quality",
        )
    ],
}

navigation = st.navigation(pages, position="sidebar", expanded=True)

with st.sidebar:
    st.divider()
    st.markdown("### E-commerce data platform")
    st.caption(
        "Portfolio analytics layer for the Olist Brazilian e-commerce dataset."
    )
    st.caption(
        "The app queries curated Gold tables in Databricks and leaves Bronze/Silver transformations in the pipeline."
    )
    st.markdown(
        "**Gold tables**\n"
        "- `ecommerce.gold.sales`\n"
        "- `ecommerce.gold.customers`\n"
        "- `ecommerce.gold.deliveries`"
    )

navigation.run()
