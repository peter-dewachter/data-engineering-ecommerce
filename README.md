# Data engineering e-commerce portfolio

An end-to-end data engineering portfolio project built on the Olist Brazilian E-Commerce Public Dataset.

## Architecture

```text
Raw Olist CSV files
        ↓
Bronze
        ↓
Silver
        ↓
Gold
        ↓
Streamlit
```

The primary engineering work happens in Databricks through the Bronze → Silver → Gold pipeline. The Streamlit application is the consumption layer on top of curated Gold tables.

## Data engineering pipeline

### Bronze
Raw source data landed with minimal transformation.

### Silver
Standardized, validated, and quality-checked entities.

### Gold
Business-facing analytical tables designed for reporting and portfolio analytics.

### Streamlit application
Interactive frontend that queries curated Gold tables in Databricks.

## Data quality

The pipeline surfaces explicit quality indicators such as:

- timestamp anomalies in deliveries
- missing delivery timestamps
- missing carrier timestamps
- missing estimated delivery dates
- uncategorized product records
- null-count checks for important business fields

## Gold layer

The Streamlit app consumes these tables:

- `ecommerce.gold.sales`
- `ecommerce.gold.customers`
- `ecommerce.gold.deliveries`

These Gold tables are the source of truth for portfolio analytics.

## Streamlit application

The frontend lives in `ecommerce_dashboard/` and uses:

- Streamlit for the UI
- Databricks SQL Connector for warehouse access
- OAuth M2M service-principal authentication with `scope=sql`

See `ecommerce_dashboard/README.md` for setup and run instructions.

## Repository structure

```text
data-engineering-ecommerce/
├── notebooks/
├── shared/
└── ecommerce_dashboard/
```

## GitHub repository

This repository is intended to showcase:

- data engineering design
- Gold-layer business modeling
- SQL and Databricks usage
- secure service-principal authentication
- analytical consumption patterns in Streamlit

## Live demo

Add the deployment URL here once the Streamlit app has been deployed and validated.

