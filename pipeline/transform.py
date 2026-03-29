"""
transform.py
Cleans, enriches, and builds staging + mart layers
Pure pandas — no paid services needed
"""

import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

PROCESSED_PATH = "data/processed"
MART_PATH      = "data/mart"


def load(name: str) -> pd.DataFrame:
    path = f"{PROCESSED_PATH}/{name}.parquet"
    df   = pd.read_parquet(path)
    log.info(f"Loaded {name}: {len(df):,} rows")
    return df


def save(df: pd.DataFrame, name: str, layer: str = "mart"):
    path = f"data/{layer}"
    os.makedirs(path, exist_ok=True)
    out  = f"{path}/{name}.parquet"
    df.to_parquet(out, index=False)
    log.info(f"  💾 Saved {name} → {out} ({len(df):,} rows)")


# ── Staging ───────────────────────────────────────────────────────────────────
def stg_orders(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building stg_orders...")
    df = df.copy()

    # Parse dates
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Remove duplicates
    before = len(df)
    df     = df.drop_duplicates(subset=["order_id"])
    log.info(f"  Removed {before - len(df)} duplicate orders")

    # Fill nulls
    df["discount"] = df["discount"].fillna(0)
    df["amount"]   = df["amount"].fillna(0)

    # Remove negative amounts
    df = df[df["amount"] >= 0]

    # Validate status
    valid_statuses = ["completed", "pending", "cancelled", "refunded"]
    df = df[df["status"].isin(valid_statuses)]

    # Add derived columns
    df["order_year"]  = df["order_date"].dt.year
    df["order_month"] = df["order_date"].dt.month
    df["order_quarter"] = df["order_date"].dt.quarter
    df["order_dow"]   = df["order_date"].dt.day_name()
    df["is_weekend"]  = df["order_date"].dt.dayofweek >= 5
    df["revenue"]     = df["amount"]
    df["profit"]      = df["amount"] * 0.35  # estimated margin

    log.info(f"  ✅ stg_orders: {len(df):,} rows")
    return df


def stg_customers(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building stg_customers...")
    df = df.copy()
    df = df.drop_duplicates(subset=["customer_id"])
    df["signup_date"]    = pd.to_datetime(df["signup_date"], errors="coerce")
    df["customer_name"]  = df["customer_name"].str.strip().str.title()
    df["email"]          = df["email"].str.lower().str.strip()
    df["is_active"]      = df["is_active"].fillna(True)
    log.info(f"  ✅ stg_customers: {len(df):,} rows")
    return df


def stg_products(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building stg_products...")
    df = df.copy()
    df = df.drop_duplicates(subset=["product_id"])
    df["price"]        = df["price"].clip(lower=0)
    df["cost"]         = df["cost"].fillna(df["price"] * 0.5)
    df["margin"]       = np.round((df["price"] - df["cost"]) / df["price"] * 100, 2)
    df["is_available"] = df["is_available"].fillna(True)
    log.info(f"  ✅ stg_products: {len(df):,} rows")
    return df


# ── Dimension Tables ──────────────────────────────────────────────────────────
def dim_customers(stg_cust: pd.DataFrame) -> pd.DataFrame:
    log.info("Building dim_customers...")
    df = stg_cust[[
        "customer_id", "customer_name", "email",
        "city", "region", "segment", "signup_date", "is_active"
    ]].copy()
    # SCD Type 2 columns
    df["valid_from"]   = pd.Timestamp("2022-01-01")
    df["valid_to"]     = pd.Timestamp("9999-12-31")
    df["is_current"]   = True
    df["record_hash"]  = pd.util.hash_pandas_object(
                            df[["customer_name","city","segment"]], index=False
                         ).astype(str)
    log.info(f"  ✅ dim_customers: {len(df):,} rows (SCD Type 2 ready)")
    return df


def dim_products(stg_prod: pd.DataFrame) -> pd.DataFrame:
    log.info("Building dim_products...")
    df = stg_prod[[
        "product_id", "product_name", "category",
        "brand", "price", "cost", "margin", "is_available"
    ]].copy()
    log.info(f"  ✅ dim_products: {len(df):,} rows")
    return df


def dim_date() -> pd.DataFrame:
    log.info("Building dim_date...")
    dates = pd.date_range(start="2022-01-01", end="2024-12-31", freq="D")
    df = pd.DataFrame({
        "date_id":      dates.strftime("%Y%m%d").astype(int),
        "full_date":    dates,
        "year":         dates.year,
        "quarter":      dates.quarter,
        "month":        dates.month,
        "month_name":   dates.month_name(),
        "week":         dates.isocalendar().week.values,
        "day":          dates.day,
        "day_name":     dates.day_name(),
        "is_weekend":   dates.dayofweek >= 5,
        "is_month_end": dates.is_month_end,
        "is_quarter_end": dates.is_quarter_end,
    })
    log.info(f"  ✅ dim_date: {len(df):,} rows")
    return df


# ── Fact Table ────────────────────────────────────────────────────────────────
def fact_orders(stg_ord: pd.DataFrame,
                stg_cust: pd.DataFrame,
                stg_prod: pd.DataFrame) -> pd.DataFrame:
    log.info("Building fact_orders...")

    df = stg_ord.merge(
        stg_cust[["customer_id", "city", "region", "segment"]],
        on="customer_id", how="left"
    ).merge(
        stg_prod[["product_id", "category", "brand", "cost"]],
        on="product_id", how="left"
    )

    df["date_id"]   = df["order_date"].dt.strftime("%Y%m%d").astype(int)
    df["gross_revenue"] = df["revenue"]
    df["discount_amount"] = df["unit_price"] * df["quantity"] * df["discount"]
    df["net_revenue"]    = df["gross_revenue"] - df["discount_amount"]
    df["cogs"]           = df["cost"] * df["quantity"]
    df["gross_profit"]   = df["net_revenue"] - df["cogs"]

    fact = df[[
        "order_id", "customer_id", "product_id", "date_id",
        "order_date", "order_year", "order_month", "order_quarter",
        "quantity", "unit_price", "discount", "discount_amount",
        "gross_revenue", "net_revenue", "cogs", "gross_profit",
        "status", "payment_method", "city", "region", "segment",
        "category", "brand", "is_weekend"
    ]]

    log.info(f"  ✅ fact_orders: {len(fact):,} rows")
    return fact


# ── Aggregated Mart Tables ─────────────────────────────────────────────────────
def mart_monthly_revenue(fact: pd.DataFrame) -> pd.DataFrame:
    log.info("Building mart_monthly_revenue...")
    df = fact[fact["status"] == "completed"].groupby(
        ["order_year", "order_month", "region"]
    ).agg(
        total_orders    =("order_id",      "count"),
        total_revenue   =("net_revenue",   "sum"),
        total_profit    =("gross_profit",  "sum"),
        avg_order_value =("net_revenue",   "mean"),
        total_customers =("customer_id",   "nunique"),
    ).reset_index().round(2)
    log.info(f"  ✅ mart_monthly_revenue: {len(df):,} rows")
    return df


def mart_customer_segments(fact: pd.DataFrame) -> pd.DataFrame:
    log.info("Building mart_customer_segments...")
    df = fact[fact["status"] == "completed"].groupby(
        ["customer_id", "segment", "region"]
    ).agg(
        total_orders    =("order_id",    "count"),
        total_spent     =("net_revenue", "sum"),
        avg_order_value =("net_revenue", "mean"),
        first_order     =("order_date",  "min"),
        last_order      =("order_date",  "max"),
    ).reset_index().round(2)

    # Customer lifetime value bucket
    df["clv_bucket"] = pd.cut(
        df["total_spent"],
        bins=[0, 10000, 50000, 200000, float("inf")],
        labels=["Low", "Medium", "High", "Platinum"]
    )
    log.info(f"  ✅ mart_customer_segments: {len(df):,} rows")
    return df


def mart_product_performance(fact: pd.DataFrame) -> pd.DataFrame:
    log.info("Building mart_product_performance...")
    df = fact[fact["status"] == "completed"].groupby(
        ["product_id", "category", "brand"]
    ).agg(
        total_orders   =("order_id",     "count"),
        total_qty      =("quantity",     "sum"),
        total_revenue  =("net_revenue",  "sum"),
        total_profit   =("gross_profit", "sum"),
        avg_price      =("unit_price",   "mean"),
        avg_discount   =("discount",     "mean"),
    ).reset_index().round(2)

    df["profit_margin_pct"] = (df["total_profit"] / df["total_revenue"] * 100).round(2)
    df["revenue_rank"]      = df["total_revenue"].rank(ascending=False).astype(int)
    log.info(f"  ✅ mart_product_performance: {len(df):,} rows")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────
def transform():
    log.info("=" * 55)
    log.info("  TRANSFORM LAYER")
    log.info("=" * 55)

    # Load raw
    raw_orders    = load("orders_raw")
    raw_customers = load("customers_raw")
    raw_products  = load("products_raw")

    # Staging
    stg_ord  = stg_orders(raw_orders)
    stg_cust = stg_customers(raw_customers)
    stg_prod = stg_products(raw_products)

    save(stg_ord,  "stg_orders",    "staging")
    save(stg_cust, "stg_customers", "staging")
    save(stg_prod, "stg_products",  "staging")

    # Dimensions
    d_customers = dim_customers(stg_cust)
    d_products  = dim_products(stg_prod)
    d_date      = dim_date()

    save(d_customers, "dim_customers", "mart")
    save(d_products,  "dim_products",  "mart")
    save(d_date,      "dim_date",      "mart")

    # Fact
    f_orders = fact_orders(stg_ord, stg_cust, stg_prod)
    save(f_orders, "fact_orders", "mart")

    # Aggregated marts
    save(mart_monthly_revenue(f_orders),    "mart_monthly_revenue",    "mart")
    save(mart_customer_segments(f_orders),  "mart_customer_segments",  "mart")
    save(mart_product_performance(f_orders),"mart_product_performance","mart")

    log.info("=" * 55)
    log.info("  ✅ TRANSFORM COMPLETE — All layers built")
    log.info("=" * 55)

    return f_orders


if __name__ == "__main__":
    transform()
