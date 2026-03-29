"""
extract.py
Reads raw data from Delta Lake and validates it
"""

import pandas as pd
from deltalake import DeltaTable
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

RAW_ORDERS_PATH    = "data/raw/delta/orders"
RAW_CUSTOMERS_PATH = "data/raw/delta/customers"
RAW_PRODUCTS_PATH  = "data/raw/delta/products"
PROCESSED_PATH     = "data/processed"


def read_delta(path: str, name: str) -> pd.DataFrame:
    log.info(f"Reading {name} from Delta Lake: {path}")
    dt = DeltaTable(path)
    df = dt.to_pandas()
    log.info(f"  ✅ {name}: {len(df):,} rows | {len(df.columns)} columns | Delta version: {dt.version()}")
    return df


def validate(df: pd.DataFrame, name: str, required_cols: list) -> bool:
    log.info(f"Validating {name}...")
    errors = []

    # Check required columns
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        errors.append(f"Missing columns: {missing}")

    # Check row count
    if len(df) == 0:
        errors.append("Empty DataFrame!")

    # Check nulls in required columns
    for col in required_cols:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                errors.append(f"Column '{col}' has {null_count} nulls")

    if errors:
        for e in errors:
            log.error(f"  ❌ {e}")
        return False

    log.info(f"  ✅ {name} passed all validation checks")
    return True


def save_processed(df: pd.DataFrame, name: str):
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    path = f"{PROCESSED_PATH}/{name}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"  💾 Saved to {path}")


def extract():
    log.info("=" * 55)
    log.info("  EXTRACT LAYER — Reading from Delta Lake")
    log.info("=" * 55)

    # Read
    orders    = read_delta(RAW_ORDERS_PATH,    "Orders")
    customers = read_delta(RAW_CUSTOMERS_PATH, "Customers")
    products  = read_delta(RAW_PRODUCTS_PATH,  "Products")

    # Validate
    orders_ok    = validate(orders,    "Orders",
                            ["order_id", "customer_id", "product_id", "amount", "order_date"])
    customers_ok = validate(customers, "Customers",
                            ["customer_id", "customer_name", "city", "region"])
    products_ok  = validate(products,  "Products",
                            ["product_id", "product_name", "category", "price"])

    if not all([orders_ok, customers_ok, products_ok]):
        raise ValueError("❌ Validation failed! Check logs above.")

    # Save processed
    save_processed(orders,    "orders_raw")
    save_processed(customers, "customers_raw")
    save_processed(products,  "products_raw")

    log.info("=" * 55)
    log.info("  ✅ EXTRACT COMPLETE")
    log.info("=" * 55)

    return orders, customers, products


if __name__ == "__main__":
    extract()
