"""
generate_data.py
Generates synthetic ecommerce data and saves to Delta Lake (Parquet format)
100% FREE — no paid services needed
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta
from deltalake import write_deltalake
from deltalake import DeltaTable

# ── Config ────────────────────────────────────────────────────────────────────
RAW_PATH       = "data/raw/delta/orders"
CUSTOMERS_PATH = "data/raw/delta/customers"
PRODUCTS_PATH  = "data/raw/delta/products"
NUM_CUSTOMERS  = 500
NUM_PRODUCTS   = 100
NUM_ORDERS     = 100_000
RANDOM_SEED    = 42

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ── Helpers ───────────────────────────────────────────────────────────────────
CITIES    = ["Bangalore", "Mumbai", "Delhi", "Chennai", "Hyderabad",
             "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat"]
REGIONS   = {"Bangalore": "South", "Mumbai": "West", "Delhi": "North",
             "Chennai": "South", "Hyderabad": "South", "Pune": "West",
             "Kolkata": "East", "Ahmedabad": "West", "Jaipur": "North",
             "Surat": "West"}
CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Kitchen",
              "Sports", "Beauty", "Toys", "Automotive", "Grocery", "Furniture"]
STATUSES   = ["completed", "pending", "cancelled", "refunded"]
STATUS_W   = [0.70, 0.15, 0.10, 0.05]

# ── 1. Customers ──────────────────────────────────────────────────────────────
def generate_customers():
    print("🧑 Generating customers...")
    cities    = np.random.choice(CITIES, NUM_CUSTOMERS)
    signup    = [datetime(2021, 1, 1) + timedelta(days=int(d))
                 for d in np.random.randint(0, 900, NUM_CUSTOMERS)]
    segments  = np.random.choice(["VIP", "Regular", "New"], NUM_CUSTOMERS,
                                  p=[0.10, 0.60, 0.30])
    df = pd.DataFrame({
        "customer_id":      [f"CUST_{str(i).zfill(5)}" for i in range(1, NUM_CUSTOMERS+1)],
        "customer_name":    [f"Customer_{i}" for i in range(1, NUM_CUSTOMERS+1)],
        "email":            [f"customer_{i}@email.com" for i in range(1, NUM_CUSTOMERS+1)],
        "city":             cities,
        "region":           [REGIONS[c] for c in cities],
        "segment":          segments,
        "signup_date":      signup,
        "is_active":        np.random.choice([True, False], NUM_CUSTOMERS, p=[0.85, 0.15]),
        "created_at":       datetime.now(),
    })
    print(f"   ✅ {len(df):,} customers generated")
    return df

# ── 2. Products ───────────────────────────────────────────────────────────────
def generate_products():
    print("📦 Generating products...")
    cats  = np.random.choice(CATEGORIES, NUM_PRODUCTS)
    price = np.round(np.random.uniform(50, 50_000, NUM_PRODUCTS), 2)
    df = pd.DataFrame({
        "product_id":       [f"PROD_{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS+1)],
        "product_name":     [f"{cats[i-1]}_Product_{i}" for i in range(1, NUM_PRODUCTS+1)],
        "category":         cats,
        "price":            price,
        "cost":             np.round(price * np.random.uniform(0.3, 0.7, NUM_PRODUCTS), 2),
        "brand":            [f"Brand_{np.random.randint(1, 21)}" for _ in range(NUM_PRODUCTS)],
        "is_available":     np.random.choice([True, False], NUM_PRODUCTS, p=[0.90, 0.10]),
        "created_at":       datetime.now(),
    })
    print(f"   ✅ {len(df):,} products generated")
    return df

# ── 3. Orders ─────────────────────────────────────────────────────────────────
def generate_orders(customers_df, products_df):
    print("🛒 Generating 100,000 orders...")
    cust_ids = customers_df["customer_id"].tolist()
    prod_ids = products_df["product_id"].tolist()
    price_map = dict(zip(products_df["product_id"], products_df["price"]))

    order_dates = [datetime(2022, 1, 1) + timedelta(days=int(d))
                   for d in np.random.randint(0, 730, NUM_ORDERS)]
    prod_choices = np.random.choice(prod_ids, NUM_ORDERS)
    quantities   = np.random.randint(1, 6, NUM_ORDERS)
    discounts    = np.round(np.random.uniform(0, 0.30, NUM_ORDERS), 2)
    statuses     = np.random.choice(STATUSES, NUM_ORDERS, p=STATUS_W)

    unit_prices  = [price_map[p] for p in prod_choices]
    amounts      = np.round(
        np.array(unit_prices) * quantities * (1 - discounts), 2
    )

    df = pd.DataFrame({
        "order_id":         [f"ORD_{str(i).zfill(7)}" for i in range(1, NUM_ORDERS+1)],
        "customer_id":      np.random.choice(cust_ids, NUM_ORDERS),
        "product_id":       prod_choices,
        "order_date":       order_dates,
        "quantity":         quantities,
        "unit_price":       unit_prices,
        "discount":         discounts,
        "amount":           amounts,
        "status":           statuses,
        "payment_method":   np.random.choice(
                                ["UPI", "Credit Card", "Debit Card", "NetBanking", "COD"],
                                NUM_ORDERS, p=[0.35, 0.25, 0.20, 0.10, 0.10]),
        "created_at":       datetime.now(),
    })
    print(f"   ✅ {len(df):,} orders generated")
    return df

# ── 4. Write to Delta Lake ────────────────────────────────────────────────────
def write_to_delta(df, path, name):
    print(f"💾 Writing {name} to Delta Lake at '{path}'...")
    os.makedirs(path, exist_ok=True)

    # Convert datetime columns to string for Delta Lake compatibility on Windows
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime"]).columns:
        df[col] = df[col].astype(str)

    write_deltalake(path, df, mode="overwrite")
    dt = DeltaTable(path)
    print(f"   ✅ Written! Version: {dt.version()} | Files: {len(dt.files())}")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  ECOMMERCE DATA PLATFORM — Data Generator")
    print("=" * 60)

    customers = generate_customers()
    products  = generate_products()
    orders    = generate_orders(customers, products)

    write_to_delta(customers, CUSTOMERS_PATH, "Customers")
    write_to_delta(products,  PRODUCTS_PATH,  "Products")
    write_to_delta(orders,    RAW_PATH,       "Orders")

    print("\n" + "=" * 60)
    print("  ✅ ALL DATA WRITTEN TO DELTA LAKE SUCCESSFULLY")
    print(f"  📁 Customers : {CUSTOMERS_PATH}")
    print(f"  📁 Products  : {PRODUCTS_PATH}")
    print(f"  📁 Orders    : {RAW_PATH}")
    print("=" * 60)
