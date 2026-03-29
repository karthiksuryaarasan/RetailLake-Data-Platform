"""
tests/test_pipeline.py
Unit tests for each pipeline stage
Run: pytest tests/ -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture
def sample_orders():
    return pd.DataFrame({
        "order_id":      ["ORD_001", "ORD_002", "ORD_003", "ORD_001"],  # dup
        "customer_id":   ["C1", "C2", "C3", "C1"],
        "product_id":    ["P1", "P2", "P1", "P1"],
        "order_date":    ["2023-01-15", "2023-02-20", "2023-03-10", "2023-01-15"],
        "quantity":      [2, 1, 3, 2],
        "unit_price":    [500.0, 1500.0, 200.0, 500.0],
        "discount":      [0.10, 0.0, 0.05, 0.10],
        "amount":        [900.0, 1500.0, 570.0, 900.0],
        "status":        ["completed", "pending", "cancelled", "completed"],
        "payment_method":["UPI", "Credit Card", "COD", "UPI"],
        "created_at":    ["2023-01-15"] * 4,
    })


@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "customer_id":   ["C1", "C2", "C3"],
        "customer_name": ["Alice Smith", "bob jones", "CHARLIE K"],
        "email":         ["ALICE@EMAIL.COM", "bob@email.com", "charlie@email.com"],
        "city":          ["Bangalore", "Mumbai", "Delhi"],
        "region":        ["South", "West", "North"],
        "segment":       ["VIP", "Regular", "New"],
        "signup_date":   ["2022-01-01", "2022-06-15", "2023-01-01"],
        "is_active":     [True, True, None],
        "created_at":    ["2022-01-01"] * 3,
    })


@pytest.fixture
def sample_products():
    return pd.DataFrame({
        "product_id":   ["P1", "P2", "P3"],
        "product_name": ["Electronics_P1", "Clothing_P2", "Books_P3"],
        "category":     ["Electronics", "Clothing", "Books"],
        "price":        [500.0, 1500.0, 200.0],
        "cost":         [300.0, None, 80.0],
        "brand":        ["Brand_1", "Brand_2", "Brand_3"],
        "is_available": [True, True, False],
        "created_at":   ["2022-01-01"] * 3,
    })


# ── Transform Tests ───────────────────────────────────────────────────────────
class TestStagingOrders:
    def test_removes_duplicates(self, sample_orders):
        from pipeline.transform import stg_orders
        result = stg_orders(sample_orders)
        assert result["order_id"].duplicated().sum() == 0

    def test_parses_dates(self, sample_orders):
        from pipeline.transform import stg_orders
        result = stg_orders(sample_orders)
        assert pd.api.types.is_datetime64_any_dtype(result["order_date"])

    def test_adds_year_month(self, sample_orders):
        from pipeline.transform import stg_orders
        result = stg_orders(sample_orders)
        assert "order_year" in result.columns
        assert "order_month" in result.columns

    def test_valid_statuses_only(self, sample_orders):
        from pipeline.transform import stg_orders
        result = stg_orders(sample_orders)
        valid = {"completed", "pending", "cancelled", "refunded"}
        assert set(result["status"].unique()).issubset(valid)

    def test_no_negative_amounts(self, sample_orders):
        from pipeline.transform import stg_orders
        result = stg_orders(sample_orders)
        assert (result["amount"] >= 0).all()


class TestStagingCustomers:
    def test_removes_duplicates(self, sample_customers):
        from pipeline.transform import stg_customers
        result = stg_customers(sample_customers)
        assert result["customer_id"].duplicated().sum() == 0

    def test_fills_null_is_active(self, sample_customers):
        from pipeline.transform import stg_customers
        result = stg_customers(sample_customers)
        assert result["is_active"].isnull().sum() == 0

    def test_email_lowercased(self, sample_customers):
        from pipeline.transform import stg_customers
        result = stg_customers(sample_customers)
        assert result["email"].str.islower().all()


class TestStagingProducts:
    def test_fills_null_cost(self, sample_products):
        from pipeline.transform import stg_products
        result = stg_products(sample_products)
        assert result["cost"].isnull().sum() == 0

    def test_adds_margin(self, sample_products):
        from pipeline.transform import stg_products
        result = stg_products(sample_products)
        assert "margin" in result.columns
        assert (result["margin"] >= 0).all()

    def test_no_negative_price(self, sample_products):
        from pipeline.transform import stg_products
        result = stg_products(sample_products)
        assert (result["price"] >= 0).all()


class TestDimDate:
    def test_covers_date_range(self):
        from pipeline.transform import dim_date
        result = dim_date()
        assert result["full_date"].min() <= pd.Timestamp("2022-01-01")
        assert result["full_date"].max() >= pd.Timestamp("2024-12-31")

    def test_no_duplicate_dates(self):
        from pipeline.transform import dim_date
        result = dim_date()
        assert result["date_id"].duplicated().sum() == 0

    def test_has_required_columns(self):
        from pipeline.transform import dim_date
        result = dim_date()
        required = ["date_id", "full_date", "year", "month", "day", "is_weekend"]
        for col in required:
            assert col in result.columns


# ── Data Generator Tests ──────────────────────────────────────────────────────
class TestDataGenerator:
    def test_customers_count(self):
        from pipeline.generate_data import generate_customers
        result = generate_customers()
        assert len(result) == 500

    def test_products_count(self):
        from pipeline.generate_data import generate_products
        result = generate_products()
        assert len(result) == 100

    def test_orders_count(self):
        from pipeline.generate_data import generate_customers, generate_products, generate_orders
        c = generate_customers()
        p = generate_products()
        o = generate_orders(c, p)
        assert len(o) == 100_000

    def test_no_null_order_ids(self):
        from pipeline.generate_data import generate_customers, generate_products, generate_orders
        c = generate_customers()
        p = generate_products()
        o = generate_orders(c, p)
        assert o["order_id"].isnull().sum() == 0

    def test_amounts_positive(self):
        from pipeline.generate_data import generate_customers, generate_products, generate_orders
        c = generate_customers()
        p = generate_products()
        o = generate_orders(c, p)
        assert (o["amount"] >= 0).all()
