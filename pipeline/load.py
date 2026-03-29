"""
load.py
Final load layer — data quality checks + summary report
Writes final mart tables back to Delta Lake
"""

import pandas as pd
import os
import logging
from deltalake import write_deltalake, DeltaTable
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


def load_parquet(name: str, layer: str = "mart") -> pd.DataFrame:
    path = f"data/{layer}/{name}.parquet"
    df   = pd.read_parquet(path)
    log.info(f"Loaded {name}: {len(df):,} rows")
    return df


def quality_check(df: pd.DataFrame, name: str, checks: dict) -> dict:
    log.info(f"🔍 Quality checking {name}...")
    results = {"table": name, "passed": [], "failed": [], "warnings": []}

    # Row count
    row_count = len(df)
    if row_count == 0:
        results["failed"].append("EMPTY TABLE")
    else:
        results["passed"].append(f"Row count: {row_count:,}")

    # Null checks
    for col in checks.get("not_null", []):
        if col in df.columns:
            nulls = df[col].isnull().sum()
            if nulls > 0:
                results["failed"].append(f"NULL in '{col}': {nulls} rows")
            else:
                results["passed"].append(f"No nulls in '{col}'")

    # Unique checks
    for col in checks.get("unique", []):
        if col in df.columns:
            dupes = df[col].duplicated().sum()
            if dupes > 0:
                results["failed"].append(f"Duplicates in '{col}': {dupes}")
            else:
                results["passed"].append(f"'{col}' is unique")

    # Accepted values
    for col, vals in checks.get("accepted_values", {}).items():
        if col in df.columns:
            invalid = ~df[col].isin(vals)
            if invalid.sum() > 0:
                results["failed"].append(f"Invalid values in '{col}': {invalid.sum()} rows")
            else:
                results["passed"].append(f"All values in '{col}' are valid")

    # Range checks
    for col, (mn, mx) in checks.get("range", {}).items():
        if col in df.columns:
            out_of_range = ((df[col] < mn) | (df[col] > mx)).sum()
            if out_of_range > 0:
                results["warnings"].append(f"'{col}' out of range [{mn},{mx}]: {out_of_range} rows")
            else:
                results["passed"].append(f"'{col}' within range [{mn},{mx}]")

    status = "✅ PASSED" if not results["failed"] else "❌ FAILED"
    log.info(f"  {status} — {len(results['passed'])} passed, {len(results['failed'])} failed")
    return results


def write_to_delta_mart(df: pd.DataFrame, name: str):
    path = f"data/delta_mart/{name}"
    os.makedirs(path, exist_ok=True)

    # Convert datetime for Delta compatibility
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype(str)

    # Convert categorical
    for col in df.select_dtypes(include=["category"]).columns:
        df[col] = df[col].astype(str)

    write_deltalake(path, df, mode="overwrite")
    dt = DeltaTable(path)
    log.info(f"  💾 Delta Lake: {path} | Version: {dt.version()}")


def print_summary_report(all_results: list, fact_df: pd.DataFrame):
    print("\n" + "=" * 65)
    print("  📊 DATA QUALITY REPORT")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    total_passed = sum(len(r["passed"]) for r in all_results)
    total_failed = sum(len(r["failed"]) for r in all_results)

    for r in all_results:
        status = "✅" if not r["failed"] else "❌"
        print(f"\n  {status} {r['table']}")
        for p in r["passed"]:
            print(f"      ✓ {p}")
        for f in r["failed"]:
            print(f"      ✗ {f}")
        for w in r["warnings"]:
            print(f"      ⚠ {w}")

    print("\n" + "-" * 65)
    print(f"  Total checks passed : {total_passed}")
    print(f"  Total checks failed : {total_failed}")

    # Business metrics
    completed = fact_df[fact_df["status"] == "completed"]
    print("\n  📈 PIPELINE METRICS")
    print(f"  Total orders processed  : {len(fact_df):,}")
    print(f"  Completed orders        : {len(completed):,}")
    print(f"  Total revenue           : ₹{completed['net_revenue'].sum():,.0f}")
    print(f"  Avg order value         : ₹{completed['net_revenue'].mean():,.0f}")
    print(f"  Unique customers        : {fact_df['customer_id'].nunique():,}")
    print(f"  Unique products         : {fact_df['product_id'].nunique():,}")
    print("=" * 65)
    print(f"\n  {'✅ ALL CHECKS PASSED' if total_failed == 0 else '❌ SOME CHECKS FAILED'}")
    print("=" * 65)


def load():
    log.info("=" * 55)
    log.info("  LOAD LAYER — Quality Checks + Delta Lake Write")
    log.info("=" * 55)

    # Load mart tables
    fact      = load_parquet("fact_orders")
    dim_cust  = load_parquet("dim_customers")
    dim_prod  = load_parquet("dim_products")
    dim_date  = load_parquet("dim_date")
    mth_rev   = load_parquet("mart_monthly_revenue")
    cust_seg  = load_parquet("mart_customer_segments")
    prod_perf = load_parquet("mart_product_performance")

    # Quality checks
    all_results = []

    all_results.append(quality_check(fact, "fact_orders", {
        "not_null":       ["order_id", "customer_id", "product_id", "amount"],
        "unique":         ["order_id"],
        "accepted_values":{"status": ["completed","pending","cancelled","refunded"]},
        "range":          {"quantity": (1, 100), "discount": (0, 1)}
    }))

    all_results.append(quality_check(dim_cust, "dim_customers", {
        "not_null": ["customer_id", "customer_name", "region"],
        "unique":   ["customer_id"],
    }))

    all_results.append(quality_check(dim_prod, "dim_products", {
        "not_null": ["product_id", "product_name", "category"],
        "unique":   ["product_id"],
        "range":    {"price": (0, 500000)}
    }))

    all_results.append(quality_check(dim_date, "dim_date", {
        "not_null": ["date_id", "full_date"],
        "unique":   ["date_id"],
    }))

    all_results.append(quality_check(mth_rev, "mart_monthly_revenue", {
        "not_null": ["order_year", "order_month", "total_revenue"],
    }))

    # Write to Delta Lake
    log.info("Writing final tables to Delta Lake...")
    write_to_delta_mart(fact,      "fact_orders")
    write_to_delta_mart(dim_cust,  "dim_customers")
    write_to_delta_mart(dim_prod,  "dim_products")
    write_to_delta_mart(dim_date,  "dim_date")
    write_to_delta_mart(mth_rev,   "mart_monthly_revenue")
    write_to_delta_mart(cust_seg,  "mart_customer_segments")
    write_to_delta_mart(prod_perf, "mart_product_performance")

    # Summary
    print_summary_report(all_results, fact)

    log.info("=" * 55)
    log.info("  ✅ LOAD COMPLETE")
    log.info("=" * 55)


if __name__ == "__main__":
    load()
