"""
run_pipeline.py
Master runner — executes full ELT pipeline end to end
Run this single file to execute everything
"""

import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
log = logging.getLogger(__name__)


def run_stage(name: str, func):
    log.info(f"\n{'='*55}")
    log.info(f"  STARTING: {name}")
    log.info(f"{'='*55}")
    start = time.time()
    try:
        func()
        elapsed = time.time() - start
        log.info(f"  ✅ {name} completed in {elapsed:.1f}s")
        return True, elapsed
    except Exception as e:
        elapsed = time.time() - start
        log.error(f"  ❌ {name} FAILED after {elapsed:.1f}s: {e}")
        raise


if __name__ == "__main__":
    print("\n" + "🚀 " * 20)
    print("  ECOMMERCE DATA PLATFORM — FULL PIPELINE RUN")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀 " * 20 + "\n")

    pipeline_start = time.time()

    from pipeline.generate_data import generate_customers, generate_products, generate_orders, write_to_delta
    from pipeline.extract import extract
    from pipeline.transform import transform
    from pipeline.load import load

    # Stage 0: Generate data (only first run)
    import os
    if not os.path.exists("data/raw/delta/orders"):
        log.info("First run — generating synthetic data...")
        customers = generate_customers()
        products  = generate_products()
        orders    = generate_orders(customers, products)
        write_to_delta(customers, "data/raw/delta/customers", "Customers")
        write_to_delta(products,  "data/raw/delta/products",  "Products")
        write_to_delta(orders,    "data/raw/delta/orders",    "Orders")
    else:
        log.info("Data already exists — skipping generation")

    # Run pipeline
    results = []
    results.append(run_stage("EXTRACT", extract))
    results.append(run_stage("TRANSFORM", transform))
    results.append(run_stage("LOAD", load))

    total = time.time() - pipeline_start

    print("\n" + "✅ " * 20)
    print("  PIPELINE COMPLETE")
    print(f"  Total time: {total:.1f}s")
    print(f"  Stages: {len(results)}/3 succeeded")
    print("✅ " * 20 + "\n")
