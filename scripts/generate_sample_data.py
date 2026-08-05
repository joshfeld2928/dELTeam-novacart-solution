"""
Generate sample datasets for the NovaCart pipeline lab.
Creates both clean data and deliberately broken records to exercise
the quarantine and schema-drift paths.

Usage:
    python scripts/generate_sample_data.py
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
import csv

ROOT = Path(__file__).parent.parent
ORDERS_DIR   = ROOT / "data" / "landing" / "orders"
CUSTOMER_DIR = ROOT / "data" / "landing" / "customers"
DB_PATH      = ROOT / "data" / "landing" / "products.db"

ORDERS_DIR.mkdir(parents=True, exist_ok=True)
CUSTOMER_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


# ── Orders CSVs (Full November 2025) - Varying daily volumes (0-10 orders) ──

ORDERS = {
    "2025-11-07": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-001","CUST-001","PROD-001","2025-11-07","2","49.99","shipped"],
        ["ORD-002","CUST-002","PROD-002","2025-11-07","1","199.00","pending"],
        ["ORD-003","CUST-003","PROD-001","2025-11-07","5","49.99","delivered"],
        # Bad row — quantity = 0 → quarantine
        ["ORD-004","CUST-001","PROD-003","2025-11-07","0","29.99","pending"],
        # Duplicate of ORD-001 — deduplicated in Silver
        ["ORD-001","CUST-001","PROD-001","2025-11-07","2","49.99","shipped"],
    ],
    "2025-11-08": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-005","CUST-004","PROD-002","2025-11-08","3","199.00","shipped"],
        ["ORD-006","CUST-001","PROD-003","2025-11-08","1","29.99","delivered"],
        ["ORD-007","CUST-002","PROD-001","2025-11-08","2","49.99","shipped"],
        ["ORD-008","CUST-003","PROD-004","2025-11-08","1","89.99","pending"],
        ["ORD-009","CUST-005","PROD-002","2025-11-08","2","199.00","delivered"],
        ["ORD-010","CUST-001","PROD-003","2025-11-08","4","29.99","shipped"],
        ["ORD-011","CUST-004","PROD-001","2025-11-08","3","49.99","pending"],
    ],
    "2025-11-09": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-012","CUST-003","PROD-003","2025-11-09","4","29.99","pending"],
        ["ORD-013","CUST-005","PROD-001","2025-11-09","2","49.99","shipped"],
        ["ORD-014","CUST-002","PROD-004","2025-11-09","1","89.99","delivered"],
        ["ORD-015","CUST-001","PROD-002","2025-11-09","2","199.00","shipped"],
    ],
    "2025-11-10": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
    ],
    "2025-11-11": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-016","CUST-001","PROD-004","2025-11-11","2","89.99","shipped"],
        ["ORD-017","CUST-003","PROD-002","2025-11-11","1","199.00","pending"],
        ["ORD-018","CUST-002","PROD-003","2025-11-11","3","29.99","delivered"],
        ["ORD-019","CUST-004","PROD-001","2025-11-11","5","49.99","shipped"],
        ["ORD-020","CUST-005","PROD-004","2025-11-11","1","89.99","pending"],
        ["ORD-021","CUST-001","PROD-003","2025-11-11","2","29.99","delivered"],
    ],
    "2025-11-12": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-022","CUST-004","PROD-001","2025-11-12","4","49.99","shipped"],
        ["ORD-023","CUST-005","PROD-004","2025-11-12","1","89.99","pending"],
        ["ORD-024","CUST-002","PROD-002","2025-11-12","2","199.00","delivered"],
        ["ORD-025","CUST-003","PROD-003","2025-11-12","6","29.99","shipped"],
        ["ORD-026","CUST-001","PROD-001","2025-11-12","3","49.99","pending"],
        ["ORD-027","CUST-004","PROD-004","2025-11-12","1","89.99","delivered"],
        ["ORD-028","CUST-005","PROD-002","2025-11-12","2","199.00","shipped"],
        ["ORD-029","CUST-002","PROD-003","2025-11-12","4","29.99","pending"],
    ],
    "2025-11-13": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-030","CUST-001","PROD-002","2025-11-13","2","199.00","delivered"],
        ["ORD-031","CUST-003","PROD-003","2025-11-13","5","29.99","shipped"],
        # Bad row — negative quantity → quarantine
        ["ORD-032","CUST-002","PROD-001","2025-11-13","-1","49.99","pending"],
    ],
    "2025-11-14": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-033","CUST-004","PROD-003","2025-11-14","3","29.99","shipped"],
        ["ORD-034","CUST-005","PROD-002","2025-11-14","1","199.00","delivered"],
        ["ORD-035","CUST-001","PROD-004","2025-11-14","2","89.99","pending"],
        ["ORD-036","CUST-003","PROD-001","2025-11-14","4","49.99","shipped"],
        ["ORD-037","CUST-002","PROD-003","2025-11-14","3","29.99","delivered"],
    ],
    "2025-11-15": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-038","CUST-001","PROD-001","2025-11-15","7","49.99","pending"],
        ["ORD-039","CUST-002","PROD-004","2025-11-15","2","89.99","shipped"],
        ["ORD-040","CUST-004","PROD-002","2025-11-15","1","199.00","delivered"],
        ["ORD-041","CUST-005","PROD-003","2025-11-15","5","29.99","shipped"],
        ["ORD-042","CUST-003","PROD-001","2025-11-15","3","49.99","pending"],
        ["ORD-043","CUST-001","PROD-004","2025-11-15","1","89.99","delivered"],
        ["ORD-044","CUST-002","PROD-002","2025-11-15","2","199.00","shipped"],
        ["ORD-045","CUST-004","PROD-003","2025-11-15","4","29.99","pending"],
        ["ORD-046","CUST-005","PROD-001","2025-11-15","6","49.99","delivered"],
    ],
    "2025-11-16": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-047","CUST-003","PROD-002","2025-11-16","1","199.00","delivered"],
    ],
    "2025-11-17": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-048","CUST-005","PROD-003","2025-11-17","6","29.99","pending"],
        ["ORD-049","CUST-001","PROD-004","2025-11-17","1","89.99","shipped"],
        ["ORD-050","CUST-002","PROD-001","2025-11-17","3","49.99","delivered"],
        ["ORD-051","CUST-004","PROD-002","2025-11-17","2","199.00","shipped"],
        ["ORD-052","CUST-003","PROD-003","2025-11-17","4","29.99","pending"],
        ["ORD-053","CUST-005","PROD-004","2025-11-17","1","89.99","delivered"],
        ["ORD-054","CUST-001","PROD-001","2025-11-17","5","49.99","shipped"],
    ],
    "2025-11-18": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-055","CUST-002","PROD-002","2025-11-18","2","199.00","delivered"],
        ["ORD-056","CUST-003","PROD-001","2025-11-18","3","49.99","shipped"],
        # Duplicate of ORD-055 — deduplicated in Silver
        ["ORD-055","CUST-002","PROD-002","2025-11-18","2","199.00","delivered"],
        ["ORD-057","CUST-004","PROD-004","2025-11-18","1","89.99","pending"],
    ],
    "2025-11-19": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-058","CUST-004","PROD-003","2025-11-19","5","29.99","pending"],
        ["ORD-059","CUST-005","PROD-004","2025-11-19","1","89.99","shipped"],
        ["ORD-060","CUST-001","PROD-002","2025-11-19","2","199.00","delivered"],
        ["ORD-061","CUST-003","PROD-001","2025-11-19","4","49.99","shipped"],
        ["ORD-062","CUST-002","PROD-003","2025-11-19","3","29.99","pending"],
        ["ORD-063","CUST-004","PROD-004","2025-11-19","2","89.99","delivered"],
    ],
    "2025-11-20": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-064","CUST-001","PROD-001","2025-11-20","8","49.99","delivered"],
        ["ORD-065","CUST-002","PROD-002","2025-11-20","1","199.00","shipped"],
        ["ORD-066","CUST-005","PROD-003","2025-11-20","4","29.99","pending"],
    ],
    "2025-11-21": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-067","CUST-003","PROD-003","2025-11-21","4","29.99","pending"],
        ["ORD-068","CUST-004","PROD-004","2025-11-21","2","89.99","shipped"],
        # Bad row — invalid status
        ["ORD-069","CUST-005","PROD-001","2025-11-21","3","49.99","cancelled"],
        ["ORD-070","CUST-001","PROD-002","2025-11-21","1","199.00","delivered"],
        ["ORD-071","CUST-002","PROD-003","2025-11-21","5","29.99","shipped"],
    ],
    "2025-11-22": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-072","CUST-001","PROD-002","2025-11-22","1","199.00","delivered"],
        ["ORD-073","CUST-002","PROD-003","2025-11-22","6","29.99","shipped"],
        ["ORD-074","CUST-003","PROD-004","2025-11-22","2","89.99","pending"],
        ["ORD-075","CUST-004","PROD-001","2025-11-22","3","49.99","delivered"],
        ["ORD-076","CUST-005","PROD-002","2025-11-22","1","199.00","shipped"],
        ["ORD-077","CUST-001","PROD-003","2025-11-22","4","29.99","pending"],
        ["ORD-078","CUST-003","PROD-004","2025-11-22","1","89.99","delivered"],
        ["ORD-079","CUST-002","PROD-001","2025-11-22","5","49.99","shipped"],
    ],
    "2025-11-23": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-080","CUST-003","PROD-001","2025-11-23","2","49.99","pending"],
        ["ORD-081","CUST-004","PROD-004","2025-11-23","1","89.99","shipped"],
        ["ORD-082","CUST-005","PROD-002","2025-11-23","3","199.00","delivered"],
    ],
    "2025-11-24": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-083","CUST-005","PROD-002","2025-11-24","3","199.00","delivered"],
        ["ORD-084","CUST-001","PROD-003","2025-11-24","5","29.99","shipped"],
        ["ORD-085","CUST-002","PROD-004","2025-11-24","2","89.99","pending"],
        ["ORD-086","CUST-003","PROD-001","2025-11-24","4","49.99","delivered"],
        ["ORD-087","CUST-004","PROD-002","2025-11-24","1","199.00","shipped"],
    ],
    "2025-11-25": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-088","CUST-002","PROD-001","2025-11-25","4","49.99","pending"],
        ["ORD-089","CUST-003","PROD-004","2025-11-25","2","89.99","shipped"],
        # Bad row — quantity = 0 → quarantine
        ["ORD-090","CUST-004","PROD-002","2025-11-25","0","199.00","delivered"],
        ["ORD-091","CUST-005","PROD-003","2025-11-25","3","29.99","pending"],
        ["ORD-092","CUST-001","PROD-001","2025-11-25","6","49.99","delivered"],
        ["ORD-093","CUST-002","PROD-004","2025-11-25","1","89.99","shipped"],
    ],
    "2025-11-26": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-094","CUST-005","PROD-003","2025-11-26","7","29.99","shipped"],
        ["ORD-095","CUST-001","PROD-001","2025-11-26","3","49.99","delivered"],
        ["ORD-096","CUST-003","PROD-002","2025-11-26","2","199.00","pending"],
        ["ORD-097","CUST-004","PROD-004","2025-11-26","1","89.99","shipped"],
    ],
    "2025-11-27": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-098","CUST-002","PROD-004","2025-11-27","1","89.99","pending"],
    ],
    "2025-11-28": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-099","CUST-004","PROD-001","2025-11-28","5","49.99","delivered"],
        ["ORD-100","CUST-005","PROD-003","2025-11-28","4","29.99","shipped"],
        ["ORD-101","CUST-001","PROD-002","2025-11-28","2","199.00","pending"],
        ["ORD-102","CUST-003","PROD-004","2025-11-28","1","89.99","delivered"],
        ["ORD-103","CUST-002","PROD-001","2025-11-28","3","49.99","shipped"],
        ["ORD-104","CUST-004","PROD-003","2025-11-28","6","29.99","pending"],
    ],
    "2025-11-29": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-105","CUST-001","PROD-004","2025-11-29","2","89.99","pending"],
        ["ORD-106","CUST-002","PROD-002","2025-11-29","1","199.00","shipped"],
        ["ORD-107","CUST-005","PROD-001","2025-11-29","4","49.99","delivered"],
        ["ORD-108","CUST-003","PROD-003","2025-11-29","3","29.99","shipped"],
        ["ORD-109","CUST-004","PROD-004","2025-11-29","1","89.99","pending"],
        ["ORD-110","CUST-001","PROD-002","2025-11-29","2","199.00","delivered"],
        ["ORD-111","CUST-002","PROD-001","2025-11-29","5","49.99","shipped"],
    ],
    "2025-11-30": [
        ["order_id","customer_id","product_id","order_date","quantity","unit_price","status"],
        ["ORD-112","CUST-003","PROD-001","2025-11-30","6","49.99","delivered"],
        ["ORD-113","CUST-004","PROD-003","2025-11-30","3","29.99","shipped"],
        ["ORD-114","CUST-005","PROD-004","2025-11-30","1","89.99","pending"],
        ["ORD-115","CUST-001","PROD-002","2025-11-30","2","199.00","delivered"],
        ["ORD-116","CUST-002","PROD-003","2025-11-30","4","29.99","shipped"],
        ["ORD-117","CUST-003","PROD-004","2025-11-30","1","89.99","pending"],
        ["ORD-118","CUST-004","PROD-001","2025-11-30","5","49.99","delivered"],
        ["ORD-119","CUST-005","PROD-002","2025-11-30","1","199.00","shipped"],
        ["ORD-120","CUST-001","PROD-003","2025-11-30","3","29.99","pending"],
    ],
}

for date_str, rows in ORDERS.items():
    path = ORDERS_DIR / f"orders_{date_str}.csv"
    with path.open("w", newline="") as f:
        csv.writer(f).writerows(rows)
    print(f"  wrote {path}")


# ── Customers JSON (nested address) ─────────────────────────────────────────

customers = [
    {"customer_id": "CUST-001", "first_name": "Alice",  "last_name": "Smith",
     "email": "alice@example.com",  "address": {"city": "New York",  "country": "US"},
     "signup_date": "2024-01-15", "tier": "gold"},
    {"customer_id": "CUST-002", "first_name": "Bob",    "last_name": "Jones",
     "email": "bob@example.com",    "address": {"city": "London",    "country": "GB"},
     "signup_date": "2024-03-22", "tier": "standard"},
    {"customer_id": "CUST-003", "first_name": "Carol",  "last_name": "White",
     "email": "carol@example.com",  "address": {"city": "Toronto",   "country": "CA"},
     "signup_date": "2023-11-05", "tier": "silver"},
    {"customer_id": "CUST-004", "first_name": "Dan",    "last_name": "Brown",
     "email": "dan@example.com",    "address": {"city": "Sydney",    "country": "AU"},
     "signup_date": "2024-06-18", "tier": "standard"},
    # Bad row — missing @ in email → quarantine
    {"customer_id": "CUST-005", "first_name": "Eve",    "last_name": "Davis",
     "email": "eve-at-example.com", "address": {"city": "Paris",     "country": "FR"},
     "signup_date": "2024-08-01", "tier": "gold"},
]

cust_path = CUSTOMER_DIR / "customers.json"
cust_path.write_text(json.dumps(customers, indent=2))
print(f"  wrote {cust_path}")


# ── Products SQLite DB ────────────────────────────────────────────────────────

conn = sqlite3.connect(DB_PATH)
conn.execute("DROP TABLE IF EXISTS products")
conn.execute("""
    CREATE TABLE products (
        product_id  TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        category    TEXT NOT NULL,
        unit_cost   REAL NOT NULL,
        supplier_id TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
""")
products = [
    ("PROD-001", "Wireless Headphones", "Electronics", 22.50, "SUP-A", "2025-10-01T10:00:00"),
    ("PROD-002", "Laptop Stand",        "Accessories",  8.75, "SUP-B", "2025-10-15T14:30:00"),
    ("PROD-003", "USB-C Hub",           "Electronics", 12.00, "SUP-A", "2025-11-01T09:00:00"),
    ("PROD-004", "Webcam HD",           "Electronics", 35.00, "SUP-C", "2025-11-05T11:15:00"),
]
conn.executemany(
    "INSERT INTO products VALUES (?,?,?,?,?,?)", products
)
conn.commit()
conn.close()
print(f"  wrote {DB_PATH} ({len(products)} products)")

print("\nSample data generation complete.")
print(f"Generated orders for {len(ORDERS)} days (Nov 7-30, 2025)")
print("Daily order counts range from 0 to 10 orders per day")
print("Next: python -m src.pipeline --date 2025-11-30 --backfill 23")
