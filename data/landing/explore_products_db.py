"""
Explore the products.db SQLite database
Lists columns and displays sample data
"""
import sqlite3
import pandas as pd
from pathlib import Path

# Get the database path
db_path = Path(__file__).parent / "products.db"

# Connect to the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("=" * 60)
print("TABLES IN DATABASE:")
print("=" * 60)
for table in tables:
    print(f"  - {table[0]}")
print()

# For each table, list columns and show data
for table in tables:
    table_name = table[0]
    print("=" * 60)
    print(f"TABLE: {table_name}")
    print("=" * 60)
    
    # Get column information
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    
    print("\nCOLUMNS:")
    for col in columns:
        col_id, col_name, col_type, not_null, default_val, pk = col
        print(f"  {col_name:20s} | Type: {col_type:10s} | PK: {pk} | Not Null: {not_null}")
    
    # Read into pandas and display head
    print(f"\nDATA PREVIEW (head()):")
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    print(df.head())
    print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")
    print()

# Close connection
conn.close()
