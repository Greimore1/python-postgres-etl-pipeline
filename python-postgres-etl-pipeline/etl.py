import csv, sys, os
import psycopg2
from psycopg2.extras import execute_values

DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")

def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app.sales (
            id bigserial primary key,
            sku text not null,
            qty int not null check (qty >= 0),
            price numeric(10,2) not null check (price >= 0),
            ts timestamptz not null default now()
        )""")

def load_csv(path):
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sku = r["sku"].strip()
            qty = int(r["qty"])
            price = float(r["price"])
            rows.append((sku, qty, price))
    return rows

def bulk_insert(cur, rows):
    execute_values(cur, "INSERT INTO app.sales (sku, qty, price) VALUES %s", rows)

def report(cur):
    cur.execute("select sku, sum(qty) as units, sum(qty*price) as revenue from app.sales group by sku order by revenue desc;")
    return cur.fetchall()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl.py <csv>"); sys.exit(1)
    data = load_csv(sys.argv[1])
    conn = psycopg2.connect(DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_table(cur)
                bulk_insert(cur, data)
                cur.execute("set search_path=app,public;")
                print(report(cur))
    finally:
        conn.close()
