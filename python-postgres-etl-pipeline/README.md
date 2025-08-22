# Python ETL to PostgreSQL

This project demonstrates a simple Python ETL workflow: validate data from CSV, bulk-load into PostgreSQL, and run an aggregate revenue report. Includes unit tests and a Makefile target.

---

## Tools Used

- **Python 3.12**
- **psycopg2**
- **PostgreSQL**
- **pytest**
- **Makefile**

---

## Skills Demonstrated

**Data Validation & Parsing:**  
Read and validated CSV fields, enforcing numeric types for `qty` and `price` before loading.

**Bulk Insertion:**  
Used `execute_values` for efficient loading into Postgres with a single round-trip per batch.

**Schema Management & Constraints:**  
Created `app.sales` with checks for non-negative quantities and prices to protect data quality.

**Reporting Queries:**  
Computed per-SKU units and revenue using SQL aggregations.

**Testing & Automation:**  
Added unit tests for parsing logic and a Makefile target for quick runs.

---

