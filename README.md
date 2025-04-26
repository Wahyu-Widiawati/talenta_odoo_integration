# 👥 Talenta-Odoo Integration

This project automates the flow of **employee data** from **Talenta** to **PostgreSQL**, ready to be used in systems like **Odoo**:

1. Fetch employee data securely from Talenta API using **Python**.
2. Insert the data into a **PostgreSQL** database with automatic table creation if needed.

> This is a portfolio project. All sensitive information (credentials, API secrets, database passwords) has been removed or replaced for public sharing.

---

## 🔧 Tech Stack

- **Python**: Data fetching and database operations
- **PostgreSQL**: Data storage
- **Talenta API**: HR data source (secured with HMAC Authentication)

---

## 🚀 How it Works

flowchart TD
    TalentaAPI[Talenta API (Employee Data)] --> PyScript[talenta_odoo_integration.py]
    PyScript --> Postgres[(PostgreSQL Database)]
