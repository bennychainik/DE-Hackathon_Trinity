# 🚀 Sprint-0: Design Phase – Data Engineering Hackathon

## 📌 1. Objective

This sprint focuses on architecting the complete data engineering solution before development begins.

Our purpose is to design:
- The ETL Pipeline
- The Data Warehouse schema (Star Schema)
- The SCD-Type 2 strategy
- The Data Flow
- Clear team roles
- Base folder/project structure

This ensures a stable foundation for implementation in Sprint-1.

---

## 🏗 2. High-Level Architecture

We designed a modular ETL system that ingests multi-region insurance data from CSV files, validates, standardizes, transforms, and loads it into a MySQL-based Data Warehouse.

### End-to-end Flow
`RAW FILES` → `INGESTION` → `VALIDATION` → `STANDARDIZATION` → `TRANSFORMATION (SCD-2)` → `STAGING TABLES` → `STAR SCHEMA (DIMENSIONS + FACT)` → `REPORTING SQL QUERIES`

---

## 📐 3. Architecture Diagram

```mermaid
graph LR
    subgraph Sources
        CSV[Regional CSV Files]
    end

    subgraph "ETL Application (Python)"
        Ingest[Ingestion Layer]
        Validate[Validation Layer]
        Clean[Standardization]
        Transform[Transformation Logic (SCD2)]
    end

    subgraph "Data Warehouse (MySQL)"
        Staging[(Staging Area)]
        Dims[[Dimension Tables]]
        Facts[[Fact Tables]]
    end

    subgraph "Reporting"
        SQL[SQL Analytical Queries]
        BI[BI/Dashboard]
    end

    CSV --> Ingest --> Validate --> Clean --> Transform
    Transform --> Staging --> Dims --> SQL
    Transform --> Staging --> Facts --> SQL
    SQL --> BI
```

---

## ⭐ 4. Key Design Decisions

### 4.1 Star Schema Chosen Over Snowflake
We selected Star Schema because it is:
- Faster in MySQL (fewer joins)
- Easier to implement in an 8-hour hackathon
- Ideal for analytical queries
- Supports SCD-2 cleanly
- Simpler and more intuitive for BI tools

*Snowflake Schema adds unnecessary complexity for this dataset.*

### 4.2 SCD-Type 2 Strategy
We apply SCD-2 on:
- **Customer Dimension** → Marital status change history
- **Policy Dimension** → Policy type & policy detail changes

Each record includes:
- `effective_start_dt`
- `effective_end_dt`
- `is_current`

This allows historical tracking for business requirements (e.g., “Who changed policy type?”).

---

## 🧱 5. Data Warehouse Schema

### 5.1 Dimension Tables
| Dimension | Purpose |
| :--- | :--- |
| `dim_customer` | Customer profile + marital status history (SCD-2) |
| `dim_policy` | Policy details + policy type history (SCD-2) |
| `dim_policy_type` | Master list of policy categories |
| `dim_address` | Region-based address details |
| `dim_date` | Calendar dimension for time-based analysis |

### 5.2 Fact Table
| Fact Table | Purpose |
| :--- | :--- |
| `fact_transactions` | Premiums, installments, total amount, late fees, foreign keys to all dimensions |

---

## 🔁 6. ETL Pipeline Design

### 6.1 Ingestion Layer
- Reads CSV files from 4 regions (East, West, South, Central)
- Loads raw data into MySQL staging tables
- Ensures traceability (no transformation yet)

### 6.2 Validation Layer
Performs DQ checks:
- Null checks
- Date validation
- Duplicate detection
- Schema consistency

### 6.3 Standardization Layer
Cleans and normalizes:
- Region names
- Policy terms (Monthly/Quarterly/Yearly)
- Gender, marital status
- Date formats

Produces a unified dataset.

### 6.4 Transformation Layer
Implements:
- Business rules
- SCD-2 logic
- Late fee calculation
- Installment calculation
- Dimension & Fact loading logic

---

## 🗂 7. Repository Structure (Sprint-0)

```
DE-Hackathon_Trinity/
│
├── src/
│   ├── ingestion.py
│   ├── validation.py
│   ├── transformation.py
│   └── utils/
│
├── sql/
│   ├── ddl/           → MySQL DWH table creation scripts
│   └── reporting/     → SQL queries for tasks (b–g)
│
├── docs/
│   ├── architecture.md
│   ├── data_model.png
│   └── scd2_design.md
│
├── logs/
│
└── README.md   (this file)
```

---

## 👥 8. Team Roles

| Member | Role | Responsibilities |
| :--- | :--- | :--- |
| **Ingestion Lead** | ETL Input | File ingestion, staging load |
| **Modeling Lead** | DWH Setup | Star schema, SCD-2, transformations |
| **Reporting Lead** | Analytics | SQL queries, documentation, insights |