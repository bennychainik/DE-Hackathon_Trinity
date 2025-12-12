# Data Modeling Strategy

## Architecture: Star Schema
We utilize a **Star Schema** approach for the Data Warehouse to optimize for:
1.  **Read Performance**: Simplified joins for reporting.
2.  **Scalability**: Easy to add new dimensions.
3.  **Simplicity**: Queries are intuitive for analytics.

## Entity Relationship Diagram (ERD) Concept

```mermaid
erDiagram
    FACT_TRANSACTIONS }|--|| DIM_CUSTOMER : "belongs to"
    FACT_TRANSACTIONS }|--|| DIM_POLICY : "relates to"
    FACT_TRANSACTIONS }|--|| DIM_ADDRESS : "located in"

    DIM_CUSTOMER {
        int customer_sk PK
        string customer_id UK
        string name
        string segment
    }

    DIM_POLICY {
        int policy_sk PK
        string policy_id UK
        string type
        date start_date
    }

    FACT_TRANSACTIONS {
        int txn_id PK
        int customer_sk FK
        int policy_sk FK
        decimal amount
        date txn_date
    }
```

## Best Practices
- **Surrogate Keys (SK)**: Always use Auto-Increment Integer SKs for joins. Never join on string business IDs if possible.
- **SCD Strategy**:
    - **Dimensions**: Type 1 (Overwrite) for corrections, Type 2 (History) for status changes (e.g., Policy Status).
    - **Facts**: Transactional (Append-only).
