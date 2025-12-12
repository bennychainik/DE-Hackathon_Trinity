# Data Governance & Logging

## Data Governance
- **Access Control**: Role-Based Access Control (RBAC) enforced at Database level.
- **Data Quality**: 
    - Critical elements (Customer ID, Policy ID) must not be NULL.
    - Currency values must be positive.
- **Versioning**: All code versioned in Git. Database schemas versioned via DDL scripts.

## Logging & Monitoring
- **Tool**: Python `logging` module.
- **Log Locations**: `logs/pipeline.log`.
- **Retention**: Logs cleared before each hackathon run (or rotated).
- **Levels**:
    - `INFO`: Normal flow (Row counts, success messages).
    - `WARNING`: Bad data (Nulls, Duplicates) - *Does not stop pipeline*.
    - `ERROR`: Pipeline failures (DB connection lost) - *Stops pipeline*.
