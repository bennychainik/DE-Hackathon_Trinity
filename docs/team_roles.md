# team Roles: Data Engineering Squad (3 Members)

## 1. Ingestion & Validation Lead
- **Focus**: "Getting data IN and Clean."
- **Responsibilities**:
    - Write scripts to read CSVs from all regions.
    - Implement `validation.py` rules (Nulls, Duplicates).
    - Handle schema changes/mismatches.
    - Output: Cleaned Staging Tables (Parquet/SQL).

## 2. Modeling & Transformation Lead
- **Focus**: "Building the Warehouse."
- **Responsibilities**:
    - Design Star Schema.
    - Implement `transformation.py` (Joins, SCD logic).
    - Load Fact/Dim tables.
    - Ensure Referential Integrity.

## 3. SQL & Reporting Lead
- **Focus**: "Getting value OUT."
- **Responsibilities**:
    - Write complex SQL queries for the Problem Statement.
    - Create Views/Marts for final reporting.
    - Maintain Documentation (`data_dictionary`, `README`).
    - Optimize Query Performance.

## Collaboration Plan
- **Kickoff (Hour 0-1)**: Agree on Schema & Folder Structure.
- **Mid-point (Hour 4)**: Integration Test (Ingestion -> DWH).
- **Final Stretch (Hour 7)**: QA & deck prep.
