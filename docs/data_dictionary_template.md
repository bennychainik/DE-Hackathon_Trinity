# Data Dictionary

| Table Name | Column Name | Data Type | Description | Source System |
|------------|-------------|-----------|-------------|---------------|
| `dim_customer` | `customer_id` | VARCHAR | Natural key from source | CRM |
| `dim_customer` | `segment` | VARCHAR | Segmentation grouping | CRM |
| `dim_policy` | `start_date` | DATE | Policy effective date | Core System |
| `fact_txn` | `amount` | DECIMAL | Transaction Value | Billing |
| `fact_txn` | `transaction_date` | DATE | Date of payment | Billing |

## Mapping Logic
- **Customer ID**: Mapped directly from `Source_File_A.CustID`.
- **Region**: Derived from `Source_File_B.State` -> Lookup Table.
