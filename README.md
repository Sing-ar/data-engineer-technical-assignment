# Data Engineer Assignment - ETL Pipeline

## Project Overview

This project implements an ETL pipeline to extract data from a SQLite database, clean and transform the data, and generate analytics-ready datasets.

The pipeline processes customer and order data through the following steps:

1. Extract data from SQLite views
2. Perform data cleaning and transformation
3. Apply business rules
4. Validate the output data
5. Export cleaned datasets

---

# Technology Stack

* Python 3.11
* Pandas
* SQLite
* SQLAlchemy
* Prefect (Workflow Orchestration)
* Pytest
* Structlog

---

# Project Structure

```
Data Engineer Assignment/
│
├── pipeline.py              # ETL flow implementation
├── test_pipeline.py         # Unit tests
├── requirements.txt         # Dependencies
├── shopdata.db              # Source database
├── README.md
│
├── clean_customers.csv      # Output customer dataset
└── clean_orders.csv         # Output order dataset
```

---

# How to Run the Pipeline

## 1. Clone Repository

```bash
git clone <repository-url>

cd "Data Engineer Assignment"
```

---

## 2. Create Virtual Environment

Windows:

```bash
python -m venv venv

venv\Scripts\activate
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Run ETL Flow

Execute the pipeline:

```bash
python pipeline.py
```

The flow will:

* Extract data from SQLite views
* Clean customer and order data
* Apply transformation logic
* Generate output files:

```
clean_customers.csv
clean_orders.csv
```

---

# How to Run Tests

This project uses `pytest` for unit testing.

Run:

```bash
pytest
```

The tests use dummy DataFrames instead of connecting to the production database.

Test coverage includes:

## Customer Cleaning

Validates:

* Duplicate customer removal
* Keeping the latest customer record
* Data cleaning logic

## Currency Conversion

Validates:

* Exchange rate calculation
* Correct converted amount output

Example:

```python
def test_currency_conversion():

    result = convert_currency(
        amount=100,
        exchange_rate=35
    )

    assert result == 3500
```

---

# Data Exploration Findings

During data exploration, the raw customer and order datasets were analyzed to identify data quality issues and understand the data structure.

## Customer Data Findings

### Duplicate Records

The customer dataset contained duplicate records based on customer identifiers/email addresses.

To solve this issue:

* Records were sorted by signup date
* The latest customer information was retained
* Older duplicated records were removed

### Missing Values

Some columns contained missing values.

Handling approach:

* Required fields were validated
* Missing values were cleaned or removed depending on business requirements

### Data Format Issues

Observed issues:

* Inconsistent phone number formats
* Extra spaces in text fields
* Different formatting patterns

Cleaning actions:

* Trimmed whitespace
* Standardized phone number format
* Validated email format

---

## Order Data Findings

### Invalid Records

The order dataset was checked for:

* Missing order information
* Incorrect data types
* Invalid numerical values

### Revenue Calculation

Order revenue was calculated using:

```
Revenue = Quantity × Price
```

This created a consistent metric for downstream analytics.

---

# Data Quality Checks

The pipeline includes validation checks:

* Required columns exist
* Duplicate records removed
* Data types validated
* Output files generated successfully

---

# ETL Pipeline Design

```
             SQLite Database
                    |
                    v
              Extract Layer
                    |
                    v
          Python Transformation
          (Cleaning + Validation)
                    |
                    v
               Load Output
                    |
                    v
       clean_customers.csv
       clean_orders.csv
```

---

# Future Improvements

Potential improvements:

* Add incremental loading
* Store processed data in a data warehouse
* Add automated data quality monitoring
* Schedule pipeline execution using Prefect deployment
* Add CI/CD pipeline for automated testing

---

# Author

Data Engineer Assignment Project

```
```
