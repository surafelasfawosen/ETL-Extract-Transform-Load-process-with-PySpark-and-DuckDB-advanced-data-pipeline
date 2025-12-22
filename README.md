# Advanced ETL Pipeline with PySpark and DuckDB for Fraud Detection

##  Project Overview

This project implements an advanced ETL (Extract, Transform, Load) data pipeline specifically designed for fraud detection analysis. The pipeline leverages **PySpark** for distributed data processing and **DuckDB** for high-performance analytics, creating a robust solution for handling large-scale financial transaction data.


# 🏦 Fraud Detection & Currency Analysis Pipeline

This repository implements a high-performance ETL pipeline for processing financial transactions, enriching data with fraud labels and currency exchange rates, and serving it for interactive analytics.

## 🏗️ System Architecture

```mermaid
graph TD
    %% Data Sources Layer
    subgraph Data_Sources [Data Sources]
        direction TB
        S1[HI-Small_Trans.csv]
        S2[fraud_detection.parquet]
        S3[currency.json]
    end

    %% Processing Layer
    subgraph Processing [Apache PySpark]
        direction TB
        P1[Distributed Processing]
        P2[Data Cleaning & Standardization]
        P3[Currency Normalization & Joins]
        P4[Feature Engineering]
    end

    %% Warehouse Layer
    subgraph Warehouse [DuckDB]
        direction TB
        W1[Analytical Data Warehouse]
        W2[High-Performance OLAP]
    end

    %% Visualization Layer
    subgraph Visualization [Power BI]
        direction TB
        V1[Interactive Fraud Dashboard]
        V2[Currency Trend Analysis]
    end

    %% Relationships
    Data_Sources --> Processing
    Processing --> Warehouse
    Warehouse --> Visualization

    %% Styling
    style Data_Sources fill:#f9f9f9,stroke:#333,stroke-width:2px
    style Processing fill:#e67e22,stroke:#d35400,stroke-width:2px,color:#fff
    style Warehouse fill:#8e44ad,stroke:#71368a,stroke-width:2px,color:#fff
    style Visualization fill:#27ae60,stroke:#1e8449,stroke-width:2px,color:#fff


## Key Features

- **Automated ETL Pipeline**: Orchestrated workflow for data extraction, transformation, and loading
- **Fraud Detection Focus**: Specialized processing for identifying fraudulent transactions
- **Scalable Processing**: PySpark-based transformations capable of handling large datasets
- **High-Performance Analytics**: DuckDB integration for fast analytical queries
- **Data Validation**: Comprehensive data quality checks and validation steps
- **Modular Architecture**: Clean separation of concerns for maintainability

##  Project Structure

```
ETL-Extract-Transform-Load-process-with-PySpark-and-DuckDB-advanced-data-pipeline/
│
├── Data Sets/
│   ├── HI-Small_Trans.csv        # Transaction dataset (475.7 MB)
│   ├── currency.json             # Currency reference data (7.9 KB)
│   └── fraud_detection.parquet   # Pre-processed fraud data (257.3 MB)
│
├── ETL_project/
│   ├── ORCHESTRATION advanced_fraud_pipeline.py  # Main ETL orchestration script
│   ├── analytics.duckdb                          # DuckDB analytics database
│   ├── big data.ipynb                           # Jupyter notebook for analysis
│   └── requirements.txt                         # Python dependencies
│
├── dashboard/
│   └── photo_2025-12-21_09-55-21.jpg           # Dashboard visualization
│
├── .gitattributes
├── .gitignore
└── README.md
```

##  Data Sources

1. **HI-Small_Trans.csv** - Main transactional dataset containing:
   - Customer transaction records
   - Transaction amounts and timestamps
   - Merchant information
   - Geographic data

2. **currency.json** - Reference data for:
   - Currency codes and conversions

3. **fraud_detection.parquet** - Pre-processed data for:
   - Fraud labels and indicators
   - Anomaly detection features
   - Historical fraud patterns

##  Installation & Setup

### Prerequisites
- Python 3.8+
- Java 8 or 11 (for PySpark)
- Git

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd ETL-Extract-Transform-Load-process-with-PySpark-and-DuckDB-advanced-data-pipeline
```

### Step 2: Install Dependencies
```bash
cd ETL_project
pip install -r requirements.txt
```

### Step 3: Set Up Environment
```bash
# Set Java Home (required for PySpark)
export JAVA_HOME=/path/to/java
```
## Project Team Members

| No | Name | Student ID | Role |
|----|------|-----------|------|
| 1 | Surafel Asfawosen | DBU1501482 | Team Lead & Data Engineer |
| 2 | Beimnet Yealemebhan | DBU1501024 | ETL Development |
| 3 | Yonatan Kiross | DBU1501656 | Data Analysis |
| 4 | Bethlehem Asrat | DBU1501062 | Data Validation & Testing |
| 5 | Ephrata Yeshaeh | DBU1501631 | Documentation & Reporting |
| 6 | Besufekad Ayalkbet | DBU1501050 | System Integration |
| 7 | Nardos Molla | DBU1501397 | Research & Model Evaluation |

##  Usage

### Running the ETL Pipeline
```python
# Navigate to the project directory
cd ETL_project

# Run the main orchestration script
python "ORCHESTRATION advanced_fraud_pipeline.py"
```

### Exploring with Jupyter Notebook
```bash
# Start Jupyter Notebook
jupyter notebook

# Open and run 'big data.ipynb'
```

### Querying Analytics Database
```python
import duckdb

# Connect to the analytics database
conn = duckdb.connect('analytics.duckdb')

# Run analytical queries
results = conn.execute("SELECT * FROM fraud_transactions LIMIT 10").fetchall()
```

##  Pipeline Components

### 1. **Extract Phase**
- Load CSV and JSON data from multiple sources
- Read Parquet files for pre-processed data
- Handle different file formats and schemas

### 2. **Transform Phase**
- **Data Cleaning**: Handle missing values, outliers, and inconsistencies
- **Feature Engineering**: Create fraud detection features
- **Data Enrichment**: Join transaction data with currency information
- **Normalization**: Standardize data formats and scales

### 3. **Load Phase**
- Store cleaned and transformed data in DuckDB to enable fast, lightweight analytical queries without requiring an external database.
- Design optimized table schemas with appropriate data types and structure to improve query efficiency and reduce storage overhead.
- Use partitioning and indexing on frequently queried fields to minimize data scans and accelerate analytical performance.

##  Analytics Capabilities

The pipeline:
- Enable real-time fraud detection scoring to identify and flag suspicious transactions as they occur.
- Support historical trend analysis to uncover long-term patterns, anomalies, and performance insights.
- Provide customer behavior profiling to better understand user activity, preferences, and spending habits.
- Perform transaction pattern recognition to detect recurring behaviors and irregular transaction flows.
- Deliver currency conversion analytics to analyze cross-border transactions and compare financial metrics across multiple currencies.

##  Testing & Validation

The pipeline includes:
- Data quality checks at each stage
- Schema validation
- Business rule enforcement
- Performance monitoring
- Error handling and logging

##  Performance Optimizations

- **PySpark optimizations**: Partitioning, caching, and broadcast joins
- **DuckDB features**: Vectorized execution and columnar storage
- **Memory management**: Efficient data serialization
- **Parallel processing**: Multi-core utilization

##  Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

##  License

This project is licensed under the MIT License - see the LICENSE file for details.

##  Support

For issues, questions, or contributions:
1. Check the existing issues
2. Create a new issue with detailed description
3. Provide sample data if applicable

##  Future Enhancements

Planned improvements:
- Real-time streaming capabilities
- Machine learning model integration
- Advanced visualization dashboard
- Cloud deployment options
- Additional data source connectors.

---
