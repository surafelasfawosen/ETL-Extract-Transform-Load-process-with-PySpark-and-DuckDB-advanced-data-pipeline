# Advanced ETL Pipeline with PySpark and DuckDB for Fraud Detection

## 📋 Project Overview

This project implements an advanced ETL (Extract, Transform, Load) data pipeline specifically designed for fraud detection analysis. The pipeline leverages **PySpark** for distributed data processing and **DuckDB** for high-performance analytics, creating a robust solution for handling large-scale financial transaction data.

## 🎯 Key Features

- **Automated ETL Pipeline**: Orchestrated workflow for data extraction, transformation, and loading
- **Fraud Detection Focus**: Specialized processing for identifying fraudulent transactions
- **Scalable Processing**: PySpark-based transformations capable of handling large datasets
- **High-Performance Analytics**: DuckDB integration for fast analytical queries
- **Data Validation**: Comprehensive data quality checks and validation steps
- **Modular Architecture**: Clean separation of concerns for maintainability

## 🏗️ Project Structure

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

## 📊 Data Sources

1. **HI-Small_Trans.csv** - Main transactional dataset containing:
   - Customer transaction records
   - Transaction amounts and timestamps
   - Merchant information
   - Geographic data

2. **currency.json** - Reference data for:
   - Currency codes and conversions
   - Exchange rates
   - Currency metadata

3. **fraud_detection.parquet** - Pre-processed data for:
   - Fraud labels and indicators
   - Anomaly detection features
   - Historical fraud patterns

## 🚀 Installation & Setup

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

## ⚙️ Usage

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

## 🔧 Pipeline Components

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
- Store processed data in DuckDB for analytics
- Create optimized tables for query performance
- Implement data partitioning and indexing

## 📈 Analytics Capabilities

The pipeline enables:
- Real-time fraud detection scoring
- Historical trend analysis
- Customer behavior profiling
- Transaction pattern recognition
- Currency conversion analytics

## 🧪 Testing & Validation

The pipeline includes:
- Data quality checks at each stage
- Schema validation
- Business rule enforcement
- Performance monitoring
- Error handling and logging

## 📊 Performance Optimizations

- **PySpark optimizations**: Partitioning, caching, and broadcast joins
- **DuckDB features**: Vectorized execution and columnar storage
- **Memory management**: Efficient data serialization
- **Parallel processing**: Multi-core utilization

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For issues, questions, or contributions:
1. Check the existing issues
2. Create a new issue with detailed description
3. Provide sample data if applicable

## 🔮 Future Enhancements

Planned improvements:
- Real-time streaming capabilities
- Machine learning model integration
- Advanced visualization dashboard
- Cloud deployment options
- Additional data source connectors

---
