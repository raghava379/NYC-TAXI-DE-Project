
# Architecture

NYC Taxi Dataset
↓
Azure Data Lake Storage Gen2 (Bronze Layer)
↓
Azure Databricks Serverless + PySpark
↓
Data Cleaning & Transformations
↓
Silver Delta Tables (Processed Data)
↓
Business Aggregations & Analytics
↓
Gold Delta Tables (Analytics Layer)
↓
Unity Catalog for Governance & Access Management


NYC Taxi Data
      ↓
ADLS Gen2 Bronze
      ↓
Databricks PySpark
      ↓
Silver Delta
      ↓
Gold Delta
      ↓
Analytics / Reporting
