# Architecture

Dynamic API Data Source
↓
Azure Data Pipeline / Ingestion Workflow
↓
Azure Data Lake Storage Gen2 (Bronze Layer)
↓
Azure Databricks Serverless + PySpark
↓
Data Cleaning & Transformations
↓
Silver Delta Tables
↓
Business Aggregations & Analytics
↓
Gold Delta Tables
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
