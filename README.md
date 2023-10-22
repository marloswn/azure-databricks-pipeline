# Azure + Databricks pipeline
Data pipeline using Azure's cloud along with Databricks, to consume, transform and load real estate data.

## Steps
- Read the JSON file
- Transform JSON file into Delta tables
- Transform JSON fields into columns, remove unnecessary columns and persist the Delta table in the silver layer

## Technologies
- Initial data format: JSON
- Final data format: Delta
- Notebook language: Python
- Cloud: Azure
- Data processing: Databricks
- Orchestration: Data Factory
- Storage: Data Lake Storage Gen2
