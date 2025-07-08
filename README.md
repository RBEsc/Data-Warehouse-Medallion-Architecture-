# DataWarehouse Project
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

## Project Overview
The data warehouse is designed using medallion architecture
![DATA LAYER](https://github.com/user-attachments/assets/5276e4f9-35cc-4547-bea3-7c3022516e61)

## Repository Contents
### Docs
- Data Warehouse Design: used to define the structure of the warehouse from the source to layers going to the users.
- Data Layer: display the layers of the data warehouse and the standards and procedures for each layer
- Data Flow: details how the data is flowing from the source to each layer of the data warehouse
- Data Mart Schema: used to define the relationship and model of the data

### SQL Scripts
- 1.0 DW_init: creation of database and schemas for bronze, silver and gold layers
- 2.0 DDL_bronze_layer: creation of tables for bronze layer
- 2.1 Load_bronze_layer: extraction of data from source
- 3.0 DDL_silver_layer: creation of tables for silver layer
- 3.1 Load_silver_layer: extration of data from bronze layer. This is the stage where data cleansing, normalization and transformation is done
- 4.0 Load_gold_layer: extraction of data from silver layer. This is the stage where data model will be is set up and converted to users view.

Note: Scripts are coded using SQL Server.

### Dataset
- source_erp
  - CUST_AZ12
  - LOC_A101
  - PX_CAT_G1V2
- source_crm
  - cust_info
  - prd_info
  - sales_details
