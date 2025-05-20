# SQL-Datawarehouse-project
📖 Overview
This project demonstrates the development of a modern data warehouse using SQL Server, encompassing ETL processes, data modeling, and analytics. It follows the Medallion Architecture pattern, organizing data into Bronze, Silver, and Gold layers to streamline data processing and analysis.

🏗️ Architecture
The data warehouse architecture is structured as follows:

Bronze Layer: Raw data ingestion from source systems (e.g., CSV files).

Silver Layer: Cleansed and transformed data, ready for analysis.

Gold Layer: Business-ready data modeled into a star schema for reporting and analytics.

🔧 Technologies Used
Database: Microsoft SQL Server

ETL Tool: T-SQL Scripts

Data Modeling: Star Schema

Analytics & Reporting: SQL-based queries
🚀 Getting Started
Prerequisites
Microsoft SQL Server installed

SQL Server Management Studio (SSMS) or any SQL client

Setup Instructions
Clone the repository:

bash
Copy
Edit
git clone https://github.com/binisha8/SQL-Datawarehouse-project.git
Create the database and schemas:

Execute the create_database.sql and create_schemas.sql scripts to set up the database and necessary schemas.

Create tables:

Run the create_tables.sql script to create the required tables in each schema.

Load data:

Use the etl_process.sql script to perform ETL operations, moving data from the Bronze to Silver and then to the Gold layer.

Run analytics queries:

Execute queries from the analytics_queries.sql script to generate insights from the data warehouse.


📝 License
This project is licensed under the MIT License.

🙌 Acknowledgments
Inspired by best practices in data warehousing and analytics. Special thanks to the contributors and the open-source community.
