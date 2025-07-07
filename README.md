
README: Near-Real-Time Data Warehouse for METRO Shopping Store

1. Introduction

This project implements a near-real-time Data Warehouse (DW) prototype for the METRO Shopping Store in Pakistan. It includes:

- MESHJOIN Algorithm implemented in Java for near-real-time ETL processes.
- Star Schema for DW, created via SQL scripts.
- OLAP Queries to analyze and derive insights from DW.

2. Project Contents

The project includes:

1. Create-DW.sql - SQL script to create the star schema for DW.
2. OLAP-Queries.sql - SQL script containing OLAP queries.
3. Main.java - Java file implementing the MESHJOIN algorithm.
4. This README file - Step-by-step instructions to execute the project.

3. Prerequisites

1. Software Requirements:

   - Java Development Kit (JDK) 11 or later.
   - Eclipse IDE for Java development.
   - A SQL database (e.g., MySQL or PostgreSQL).
   - JDBC driver for the database.

2. Database Setup:

   - Install and set up your preferred SQL database.
   - Create a database (e.g., metro_dw) to house the DW tables.


4. Step-by-Step Instructions

Step 1: Setting up the Star Schema

1. Open your SQL database management tool (e.g., phpMyAdmin, MySQL Workbench).
2. Load the Create-DW.sql script.
3. Run the script to:

   - Drop existing tables (if any).
   - Create the fact and dimension tables with the appropriate primary and foreign keys.

Step 2: Configuring the Java Project

1. Open Eclipse IDE and create a new Java project.
2. Import the Main.java file into your project.
3. Add the required JDBC driver jar to the project:

   - Right-click the project > Build Path > Configure Build Path > Libraries > Add External JARs.

4. Edit the Main.java file to set database credentials:

   - Locate the section for database configuration.
   - Replace placeholders with your database details (URL, username, password).

Step 3: Running the MESHJOIN Algorithm

1. Compile and run the Main.java file in Eclipse.

2. The program will:

   - Extract transaction data from the TRANSACTIONS table.
   - Transform the data using the CUSTOMERS and PRODUCTS master data tables via MESHJOIN.
   - Load the transformed data into the DW.

Step 4: Executing OLAP Queries

1. Load the OLAP-Queries.sql file into your SQL management tool.
2. Run individual queries to perform analyses such as:
   - Top revenue-generating products.
   - Trend analysis for store revenue growth.
   - Seasonal sales analysis.


5. Notes
1. Ensure that the database schema is consistent with the table and column names expected in the Java program and SQL scripts.
2. Validate your database credentials before running the Java program.
3. All OLAP queries assume data has been correctly loaded into the DW.


6. Contact

For any questions or issues, please contact:

Hammad Sikandar
03057882280
hammadsikandar8191@gmail.com
