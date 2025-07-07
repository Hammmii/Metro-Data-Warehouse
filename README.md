# Near-Real-Time Data Warehouse for METRO Shopping Store

## 1. Introduction

This project implements a **near-real-time Data Warehouse (DW)** prototype for the **METRO Shopping Store in Pakistan**. It includes:

- ✅ MESHJOIN Algorithm implemented in Java for near-real-time ETL processes  
- ✅ Star Schema for DW using SQL scripts  
- ✅ OLAP Queries to derive insights from the DW  

---

## 2. Project Contents

| File                  | Description                                      |
|-----------------------|--------------------------------------------------|
| `Create-DW.sql`       | SQL script to create the star schema             |
| `OLAP-Queries.sql`    | SQL script with analytical OLAP queries          |
| `Main.java`           | Java implementation of the MESHJOIN algorithm    |
| `README.md`           | Setup and execution instructions                 |

---

## 3. Prerequisites

### Software Requirements:
- Java Development Kit (JDK) 11 or later
- Eclipse IDE (or any Java IDE)
- A SQL database (e.g., MySQL or PostgreSQL)
- JDBC driver for your selected database

### Database Setup:
- Install and set up your preferred SQL database
- Create a new database (e.g., `metro_dw`) for the warehouse

---

## 4. Step-by-Step Instructions

### Step 1: Setting up the Star Schema
1. Open your SQL database tool (e.g., MySQL Workbench)
2. Load and run `Create-DW.sql` to:
   - Drop existing tables (if any)
   - Create fact and dimension tables with appropriate keys

### Step 2: Configuring the Java Project
1. Open Eclipse and create a new Java project
2. Import `Main.java` into the project
3. Add JDBC driver:
   - `Right-click project > Build Path > Configure Build Path > Libraries > Add External JARs`
4. In `Main.java`, set your database configuration:
   - Update URL, username, and password

### Step 3: Running the MESHJOIN Algorithm
1. Compile and run `Main.java`
2. The program will:
   - Extract data from the `TRANSACTIONS` table
   - Transform it using `CUSTOMERS` and `PRODUCTS` master tables
   - Load into the data warehouse

### Step 4: Executing OLAP Queries
1. Load `OLAP-Queries.sql` into your SQL tool
2. Run queries such as:
   - Top revenue-generating products
   - Revenue trend analysis
   - Seasonal sales insights

---

## 5. Notes
- Ensure your database schema matches what’s expected in the scripts and Java file
- Double-check database credentials before running the program
- OLAP queries assume data has already been loaded into DW

---

## 6. Contact

**Hammad Sikandar**  
📞 0305-7882280  
✉️ hammadsikandar8191@gmail.com  