
CREATE DATABASE IF NOT EXISTS datawarehouse;
USE datawarehouse;


DROP TABLE IF EXISTS sales_fact;
DROP TABLE IF EXISTS products_dim;
DROP TABLE IF EXISTS customers_dim;
DROP TABLE IF EXISTS time_dim;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;


CREATE TABLE transactions (
    order_id INT,
    order_date DATETIME,
    product_id INT,
    quantity_ordered INT,
    customer_id INT,
    time_id INT
);


CREATE TABLE products (
    product_id INT,
    product_name VARCHAR(255),
    product_price DECIMAL(10, 2),
    supplier_id INT,
    supplier_name VARCHAR(255),
    store_id INT,
    store_name VARCHAR(255)
);


CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(255),
    gender VARCHAR(10)
);


CREATE TABLE products_dim (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_price DECIMAL(10, 2),
    supplier_id INT,
    supplier_name VARCHAR(255),
    store_id INT,
    store_name VARCHAR(255)
);


CREATE TABLE customers_dim (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    gender VARCHAR(10)
);


CREATE TABLE time_dim (


    time_id INT PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    week_day VARCHAR(15),
    hour INT
    
);


CREATE TABLE sales_fact (

    order_id INT,
    order_date DATETIME,
    product_id INT,
    customer_id INT,
    time_id INT,
    quantity_ordered INT,
    total_sale DECIMAL(10, 2),
    PRIMARY KEY (order_id),
    FOREIGN KEY (product_id) REFERENCES products_dim(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers_dim(customer_id),
    FOREIGN KEY (time_id) REFERENCES time_dim(time_id)
    
);