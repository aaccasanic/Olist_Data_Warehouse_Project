USE OlistDW;
 
CREATE TABLE dw.fact_orders (
	order_key INT IDENTITY(1,1) PRIMARY KEY,

    order_id VARCHAR(50),

	customer_key INT,
    product_key INT,
    seller_key INT,
    date_key INT,

	-- métricas
    total_order_value FLOAT,
    total_freight FLOAT,
    total_items INT,

	-- métricas
    delivery_days INT,
    delivery_delay INT,

	-- tiempos
    avg_review_score FLOAT
);

CREATE TABLE dw.fact_order_items (
    order_item_key INT IDENTITY(1,1) PRIMARY KEY,

    order_id VARCHAR(50),

    customer_key INT,
    product_key INT,
    seller_key INT,
    date_key INT,

    order_item_id INT,

    price FLOAT,
    freight_value FLOAT,
    total_value FLOAT
);

CREATE TABLE dw.dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(25)
);

CREATE TABLE dw.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50),
    category_name VARCHAR(100),
    category_name_english VARCHAR(100)
);

CREATE TABLE dw.dim_seller (
    seller_key INT IDENTITY(1,1) PRIMARY KEY,
    seller_id VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(25)
);

CREATE TABLE dw.dim_location (
	location_key INT IDENTITY(1,1) PRIMARY KEY,
    zip_code_prefix VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(25),
    lat DECIMAL(9,6),
    lng DECIMAL(9,6)
);

CREATE TABLE dw.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    month_name VARCHAR(20)
);

/*
DROP TABLE dw.fact_orders
DROP TABLE dw.fact_order_items
DROP TABLE dw.dim_customer
DROP TABLE dw.dim_product
DROP TABLE dw.dim_seller
DROP TABLE dw.dim_location
DROP TABLE dw.dim_date
*/

/*
select * from dw.fact_orders
select * from dw.fact_order_items
select * from dw.dim_customer
select * from dw.dim_product
select * from dw.dim_seller
select * from dw.dim_location
select * from dw.dim_date
*/


