USE OlistDW;

CREATE TABLE stg.customers (
	customer_id VARCHAR(50), 
	customer_unique_id VARCHAR(50),
	customer_zip_code_prefix VARCHAR(10),
	customer_city VARCHAR(50),
	customer_state VARCHAR(20)
);

CREATE TABLE stg.geolocation (
	geolocation_zip_code_prefix VARCHAR(10),
	geolocation_lat DECIMAL(9,6),
	geolocation_lng DECIMAL(9,6),
	geolocation_city VARCHAR(50),
	geolocation_state VARCHAR(20)

);

CREATE TABLE stg.order_items (
	order_id VARCHAR(50),
	order_item_id NUMERIC(3),
	product_id VARCHAR(50),
	seller_id VARCHAR(50),
	shipping_limit_date DATETIME,
	price FLOAT,
	freight_value FLOAT
);

CREATE TABLE stg.order_payments (
	order_id VARCHAR(50),
	payment_sequential NUMERIC(3),
	payment_type VARCHAR(50),
	payment_installments NUMERIC(3),
	payment_value FLOAT
);

CREATE TABLE stg.order_reviews (
	review_id VARCHAR(50),
	order_id VARCHAR(50),
	review_score NUMERIC(3),
	review_comment_title VARCHAR(50),
	review_comment_message VARCHAR(MAX),
	review_creation_date DATETIME,
	review_answer_timestamp DATETIME
);

CREATE TABLE stg.orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATE
);

CREATE TABLE stg.products (
	product_id VARCHAR(50),
	product_category_name VARCHAR(50),
	product_name_lenght NUMERIC(3),
	product_description_lenght NUMERIC(5),
	product_photos_qty NUMERIC(3),
	product_weight_g NUMERIC(5),
	product_length_cm NUMERIC(3),
	product_height_cm NUMERIC(3),
	product_width_cm NUMERIC(3),
);

CREATE TABLE stg.sellers (
	seller_id VARCHAR(50),
	seller_zip_code_prefix VARCHAR(10),
	seller_city VARCHAR(50),
	seller_state VARCHAR(20)
);

CREATE TABLE stg.product_category_name_translation (
	product_category_name VARCHAR(100),
	product_category_name_english VARCHAR(100)
);


/*
DROP TABLE stg.customers
DROP TABLE stg.geolocation
DROP TABLE stg.order_items
DROP TABLE stg.order_payments
DROP TABLE stg.order_reviews
DROP TABLE stg.orders
DROP TABLE stg.products
DROP TABLE stg.sellers
DROP TABLE stg.product_category_name_translation
*/

/*
select * from stg.customers;
select * from stg.geolocation;
select * from stg.order_items;
select * from stg.order_payments;
select * from stg.order_reviews;
select * from stg.orders;
select * from stg.products;
select * from stg.sellers;
select * from stg.product_category_name_translation
*/
