USE OlistDW;

CREATE TABLE raw.customers (
	customer_id VARCHAR(255), 
	customer_unique_id VARCHAR(255),
	customer_zip_code_prefix VARCHAR(255),
	customer_city VARCHAR(255),
	customer_state VARCHAR(255)
);

CREATE TABLE raw.geolocation (
	geolocation_zip_code_prefix VARCHAR(255),
	geolocation_lat VARCHAR(255),
	geolocation_lng VARCHAR(255),
	geolocation_city VARCHAR(255),
	geolocation_state VARCHAR(255)

);

CREATE TABLE raw.order_items (
	order_id VARCHAR(255),
	order_item_id VARCHAR(255),
	product_id VARCHAR(255),
	seller_id VARCHAR(255),
	shipping_limit_date VARCHAR(255),
	price VARCHAR(255),
	freight_value VARCHAR(255)
);

CREATE TABLE raw.order_payments (
	order_id VARCHAR(255),
	payment_sequential VARCHAR(255),
	payment_type VARCHAR(255),
	payment_installments VARCHAR(255),
	payment_value VARCHAR(255)
);

CREATE TABLE raw.order_reviews (
	review_id VARCHAR(255),
	order_id VARCHAR(255),
	review_score VARCHAR(255),
	review_comment_title VARCHAR(255),
	review_comment_message VARCHAR(MAX),
	review_creation_date VARCHAR(255),
	review_answer_timestamp VARCHAR(255)
);

CREATE TABLE raw.orders (
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    order_status VARCHAR(255),
    order_purchase_timestamp VARCHAR(255),
    order_approved_at VARCHAR(255),
    order_delivered_carrier_date VARCHAR(255),
    order_delivered_customer_date VARCHAR(255),
    order_estimated_delivery_date VARCHAR(255)
);

CREATE TABLE raw.products (
	product_id VARCHAR(255),
	product_category_name VARCHAR(255),
	product_name_lenght VARCHAR(255),
	product_description_lenght VARCHAR(255),
	product_photos_qty VARCHAR(255),
	product_weight_g VARCHAR(255),
	product_length_cm VARCHAR(255),
	product_height_cm VARCHAR(255),
	product_width_cm VARCHAR(255),
);

CREATE TABLE raw.sellers (
	seller_id VARCHAR(255),
	seller_zip_code_prefix VARCHAR(255),
	seller_city VARCHAR(255),
	seller_state VARCHAR(255)
);

CREATE TABLE raw.product_category_name_translation (
	product_category_name VARCHAR(255),
	product_category_name_english VARCHAR(255)
);


/*
DROP TABLE raw.customers
DROP TABLE raw.geolocation
DROP TABLE raw.order_items
DROP TABLE raw.order_payments
DROP TABLE raw.order_reviews
DROP TABLE raw.orders
DROP TABLE raw.products
DROP TABLE raw.sellers
DROP TABLE raw.product_category_name_translation
*/

/*
select * from raw.customers;
select * from raw.geolocation;
select * from raw.order_items;
select * from raw.order_payments;
select * from raw.order_reviews;
select * from raw.orders;
select * from raw.products;
select * from raw.sellers;
select * from raw.product_category_name_translation
*/