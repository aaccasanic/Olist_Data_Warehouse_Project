-- CREATE DATABASE OlistDW;

Use OlistDW
GO

/*
	-----------------------------------------------------------------------------
	-- CREACIÓN DE USUARIO PARA LA CONEXIÓN CON AZURE Y SCHEMAS QUE SE UTILIZARÁN
	-----------------------------------------------------------------------------

	CREATE LOGIN adf_user WITH PASSWORD = 'Password123!';
	GO
	
	USE OlistDW;
	GO
	
	CREATE USER adf_user FOR LOGIN adf_user;
	GO
	
	ALTER ROLE db_owner ADD MEMBER adf_user;
	GO
	
	CREATE SCHEMA raw;
	GO
	
	CREATE SCHEMA stg;
	GO
	
	CREATE SCHEMA dw;
	GO
*/

	-----------------------------------------------
	-- 1). LIMPIEZA Y STAGING DE LA TABLA CUSTOMERS
	-----------------------------------------------
INSERT INTO stg.customers(
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
)
SELECT
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	UPPER(LEFT(customer_city, 1)) + LOWER(SUBSTRING(customer_city, 2, LEN(customer_city))),
	CASE customer_state
	    WHEN 'AC' THEN 'Acre'
	    WHEN 'AL' THEN 'Alagoas'
	    WHEN 'AP' THEN 'Amapa'
	    WHEN 'AM' THEN 'Amazonas'
	    WHEN 'BA' THEN 'Bahia'
	    WHEN 'CE' THEN 'Ceara'
	    WHEN 'DF' THEN 'Distrito Federal'
	    WHEN 'ES' THEN 'Espirito Santo'
	    WHEN 'GO' THEN 'Goias'
	    WHEN 'MA' THEN 'Maranhao'
	    WHEN 'MT' THEN 'Mato Grosso'
	    WHEN 'MS' THEN 'Mato Grosso do Sul'
	    WHEN 'MG' THEN 'Minas Gerais'
	    WHEN 'PA' THEN 'Para'
	    WHEN 'PB' THEN 'Paraiba'
	    WHEN 'PR' THEN 'Parana'
	    WHEN 'PE' THEN 'Pernambuco'
	    WHEN 'PI' THEN 'Piaui'
	    WHEN 'RJ' THEN 'Rio de Janeiro'
	    WHEN 'RN' THEN 'Rio Grande do Norte'
	    WHEN 'RS' THEN 'Rio Grande do Sul'
	    WHEN 'RO' THEN 'Rondonia'
	    WHEN 'RR' THEN 'Roraima'
	    WHEN 'SC' THEN 'Santa Catarina'
	    WHEN 'SP' THEN 'Sao Paulo'
	    WHEN 'SE' THEN 'Sergipe'
	    WHEN 'TO' THEN 'Tocantins'
	END AS geolocation_statetate
FROM 
raw.customers;

	--------------------------------------------------------------------------------------
	-- 2). LIMPIEZA Y STAGING DE LA TABLA GEOLOCATION USANDO LA FUNCIÓN dbo.fn_clean_city
	--------------------------------------------------------------------------------------
INSERT INTO stg.geolocation(
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state
)
SELECT 
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	dbo.fn_clean_city(geolocation_city) AS geolocation_city,
	CASE geolocation_state
	    WHEN 'AC' THEN 'Acre'
	    WHEN 'AL' THEN 'Alagoas'
	    WHEN 'AP' THEN 'Amapa'
	    WHEN 'AM' THEN 'Amazonas'
	    WHEN 'BA' THEN 'Bahia'
	    WHEN 'CE' THEN 'Ceara'
	    WHEN 'DF' THEN 'Distrito Federal'
	    WHEN 'ES' THEN 'Espirito Santo'
	    WHEN 'GO' THEN 'Goias'
	    WHEN 'MA' THEN 'Maranhao'
	    WHEN 'MT' THEN 'Mato Grosso'
	    WHEN 'MS' THEN 'Mato Grosso do Sul'
	    WHEN 'MG' THEN 'Minas Gerais'
	    WHEN 'PA' THEN 'Para'
	    WHEN 'PB' THEN 'Paraiba'
	    WHEN 'PR' THEN 'Parana'
	    WHEN 'PE' THEN 'Pernambuco'
	    WHEN 'PI' THEN 'Piaui'
	    WHEN 'RJ' THEN 'Rio de Janeiro'
	    WHEN 'RN' THEN 'Rio Grande do Norte'
	    WHEN 'RS' THEN 'Rio Grande do Sul'
	    WHEN 'RO' THEN 'Rondonia'
	    WHEN 'RR' THEN 'Roraima'
	    WHEN 'SC' THEN 'Santa Catarina'
	    WHEN 'SP' THEN 'Sao Paulo'
	    WHEN 'SE' THEN 'Sergipe'
	    WHEN 'TO' THEN 'Tocantins'
	END AS geolocation_state
FROM raw.geolocation;

	-------------------------------------------------
	-- 3). LIMPIEZA Y STAGING DE LA TABLA ORDER ITEMS
	-------------------------------------------------
INSERT INTO stg.order_items(
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
)
SELECT
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	ROUND(price,2) AS price,
	ROUND(freight_value,2) AS freight_value
FROM
raw.order_items;

	----------------------------------------------------
	-- 4). LIMPIEZA Y STAGING DE LA TABLA ORDER PAYMENTS
	----------------------------------------------------
INSERT INTO stg.order_payments(
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
)
SELECT
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	round(payment_value,2) AS payment_value
FROM raw.order_payments;

	----------------------------------------------------
	-- 5). LIMPIEZA Y STAGING DE LA TABLA ORDER REVIEWS
	----------------------------------------------------
INSERT INTO stg.order_reviews
(
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
)
SELECT
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	TRY_CONVERT(DATE,review_creation_date) AS review_creation_date,
	TRY_CONVERT(DATETIME,REPLACE(review_answer_timestamp,CHAR(13), '')) AS review_answer_timestamp -- Salto de línea invisible en el origen de datos
FROM raw.order_reviews;

	----------------------------------------------------
	-- 5). LIMPIEZA Y STAGING DE LA TABLA ORDERS
	----------------------------------------------------
INSERT INTO stg.orders(
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date
)
SELECT
	order_id,
	customer_id,
	order_status,
	TRY_CONVERT(DATETIME,order_purchase_timestamp),
	TRY_CONVERT(DATETIME,order_approved_at),
	TRY_CONVERT(DATETIME,order_delivered_carrier_date),
	TRY_CONVERT(DATETIME,order_delivered_customer_date),
	TRY_CONVERT(DATE,order_estimated_delivery_date)
FROM raw.orders;

	----------------------------------------------------
	-- 6). LIMPIEZA Y STAGING DE LA TABLA PRODUCTS
	----------------------------------------------------
/*
-----------------------------AGREGA LAS CATEGORÍAS FALTANTES----------------------------------
INSERT INTO raw.product_category_name_translation
VALUES	('pc_gamer','pc_gamer'),
		('portateis_cozinha_e_preparadores_de_alimentos','portable_kitchen_and_food_preparators')
*/

INSERT INTO stg.products(
	product_id,
	product_category_name,
	product_name_lenght,
	product_description_lenght,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm
)
SELECT
	P.product_id,
	PT.product_category_name_english AS product_category_name,
	P.product_name_lenght,
	P.product_description_lenght,
	P.product_photos_qty,
	P.product_weight_g,
	P.product_length_cm,
	P.product_height_cm,
	P.product_width_cm
FROM raw.product_category_name_translation PT
RIGHT JOIN raw.products P
ON P.product_category_name = PT.product_category_name;

	----------------------------------------------------
	-- 7). LIMPIEZA Y STAGING DE LA TABLA SELLERS
	----------------------------------------------------

INSERT INTO stg.sellers(
seller_id,
seller_zip_code_prefix,
seller_city,
seller_state
)
select
seller_id,
seller_zip_code_prefix,
UPPER(LEFT(seller_city,1)) + LOWER(SUBSTRING(seller_city, 2,LEN(seller_city))),
CASE seller_state
	    WHEN 'AC' THEN 'Acre'
	    WHEN 'AL' THEN 'Alagoas'
	    WHEN 'AP' THEN 'Amapa'
	    WHEN 'AM' THEN 'Amazonas'
	    WHEN 'BA' THEN 'Bahia'
	    WHEN 'CE' THEN 'Ceara'
	    WHEN 'DF' THEN 'Distrito Federal'
	    WHEN 'ES' THEN 'Espirito Santo'
	    WHEN 'GO' THEN 'Goias'
	    WHEN 'MA' THEN 'Maranhao'
	    WHEN 'MT' THEN 'Mato Grosso'
	    WHEN 'MS' THEN 'Mato Grosso do Sul'
	    WHEN 'MG' THEN 'Minas Gerais'
	    WHEN 'PA' THEN 'Para'
	    WHEN 'PB' THEN 'Paraiba'
	    WHEN 'PR' THEN 'Parana'
	    WHEN 'PE' THEN 'Pernambuco'
	    WHEN 'PI' THEN 'Piaui'
	    WHEN 'RJ' THEN 'Rio de Janeiro'
	    WHEN 'RN' THEN 'Rio Grande do Norte'
	    WHEN 'RS' THEN 'Rio Grande do Sul'
	    WHEN 'RO' THEN 'Rondonia'
	    WHEN 'RR' THEN 'Roraima'
	    WHEN 'SC' THEN 'Santa Catarina'
	    WHEN 'SP' THEN 'Sao Paulo'
	    WHEN 'SE' THEN 'Sergipe'
	    WHEN 'TO' THEN 'Tocantins'
	END AS seller_state
from raw.sellers;