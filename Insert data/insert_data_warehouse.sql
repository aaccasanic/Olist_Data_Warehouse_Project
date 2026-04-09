USE OlistDW;

-------------------CUSTOMERS--------------------------------------

INSERT INTO dw.dim_customer(
	customer_id,
	customer_unique_id,
	city,
	state
)
SELECT
	customer_id,
	customer_unique_id,
	customer_city,
	customer_state
FROM stg.customers;

-------------------PRODUCTS---------------------------------------

INSERT INTO dw.dim_product(
	product_id,
	category_name,
	category_name_english
)
SELECT
	p.product_id,
	t.product_category_name,
	p.product_category_name
FROM stg.products p
LEFT JOIN raw.product_category_name_translation t 
	ON p.product_category_name = t.product_category_name_english;

-------------------SELLERS----------------------------------------

INSERT INTO dw.dim_seller(
	seller_id,
	city,
	state
)
SELECT
	seller_id,
	seller_city,
	seller_state
FROM stg.sellers;

-------------------DATE-------------------------------------------

WITH dates AS (
    SELECT CAST('2016-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM dates
    WHERE d < '2020-12-31'
)
INSERT INTO dw.dim_date
SELECT
    CONVERT(INT, CONVERT(VARCHAR(8), d, 112)),
    d,
    YEAR(d),
    MONTH(d),
    DAY(d),
    DATENAME(MONTH, d)
FROM dates
OPTION (MAXRECURSION 0);

-------------------LOCATION---------------------------------------

INSERT INTO dw.dim_location(
zip_code_prefix,
city,
state,
lat,
lng
)
SELECT
	geolocation_zip_code_prefix,
	geolocation_city,
	geolocation_state,
	AVG(geolocation_lat),
	AVG(geolocation_lng)
FROM stg.geolocation
GROUP BY
	geolocation_zip_code_prefix,
	geolocation_city,
	geolocation_state;

-------------------FACT TABLE ORDERS-------------------------------------

INSERT INTO dw.fact_orders(
    order_id,
    customer_key,
    product_key,
    seller_key,
    date_key,
    total_order_value,
    total_freight,
    total_items,
    delivery_days,
    delivery_delay,
    avg_review_score
)
SELECT
	o.order_id,
	
	dc.customer_key,
	dp.product_key,
	ds.seller_key,

	CONVERT(INT, FORMAT(o.order_purchase_timestamp,'yyyyMMdd')),

	SUM(oi.price + oi.freight_value),
	SUM(oi.freight_value),
	COUNT(oi.order_item_id),

	DATEDIFF(DAY, o.order_purchase_timestamp,o.order_estimated_delivery_date),
	DATEDIFF(DAY, o.order_estimated_delivery_date,o.order_delivered_customer_date),

	AVG(r.review_score)
	
FROM stg.orders o

JOIN stg.order_items oi ON o.order_id = oi.order_id

LEFT JOIN stg.order_reviews r ON r.order_id = o.order_id

JOIN dw.dim_customer dc ON o.customer_id = dc.customer_id
JOIN dw.dim_product dp ON oi.product_id = dp.product_id
JOIN dw.dim_seller ds ON oi.seller_id = ds.seller_id

GROUP BY
    o.order_id,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date;

-------------------FACT TABLE ORDER ITEMS-------------------------------------

INSERT INTO dw.fact_order_items (
    order_id,
    customer_key,
    product_key,
    seller_key,
    date_key,
    order_item_id,
    price,
    freight_value,
    total_value
)
SELECT
    oi.order_id,

    dc.customer_key,
    dp.product_key,
    ds.seller_key,

    CONVERT(INT, CONVERT(VARCHAR(8), o.order_purchase_timestamp, 112)),

    oi.order_item_id,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value AS total_value

FROM stg.order_items oi

JOIN stg.orders o 
    ON oi.order_id = o.order_id

JOIN dw.dim_customer dc 
    ON o.customer_id = dc.customer_id

JOIN dw.dim_product dp 
    ON oi.product_id = dp.product_id

JOIN dw.dim_seller ds 
    ON oi.seller_id = ds.seller_id;


------------CONEXIÓN DE LOCATION CON CUSTOMERS Y SELLERS----------

ALTER TABLE dw.dim_customer
ADD location_key INT;

UPDATE c
SET c.location_key = l.location_key
FROM dw.dim_customer c
JOIN dw.dim_location l
    ON c.city = l.city
   AND c.state = l.state;

ALTER TABLE dw.dim_seller
ADD location_key INT;

UPDATE s
SET s.location_key = l.location_key
FROM dw.dim_seller s
JOIN dw.dim_location l
    ON s.city = l.city
   AND s.state = l.state;