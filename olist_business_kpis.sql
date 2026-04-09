USE OlistDW;

-- =====================================
-- OLIST BUSINESS KPI ANALYSIS
-- Author: Anthony Ccasani
-- Description: KPI queries for business analysis
-- =====================================



------------MÉTRICAS DE REVENUE Y CRECIMIENTO-------------

-- Ventas por mes

SELECT
    d.year,
    d.month,
    SUM(f.total_order_value) AS revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    AVG(f.total_order_value) AS avg_order_value
FROM dw.fact_orders f
JOIN dw.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Crecimiento mensual (%)

WITH revenue_cte AS (
    SELECT
        d.year,
        d.month,
        SUM(f.total_order_value) AS revenue
    FROM dw.fact_orders f
    JOIN dw.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT *,
    LAG(revenue) OVER (ORDER BY year, month) AS prev_revenue,
    (revenue - LAG(revenue) OVER (ORDER BY year, month)) * 1.0 
        / LAG(revenue) OVER (ORDER BY year, month) AS growth_rate
FROM revenue_cte;

------------PERFORMANCE DE ENTREGAS-----------------------

-- Tiempo promedio de entrega

SELECT
    AVG(delivery_days) AS avg_delivery_days,
    AVG(delivery_delay) AS avg_delay
FROM dw.fact_orders;

-- % de entregas tardías

SELECT
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dw.fact_orders) AS late_delivery_pct
FROM dw.fact_orders
WHERE delivery_delay > 0;

--	Entregas por Estado

SELECT
    c.state,
    AVG(f.delivery_days) AS avg_delivery_days
FROM dw.fact_orders f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY avg_delivery_days DESC;

------------MÉTRICAS DE CLIENTES--------------------------

-- Top Clientes por Revenue

SELECT TOP 10
    c.customer_unique_id,
    SUM(f.total_order_value) AS total_spent
FROM dw.fact_orders f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_unique_id
ORDER BY total_spent DESC;

-- Customer Lifetime Value (CLV)

SELECT
    c.customer_unique_id,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_order_value) AS total_spent,
    AVG(f.total_order_value) AS avg_ticket
FROM dw.fact_orders f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_unique_id;

------------MÉTRICAS DE PRODUCTOS--------------------------

-- Top Productos

SELECT TOP 10
    p.category_name_english,
    SUM(f.total_value) AS revenue
FROM dw.fact_order_items f
JOIN dw.dim_product p ON f.product_key = p.product_key
GROUP BY p.category_name_english
ORDER BY revenue DESC;

-- Productos más vendidos

SELECT TOP 10
    p.category_name_english,
    COUNT(*) AS total_items
FROM dw.fact_order_items f
JOIN dw.dim_product p ON f.product_key = p.product_key
GROUP BY p.category_name_english
ORDER BY total_items DESC;

-- Ticket promedio por producto

SELECT
    p.category_name_english,
    AVG(f.total_value) AS avg_item_value
FROM dw.fact_order_items f
JOIN dw.dim_product p ON f.product_key = p.product_key
GROUP BY p.category_name_english;

------------MÉTRICAS DE SELLERS--------------------------

-- Top Sellers

SELECT TOP 10
    s.seller_id,
    SUM(f.total_order_value) AS revenue
FROM dw.fact_orders f
JOIN dw.dim_seller s ON f.seller_key = s.seller_key
GROUP BY s.seller_id
ORDER BY revenue DESC;

-- Sellers con peor performance (entregas)

SELECT TOP 10
    s.seller_id,
    AVG(f.delivery_delay) AS avg_delay
FROM dw.fact_orders f
JOIN dw.dim_seller s ON f.seller_key = s.seller_key
GROUP BY s.seller_id
ORDER BY avg_delay DESC;

------------MÉTRICAS DE SATISFACCIÓN AL CLIENTE--------------

-- Score promedio

SELECT
	AVG(avg_review_score) AS avg_review_score
FROM dw.fact_orders;

-- Score vs tiempo de entrega

SELECT
    CASE 
        WHEN delivery_days <= 3 THEN 'Fast'
        WHEN delivery_days <= 7 THEN 'Medium'
        ELSE 'Slow'
    END AS delivery_speed,
    AVG(avg_review_score) AS avg_review
FROM dw.fact_orders
GROUP BY
    CASE 
        WHEN delivery_days <= 3 THEN 'Fast'
        WHEN delivery_days <= 7 THEN 'Medium'
        ELSE 'Slow'
    END;

------------FUNNEL DE NEGOCIO--------------------------

SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    AVG(total_order_value) AS avg_ticket,
    AVG(delivery_days) AS avg_delivery,
    AVG(avg_review_score) AS avg_satisfaction
FROM dw.fact_orders;

------------ANÁLISIS GEOGRÁFICO------------------------

-- Ventas por estado

SELECT
    c.state,
    SUM(f.total_order_value) AS revenue
FROM dw.fact_orders f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY revenue DESC;

------------ANALISIS DE RETENCIÓN DE CLIENTES POR COHORTES------------------------

WITH cohort AS (
    SELECT
        c.customer_unique_id,
        MIN(d.full_date) AS first_purchase
    FROM dw.fact_orders f
    JOIN dw.dim_customer c ON f.customer_key = c.customer_key
    JOIN dw.dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_unique_id
),
activity AS (
    SELECT
        c.customer_unique_id,
        d.full_date
    FROM dw.fact_orders f
    JOIN dw.dim_customer c ON f.customer_key = c.customer_key
    JOIN dw.dim_date d ON f.date_key = d.date_key
)
SELECT
    YEAR(first_purchase) AS cohort_year,
    MONTH(first_purchase) AS cohort_month,
    DATEDIFF(MONTH, first_purchase, full_date) AS months_since_first,
    COUNT(DISTINCT a.customer_unique_id) AS active_users
FROM cohort c
JOIN activity a ON c.customer_unique_id = a.customer_unique_id
GROUP BY
    YEAR(first_purchase),
    MONTH(first_purchase),
    DATEDIFF(MONTH, first_purchase, full_date)
ORDER BY 1,2,3;

------------RFM SEGMENTATION----------------------------------------------------

WITH rfm AS (
    SELECT
        c.customer_unique_id,
        MAX(d.full_date) AS last_purchase,
        COUNT(DISTINCT f.order_id) AS frequency,
        SUM(f.total_order_value) AS monetary
    FROM dw.fact_orders f
    JOIN dw.dim_customer c ON f.customer_key = c.customer_key
    JOIN dw.dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_unique_id
)
SELECT *,
    DATEDIFF(DAY, last_purchase, GETDATE()) AS recency_days
FROM rfm;

------------CUSTOMER CHURN----------------------------------------------------

SELECT
    customer_unique_id,
    MAX(d.full_date) AS last_purchase,
    CASE 
        WHEN DATEDIFF(DAY, MAX(d.full_date), GETDATE()) > 90 THEN 'Churned'
        ELSE 'Active'
    END AS status
FROM dw.fact_orders f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
JOIN dw.dim_date d ON f.date_key = d.date_key
GROUP BY customer_unique_id;

------------DELIVERY PERFORMANCE SEGMENTATION--------------------------------------------

SELECT
    CASE 
        WHEN delivery_delay <= 0 THEN 'On Time'
        WHEN delivery_delay <= 3 THEN 'Slight Delay'
        ELSE 'Severe Delay'
    END AS delivery_status,
    COUNT(*) AS total_orders,
    AVG(avg_review_score) AS avg_review
FROM dw.fact_orders
GROUP BY
    CASE 
        WHEN delivery_delay <= 0 THEN 'On Time'
        WHEN delivery_delay <= 3 THEN 'Slight Delay'
        ELSE 'Severe Delay'
    END;

------------MARKET BASKET----------------------------------------------------------

SELECT
    p1.category_name_english AS product_1,
    p2.category_name_english AS product_2,
    COUNT(*) AS times_bought_together
FROM dw.fact_order_items i1
JOIN dw.fact_order_items i2 
    ON i1.order_id = i2.order_id 
   AND i1.product_key < i2.product_key
JOIN dw.dim_product p1 ON i1.product_key = p1.product_key
JOIN dw.dim_product p2 ON i2.product_key = p2.product_key
GROUP BY p1.category_name_english, p2.category_name_english
ORDER BY times_bought_together DESC;


------------CONTRIBUTION ANALYSIS (PARETO 80/20)----------------------------------------------------------

WITH revenue AS (
    SELECT
        p.category_name_english,
        SUM(f.total_order_value) AS revenue
    FROM dw.fact_orders f
    JOIN dw.dim_product p ON f.product_key = p.product_key
    GROUP BY p.category_name_english
)
SELECT *,
    SUM(revenue) OVER (ORDER BY revenue DESC) * 1.0 
        / SUM(revenue) OVER () AS cumulative_pct
FROM revenue
ORDER BY revenue DESC;

------------CUSTOMER SEGMENT BY LOCATION----------------------------------------------------------

SELECT
    l.state,
    COUNT(DISTINCT c.customer_unique_id) AS customers,
    AVG(f.total_order_value) AS avg_spent
FROM dw.fact_orders f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
JOIN dw.dim_location l ON c.location_key = l.location_key
GROUP BY l.state
ORDER BY avg_spent DESC;

------------ORDER BEHAVIOR ANALYSIS (horario)----------------------------------------------------------

SELECT
    DATEPART(HOUR, d.full_date) AS hour,
    COUNT(*) AS total_orders
FROM dw.fact_orders f
JOIN dw.dim_date d ON f.date_key = d.date_key
GROUP BY DATEPART(HOUR, d.full_date)
ORDER BY hour;

------------CUSTOMER REPEAT RATE----------------------------------------------------------

SELECT
    COUNT(DISTINCT customer_unique_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_unique_id END) AS repeat_customers
FROM (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT f.order_id) AS order_count
    FROM dw.fact_orders f
    JOIN dw.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_unique_id
) t;

------------AOV + DISTRIBUCIÓN----------------------------------------------------------

SELECT
    CASE 
        WHEN total_order_value < 50 THEN 'Low'
        WHEN total_order_value < 150 THEN 'Medium'
        ELSE 'High'
    END AS order_bucket,
    COUNT(*) AS orders
FROM dw.fact_orders
GROUP BY
    CASE 
        WHEN total_order_value < 50 THEN 'Low'
        WHEN total_order_value < 150 THEN 'Medium'
        ELSE 'High'
    END;