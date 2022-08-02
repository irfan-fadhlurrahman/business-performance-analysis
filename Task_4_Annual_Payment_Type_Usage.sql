WITH payment_types_usage AS (
-- 1. Total usage of various payment types from 2016 - 2018
    SELECT 
        DISTINCT payment_type,
        COUNT(payment_type) OVER (
            PARTITION BY payment_type
        ) AS total_usage
    FROM 
        order_payments
    ORDER BY 
        2 DESC
),
-- 2. Annual payment usage
annual_payment_usage AS (
    SELECT 
        DISTINCT payment_type,
        SUM(CASE year WHEN 2016 THEN usage_count ELSE 0 END) AS "year_2016",
        SUM(CASE year WHEN 2017 THEN usage_count ELSE 0 END) AS "year_2017",
        SUM(CASE year WHEN 2018 THEN usage_count ELSE 0 END) AS "year_2018"
    FROM (
        SELECT 
            DISTINCT EXTRACT(year FROM o.order_purchase_timestamp) AS year,
            p.payment_type,
            COUNT(p.payment_type) OVER (
                PARTITION BY p.payment_type, EXTRACT(year FROM o.order_purchase_timestamp)
            ) AS usage_count
        FROM 
            order_payments AS p
        JOIN 
            orders AS o ON p.order_id = o.order_id
    ) AS subquery
    GROUP BY 
        1
    ORDER BY 
        2 DESC
)
-- 3. Combine all query into one table
SELECT 
    p.payment_type, 
    p.total_usage,
    a.year_2016, 
    a.year_2017, 
    a.year_2018,
    ROUND(100 * (a.year_2018 - a.year_2017) / NULLIF(a.year_2017, 0), 1) AS pct_change_2017_2018
FROM 
    payment_types_usage AS p
JOIN 
    annual_payment_usage AS a ON p.payment_type = a.payment_type