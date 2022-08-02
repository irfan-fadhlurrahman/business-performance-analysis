WITH orders_joined_order_items AS (
-- Reusable table for the rest of query below
    SELECT 
        EXTRACT(year FROM o.order_purchase_timestamp) AS year,
        o.order_id, 
        o.order_status,
        i.product_id, 
        i.price,
        i.freight_value
    FROM 
        orders AS o 
    JOIN 
        order_items AS i ON o.order_id = i.order_id
),
-- 1. Total revenue per year
total_revenue AS (
    SELECT 
        year, 
        ROUND(SUM(total_price), 0) AS total_revenue
    FROM (
        SELECT 
            year,
            SUM(price + freight_value) AS total_price
        FROM 
            orders_joined_order_items
        WHERE 
            order_status = 'delivered'
        GROUP BY
            1
    ) AS subquery
    GROUP BY
        1
    ORDER BY
        1
),
-- 2. Total number of canceled orders per year
canceled_order AS (
    SELECT 
        year,
        COUNT(order_id) AS total_canceled_order
    FROM 
        orders_joined_order_items
    WHERE
        order_status = 'canceled'
    GROUP BY
        1
    ORDER BY
        1
),
-- 3. Top product category that generates most revenue
top_product_categories AS (
    SELECT 
        year, 
        top_product_category,
        top_product_revenue
    FROM (
        SELECT 
            year,
            p.product_category_name AS top_product_category,
            SUM(o.price + o.freight_value) AS top_product_revenue,
            RANK () OVER (
                PARTITION BY year
                ORDER BY SUM(o.price + o.freight_value) DESC
            ) AS rank
        FROM 
            orders_joined_order_items as o
        JOIN 
            product as p ON o.product_id = p.product_id
        WHERE  
            order_status = 'delivered' 
        GROUP BY
            1, 2
    ) AS subquery
    WHERE
        rank = 1
    ORDER BY
        1
),
-- 4. Top product category that mostly canceled by customers
cancel_product_categories AS (
    SELECT 
        year, 
        most_canceled_product,
        total_cancellation
    FROM (
        SELECT 
            year,
            p.product_category_name AS most_canceled_product,
            COUNT(p.product_category_name) AS total_cancellation,
            RANK () OVER (
                PARTITION BY year
                ORDER BY COUNT(p.product_category_name) DESC
            ) AS rank 
        FROM 
            orders_joined_order_items as o
        JOIN 
            product as p ON o.product_id = p.product_id
        WHERE  
            order_status = 'canceled' 
        GROUP BY
            1, 2
    ) AS subquery
    WHERE 
        rank = 1
    ORDER BY
        1
)
-- 5. Combine all metrics into one table
SELECT 
    tr.year,
    tr.total_revenue,
    co.total_canceled_order,
    tpc.top_product_category,
    tpc.top_product_revenue,
    cpc.most_canceled_product,
    cpc.total_cancellation
FROM
    total_revenue AS tr
JOIN
    canceled_order AS co ON tr.year = co.year
JOIN
    top_product_categories AS tpc ON tr.year = tpc.year
JOIN
    cancel_product_categories AS cpc ON tr.year = cpc.year