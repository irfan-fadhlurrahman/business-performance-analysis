WITH orders_customers_joined_1 AS (
    SELECT 
        c.customer_unique_id, 
        o.order_purchase_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp
        ) AS row_number
    FROM 
        orders AS o
    JOIN 
        customers AS c ON o.customer_id = c.customer_id
),
active_customers AS (
    SELECT 
        year, 
        ROUND(AVG(total_customer_per_month), 0) AS avg_monthly_active_user
    FROM (
        SELECT 
            EXTRACT(year FROM order_purchase_timestamp) AS year,
            EXTRACT(month FROM order_purchase_timestamp) AS month,
            COUNT(DISTINCT customer_unique_id) AS total_customer_per_month
        FROM 
            orders_customers_joined_1
        GROUP BY 
            1, 2   
    ) AS subquery
    GROUP BY 
        1
),
new_customers AS (
    SELECT 
        EXTRACT(year FROM order_purchase_timestamp) AS year,
        COUNT(customer_unique_id) AS total_new_customer
    FROM 
        orders_customers_joined_1
    WHERE
        row_number = 1
    GROUP BY 
        1
    ORDER BY 
        1
),
orders_customers_joined_2 AS (
    SELECT
        c.customer_unique_id,
        o.order_purchase_timestamp,
        ROW_NUMBER() OVER (
        PARTITION BY c.customer_unique_id, EXTRACT(year FROM o.order_purchase_timestamp)
        ) AS row_number
    FROM 
        orders AS o
    JOIN 
        customers AS c ON o.customer_id = c.customer_id
),
repeat_customers AS (
    SELECT 
        EXTRACT(year FROM order_purchase_timestamp) AS year,
        COUNT(customer_unique_id) AS total_customer
    FROM orders_customers_joined_2
    WHERE 
        row_number = 2
    GROUP BY 
        1
),
order_frequency AS (
    SELECT 
        year, 
        ROUND(AVG(total_order), 3) AS avg_order_freq
    FROM (
        SELECT 
            customer_unique_id,
            EXTRACT(year FROM order_purchase_timestamp) AS year,
            MAX(row_number) AS total_order
        FROM 
            orders_customers_joined_2
        GROUP BY
            1, 2    
    ) AS subquery
    GROUP BY 
        1
    ORDER BY 
        1
)
SELECT
    ac.year,
    ac.avg_monthly_active_user,
    nc.total_new_customer,
    rc.total_customer AS total_repeated_customer,
    of.avg_order_freq AS avg_total_order
    
FROM
    active_customers AS ac
JOIN 
    new_customers AS nc ON ac.year = nc.year
JOIN 
    repeat_customers AS rc ON ac.year = rc.year
JOIN 
    order_frequency AS of ON ac.year = of.year