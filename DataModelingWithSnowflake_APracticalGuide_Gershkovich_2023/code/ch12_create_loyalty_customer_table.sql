USE ROLE accountadmin;
USE DATABASE demo_tpch;
USE SCHEMA operations;

CREATE TABLE IF NOT EXISTS loyalty_customer (
    customer_id number(38, 0) NOT NULL,
    level varchar NOT NULL COMMENT 'Customer loyalty status (Gold, Silver, or Bronze) calculated from sales order volume',
    type varchar NOT NULL COMMENT 'Early supporter or frequent shopper',
    points_amount number NOT NULL COMMENT 'Loyalty points score',
    comments varchar,

    CONSTRAINT pk_loyalty_customer PRIMARY KEY (customer_id) RELY,
    CONSTRAINT fk_loyalty_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) RELY
)
COMMENT = 'Client loyalty program with gold, silver, bronze statuses'
AS

WITH cust AS (
    SELECT
        customer_id,
        name,
        address,
        location_id,
        phone,
        account_balance_usd,
        market_segment,
        comment
    FROM customer
),

ord AS (
    SELECT
        sales_order_id,
        customer_id,
        order_status,
        total_price_usd,
        order_date,
        order_priority,
        clerk,
        ship_priority,
        comment
    FROM sales_order
),

cust_ord AS (
    SELECT
        customer_id,
        SUM(total_price_usd) AS total_price_usd 
    FROM (
        SELECT
            ord.customer_id,
            ord.total_price_usd
        FROM ord
        INNER JOIN cust ON
            ord.customer_id = cust.customer_id
        -- https://pushmetrics.io/blog/why-use-where-1-1-in-sql-queries-exploring-the-surprising-benefits-of-a-seemingly-redundant-clause/
        -- easily concatenate additional conditions using the AND operator without worrying about whether it is the first condition or not
        WHERE TRUE
            AND cust.account_balance_usd > 0 -- no negative balances
            AND cust.location_id != 22
    )
    GROUP BY customer_id
),

-- Get the top 400 customers by total_price_usd 
top_four_hundred AS (
    SELECT
        cust_ord.customer_id,
        cust_ord.total_price_usd,
        DENSE_RANK() OVER (ORDER BY total_price_usd DESC) AS cust_level,
        CASE
            WHEN cust_level BETWEEN 1 AND 20 THEN 'gold'
            WHEN cust_level BETWEEN 21 AND 100 THEN 'silver'
            WHEN cust_level BETWEEN 101 AND 400 THEN 'bronze'
        END AS loyalty_level
    FROM cust_ord
    -- QUALIFY clause filters the results of window functions
    -- QUALIFY does with window functions what HAVING does with aggregate functions + GROUP BY clauses
    -- https://docs.snowflake.com/en/sql-reference/constructs/qualify
    QUALIFY cust_level <= 400 -- return only top 400 customers
    ORDER BY cust_level ASC
),

early_supporters AS (
    SELECT
        -- $1 is a reference to the first argument of the function
        $1 AS customer_id
    -- VALUES: https://docs.snowflake.com/en/sql-reference/constructs/values
    FROM VALUES (349642), (896215), (350965), (404707), (509986)
),

-- Combine the two sets of loyalty customers
all_loyalty AS (
    SELECT
        customer_id,
        loyalty_level,
        'top 400' AS type
    FROM top_four_hundred
    
    UNION ALL -- does NOT remove duplicates

    SELECT
        customer_id,
        'gold' AS loyalty_level,
        'early supporter' AS type
    FROM early_supporters    
),

-- Rename some columns
rename AS (
    SELECT
        customer_id,
        loyalty_level AS level,
        type,
        0 AS points_amount, -- This will be updated by the marketing team
        '' AS comments
    FROM all_loyalty
)

SELECT
    customer_id,
    level,
    type,
    points_amount,
    comments
FROM rename
;


SELECT
    customer_id,
    level,
    type,
    points_amount,
    comments
FROM loyalty_customer
SAMPLE (10 ROWS)
;



-- The marketing team decides that a weekly refresh is insufficient, + that the data needs to be updated in real time
CREATE VIEW IF NOT EXISTS loyalty_customer_real_time AS
WITH cust AS (
    SELECT
        customer_id,
        name,
        address,
        location_id,
        phone,
        account_balance_usd,
        market_segment,
        comment
    FROM customer
),

ord AS (
    SELECT
        sales_order_id,
        customer_id,
        order_status,
        total_price_usd,
        order_date,
        order_priority,
        clerk,
        ship_priority,
        comment
    FROM sales_order
),

cust_ord AS (
    SELECT
        customer_id,
        SUM(total_price_usd) AS total_price_usd 
    FROM (
        SELECT
            ord.customer_id,
            ord.total_price_usd
        FROM ord
        INNER JOIN cust ON
            ord.customer_id = cust.customer_id
        -- https://pushmetrics.io/blog/why-use-where-1-1-in-sql-queries-exploring-the-surprising-benefits-of-a-seemingly-redundant-clause/
        -- easily concatenate additional conditions using the AND operator without worrying about whether it is the first condition or not
        WHERE TRUE
            AND cust.account_balance_usd > 0 -- no negative balances
            AND cust.location_id != 22
    )
    GROUP BY customer_id
),

-- Get the top 400 customers by total_price_usd 
top_four_hundred AS (
    SELECT
        cust_ord.customer_id,
        cust_ord.total_price_usd,
        DENSE_RANK() OVER (ORDER BY total_price_usd DESC) AS cust_level,
        CASE
            WHEN cust_level BETWEEN 1 AND 20 THEN 'gold'
            WHEN cust_level BETWEEN 21 AND 100 THEN 'silver'
            WHEN cust_level BETWEEN 101 AND 400 THEN 'bronze'
        END AS loyalty_level
    FROM cust_ord
    -- QUALIFY clause filters the results of window functions
    -- QUALIFY does with window functions what HAVING does with aggregate functions + GROUP BY clauses
    -- https://docs.snowflake.com/en/sql-reference/constructs/qualify
    QUALIFY cust_level <= 400 -- return only top 400 customers
    ORDER BY cust_level ASC
),

early_supporters AS (
    SELECT
        -- $1 is a reference to the first argument of the function
        $1 AS customer_id
    -- VALUES: https://docs.snowflake.com/en/sql-reference/constructs/values
    FROM VALUES (349642), (896215), (350965), (404707), (509986)
),

-- Combine the two sets of loyalty customers
all_loyalty AS (
    SELECT
        customer_id,
        loyalty_level,
        'top 400' AS type
    FROM top_four_hundred
    
    UNION ALL -- does NOT remove duplicates

    SELECT
        customer_id,
        'gold' AS loyalty_level,
        'early supporter' AS type
    FROM early_supporters    
),

-- Rename some columns
rename AS (
    SELECT
        customer_id,
        loyalty_level AS level,
        type,
        0 AS points_amount, -- This will be updated by the marketing team
        '' AS comments
    FROM all_loyalty
)

SELECT
    customer_id,
    level,
    type,
    points_amount,
    comments
FROM rename
;


SELECT
    customer_id,
    level,
    type,
    points_amount,
    comments
FROM loyalty_customer_real_time
SAMPLE (10 ROWS)
;