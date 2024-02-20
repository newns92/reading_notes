USE ROLE accountadmin;
USE DATABASE demo_tpch;
USE SCHEMA operations;

SELECT
    customer.name AS customer_name,
    location.name AS location_name
FROM customer
JOIN location ON
    customer.location_id = location.location_id
WHERE customer.customer_id = '775699'
;

-- Blindly trying to obtain a customer name from the SALES_ORDER table can have unexpected consequences
-- As the relational model/diagram indicates, a customer may place multiple orders
-- Therefore, CUSTOMER_ID is not unique in the SALES_ORDER table
-- Attempting to join CUSTOMER to SALES_ORDER to obtain customer name would not be as straightforward as in the previous example
-- Although the resulting query would look similar
SELECT
    customer.name AS customer_name
FROM sales_order
JOIN customer ON
    customer.customer_id = sales_order.customer_id
WHERE sales_order.customer_id = '775699'
;
-- However, the query returns multiple records in place of a single name (1 per order made by customer 775699): 


-- Even if a join is specified in a query, but no columns from the joined table are selected as part of the result, the RELY property will tell the Snowflake query engine to avoid performing the join
-- If we modify the previous query (joining CUSTOMER + LOCATION tables) but only request information from CUSTOMER, then RELY will help us avoid the unnecessary join operation
SELECT
    customer.name AS customer_name
FROM customer
JOIN location ON
    customer.location_id = location.location_id
WHERE customer.customer_id = '775699'
;
-- This can be seen in the query profile of a query in the query history section of the main Snowflake UI


SELECT * FROM location WHERE location_id = 22;