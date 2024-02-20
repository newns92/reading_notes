--------------------------------------------------------------------
-- Setting up environments
--------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE demo_tpch;
CREATE OR REPLACE SCHEMA time_travel_except;
USE WAREHOUSE demo_wh;

-- Transient tables persist until explicitly dropped and are available to all users with the appropriate privileges
-- Transient tables are similar to permanent tables with the key difference that they do NOT have a Fail-safe period
CREATE OR REPLACE TRANSIENT TABLE customer (
    customer_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar(15) NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    market_segment varchar(10) NOT NULL,
    comment varchar COMMENT 'User comments',

    CONSTRAINT pk_customer PRIMARY KEY (customer_id)
)
COMMENT = 'Loaded from Snowflake sample data (tpch_sf10.customer)'
AS
SELECT
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
FROM snowflake_sample_data.tpch_sf10.customer
;

SELECT COUNT(*) FROM customer;
SELECT * FROM customer SAMPLE (5 ROWS);


--------------------------------------------------------------------
-- Perform a random/complex update 
--------------------------------------------------------------------
-- Uses a randomly generated filter
SET before_time = CURRENT_TIMESTAMP();

UPDATE customer
SET
    account_balance_usd = account_balance_usd + 1
WHERE TRUE
    AND RIGHT(customer_id, 4) = RIGHT(RANDOM(), 4)
;
-- Should see about 150-200 records updated


-- Recall the SELECT syntax, including time travel options
/*
	SELECT ...
	FROM ...
	  {
	   AT( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) |
	   BEFORE( STATEMENT => <id> )
	  }
	[ ... ]
*/
-- https://docs.snowflake.com/en/user-guide/data-time-travel
-- https://quickstarts.snowflake.com/guide/getting_started_with_time_travel/index.html#0


-- Recall the Snowflake set operators:
-- { INTERSECT | { MINUS | EXCEPT } | UNION [ ALL ] } 
-- https://docs.snowflake.com/en/sql-reference/operators-query


-- Count how many records were updated by selecting the PK and changed column from the original table using Time Travel
-- Compare it to the current version
SELECT
    COUNT(*) AS cnt
FROM (
    SELECT
        customer_id,
        account_balance_usd
    FROM customer
    AT(TIMESTAMP => $before_time) AS customer_before

    EXCEPT -- Like MINUS, removes rows from a query’s result set which appear in another query’s result set, with duplicate elimination

    -- To be removed (the account balances that didn't change)
    SELECT
        customer_id,
        account_balance_usd
    FROM customer AS customer_now
)
;
-- Should see same number as "rows updated" from the UPDATE command above


-- Get the before and after values side-by-side
WITH updated AS (
    -- Get the list of changed PK's
    SELECT
        customer_id,
        account_balance_usd
    FROM customer
    AT(TIMESTAMP => $before_time) AS customer_before

    EXCEPT -- Like MINUS, removes rows from a query’s result set which appear in another query’s result set, with duplicate elimination

    -- To be removed (the account balances that didn't change)
    SELECT
        customer_id,
        account_balance_usd
    FROM customer AS customer_now
),

original AS (
    -- Get the before values
    SELECT
        customer_id,
        account_balance_usd
    FROM customer
    AT(TIMESTAMP => $before_time) AS customer_before
),

now AS (
    -- Get the after values
    SELECT
        customer_id,
        account_balance_usd
    FROM customer
)
-- JOIN the before and after values to see side-by-side
SELECT
    original.customer_id,
    original.account_balance_usd AS acct_bal_before,
    now.account_balance_usd AS acct_bal_now
FROM original
INNER JOIN now
    ON now.customer_id = original.customer_id -- USING (customer_id)
-- Get just those that changes
INNER JOIN updated
    ON updated.customer_id = original.customer_id -- USING (customer_id)
;
