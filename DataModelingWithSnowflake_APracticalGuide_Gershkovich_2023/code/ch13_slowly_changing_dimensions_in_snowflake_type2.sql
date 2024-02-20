USE ROLE accountadmin;
USE DATABASE demo_tpch;
USE SCHEMA ch13_dims;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Type 2 SCD = New record
---------------------------------------------------------------------------------------------------------------------

-- Reset source table
CREATE OR REPLACE TABLE src_customer CLONE src_customer_bak;

SELECT COUNT(*) FROM src_customer;  -- 375,000

-- Create base table
CREATE OR REPLACE TABLE dim_customers_t2 (
    customer_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar(15) NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    market_segment varchar(10) NOT NULL,
    comment varchar COMMENT 'Base load of one fourth of total records',
    -- Use timestamps for the TO and FROM columns
    -- This is because this example will perform multiple loads in a given day
    -- NOTE: Most business scenarios would use a date type column
    from_dts timestamp_ntz NOT NULL,
    to_dts timestamp_ntz NOT NULL,
    diff_hash varchar(32) NOT NULL,

    CONSTRAINT pk_customer PRIMARY KEY (customer_id, from_dts)    
)
COMMENT = 'Type 2 SCD for Customer'
AS
SELECT
    customer_id,
    name,
    address,
    location_id,
    phone,
    account_balance_usd,
    market_segment,
    comment,
    __ldts,
    '9999-12-31'::timestamp_ntz,
    MD5(account_balance_usd) -- Hash Type 2 attributes for easy comparisons
FROM src_customer
;

-- Create a STREAM on the Type 2 SCD
CREATE OR REPLACE STREAM strm_dim_customers_t2 ON TABLE dim_customers_t2;

-- Simulate a source load
EXECUTE TASK load_src_customer;

SELECT COUNT(*) FROM src_customer;  -- 376,000 (added 1,000 records)


-- Load Type 2 changes
-- STEP 1: Similar to Type 1 SCD MERGE
MERGE INTO dim_customers_t2 AS dc
USING (
    SELECT
        customer_id,
        name,
        address,
        location_id,
        phone,
        account_balance_usd,
        market_segment,
        comment,
        __ldts,
        MD5(account_balance_usd) AS diff_hash
    FROM src_customer
    -- Get only the latest records from src_customer
    -- In a real-world scenario, create a VIEW to get the latest records to make the logic leaner    
    WHERE __ldts = (SELECT MAX(__ldts) FROM src_customer)
) AS sc
ON
    dc.customer_id = sc.customer_id
    AND dc.to_dts = '9999-12-31' -- get only current/latest records
WHEN NOT MATCHED -- New records, so insert them
THEN INSERT VALUES (
    customer_id,
    name,
    address,
    location_id,
    phone,
    account_balance_usd,
    market_segment,
    comment,
    __ldts, -- FROM date
    '9999-12-31'::timestamp_ntz, -- TO date
    MD5(account_balance_usd) -- Hash type 2 attributes for easy compare
)
WHEN MATCHED -- Record exists, so check for changes
    AND dc.diff_hash != sc.diff_hash -- check for the changes in the SCD
THEN UPDATE
SET
    dc.account_balance_usd = sc.account_balance_usd,
    dc.from_dts = sc.__ldts, -- Update the FROM date to the latest load
    dc.diff_hash = sc.diff_hash
;

-- Should see ~750 records inserted (3/4 new records) and ~100 rows updated (10% were changed)

-- SELECT
--     customer_id,
--     COUNT(*)
-- FROM dim_customers_t2
-- GROUP BY customer_id
-- ORDER BY customer_id
-- -- HAVING COUNT(*) > 1
-- ;

-- SELECT * FROM strm_dim_customers_t2;

-- STEP 2: Update metadata in updated Type 2 attributes
INSERT INTO dim_customers_t2
SELECT
    customer_id,
    name,
    address,
    location_id,
    phone,
    account_balance_usd,
    market_segment,
    comment,
    from_dts, -- original FROM date
    DATEADD(SECOND, -1, new_to_dts), -- Delimit new TO date to be less than the inserted FROM date
    diff_hash
FROM strm_dim_customers_t2 AS strm
INNER JOIN ((SELECT MAX(__ldts) AS new_to_dts FROM src_customer)) -- Get the to_dts for the current load
ON TRUE
    AND strm.metadata$action = 'DELETE' -- Get the before image for updated records
WHERE TRUE
;

-- Should see the same number of rows that were updated in the previous step INSERTED in this step (~75-100)

-- Recreate the stream because it now contains the inserted (updated) records
--  - This step is optional because our logic filters on  strm.metadata$action = 'DELETE', but it's cleaner
CREATE OR REPLACE STREAM strm_dim_customer_t2 ON TABLE dim_customers_t2;








---------------------------------------------------------------------------------------------------------------------
-- Embed steps 1 and 2 from above into a TASK tree for easy loading
---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TASK tsk_load_dim_customer_t2
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
AS
SELECT TRUE
;

-- Create MERGE task (Step 1)
CREATE OR REPLACE TASK tsk_load_1_dim_customer_t2
WAREHOUSE = demo_wh
AFTER tsk_load_dim_customer_t2
AS
MERGE INTO dim_customers_t2 AS dc
-- Get only the latest records from src_customer
-- In a real-world scenario, create a VIEW to get the latest records to make the logic leaner
USING (
    SELECT
        customer_id,
        name,
        address,
        location_id,
        phone,
        account_balance_usd,
        market_segment,
        comment,
        __ldts,
        MD5(account_balance_usd) AS diff_hash
    FROM src_customer
    WHERE __ldts = (SELECT MAX(__ldts) FROM src_customer)
) AS sc
ON
    dc.customer_id = sc.customer_id
    AND dc.to_dts = '9999-12-31' -- get only current/latest records
WHEN NOT MATCHED -- New records, so insert them
THEN INSERT VALUES (
    customer_id,
    name,
    address,
    location_id,
    phone,
    account_balance_usd,
    market_segment,
    comment,
    __ldts, -- FROM date
    '9999-12-31'::timestamp_ntz, -- TO date
    MD5(account_balance_usd) -- Hash type 2 attributes for easy compare
)
WHEN MATCHED -- Record exists, so check for changes
    AND dc.diff_hash != sc.diff_hash -- check for the changes in the SCD
THEN UPDATE
SET
    dc.account_balance_usd = sc.account_balance_usd,
    dc.from_dts = sc.__ldts, -- Update the FROM date to the latest load
    dc.diff_hash = sc.diff_hash
;

ALTER TASK tsk_load_1_dim_customer_t2 resume;

-- Create INSERT task (Step 2)
CREATE OR REPLACE TASK tsk_load_2_dim_customer_t2
WAREHOUSE = demo_wh
AFTER tsk_load_1_dim_customer_t2
AS
INSERT INTO dim_customer_t2
SELECT
    customer_id,
    name,
    address,
    location_id,
    phone,
    account_balance_usd,
    market_segment,
    comment,
    from_dts, -- original FROM date
    DATEADD(SECOND, -1, new_to_dts), -- Delimit new TO date to be less than the inserted FROM date
    diff_hash
FROM strm_dim_customers_t2 AS strm
INNER JOIN ((SELECT MAX(__ldts) AS new_to_dts FROM src_customer)) -- Get the to_dts for the current load
ON TRUE
    AND strm.metadata$action = 'DELETE' -- Get the before image for updated records
WHERE TRUE
;

ALTER TASK tsk_load_2_dim_customer_t2 resume;

-- Simulate a source load
EXECUTE TASK load_src_customer;

-- SELECT COUNT(*) FROM dim_customers_t2;

-- Load Type 2 SCD's
EXECUTE TASK tsk_load_dim_customer_t2;

-- SELECT COUNT(*) FROM dim_customers_t2;
-- SELECT * FROM strm_dim_customers_t2;
