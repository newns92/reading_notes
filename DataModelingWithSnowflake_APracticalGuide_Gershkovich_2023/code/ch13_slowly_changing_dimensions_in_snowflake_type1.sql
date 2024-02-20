USE ROLE accountadmin;
USE DATABASE demo_tpch;
USE SCHEMA ch13_dims;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Type 1 SCD = Overwrite
---------------------------------------------------------------------------------------------------------------------

-- Reset source table
CREATE OR REPLACE TABLE src_customer CLONE src_customer_bak;

-- Create base table
CREATE OR REPLACE TABLE dim_customers_t1 (
    customer_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar(15) NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    market_segment varchar(10) NOT NULL,
    comment varchar COMMENT 'Base load of one fourth of total records',
    diff_hash varchar(32) NOT NULL,
    __ldts timestamp_ntz NOT NULL DEFAULT current_timestamp() COMMENT 'Load datetime of latest source record',

    CONSTRAINT pk_customer PRIMARY KEY (customer_id)    
)
COMMENT = 'Type 1 SCD for Customer'
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
    MD5(account_balance_usd), -- Hash Type 1 attributes for easy comparisons
    __ldts
FROM src_customer
;

-- Simulate a source load
EXECUTE TASK load_src_customer;

-- Load Type 1 records
MERGE INTO dim_customers_t1 AS dc
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
ON dc.customer_id = sc.customer_id
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
    diff_hash,
    __ldts
)
WHEN MATCHED -- Record exists, so check for changes
    AND dc.diff_hash != sc.diff_hash -- check for the changes in the SCD
THEN UPDATE
SET
    dc.account_balance_usd = sc.account_balance_usd,
    dc.__ldts = sc.__ldts, -- Indicates when last updated
    dc.diff_hash = sc.diff_hash
;

-- Should see ~750 records inserted (3/4 new records) and ~100 rows updated (10% were changed)