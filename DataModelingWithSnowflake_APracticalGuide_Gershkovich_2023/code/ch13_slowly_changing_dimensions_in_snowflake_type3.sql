USE ROLE accountadmin;
USE DATABASE demo_tpch;
USE SCHEMA ch13_dims;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Type 3 SCD = New Column
---------------------------------------------------------------------------------------------------------------------

-- Reset source table
CREATE OR REPLACE TABLE src_customer CLONE src_customer_bak;

-- Create base table
-- NOTE: Before introducing a Type 3 SCD in this example, it resembles a Type 1 SCD
CREATE OR REPLACE TABLE dim_customers_t3 (
    customer_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar(15) NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    market_segment varchar(10) NOT NULL,
    comment varchar COMMENT 'Base load of one fourth of total records',
    diff_hash varchar(32) NOT NULL,
    __ldts timestamp_ntz NOT NULL DEFAULT current_timestamp() COMMENT 'Load date of the latest source record',

    CONSTRAINT pk_customer PRIMARY KEY (customer_id)
)
COMMENT = 'Type 3 SCD for Customer'
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
    MD5(account_balance_usd), -- Hash Type 3 attributes for easy comparisons
    __ldts
FROM src_customer
;

SELECT * FROM dim_customers_t3 SAMPLE (10 ROWS);


-- Add a type 3 dimension and instantiate it
ALTER TABLE dim_customers_t3 ADD COLUMN original_account_balance_usd number(12, 2);

UPDATE dim_customers_t3
SET original_account_balance_usd = account_balance_usd
;
-- Should see a result that all 375,000 rows were updated

SELECT * FROM dim_customers_t3 SAMPLE (10 ROWS);


-- Simulate a source loud
EXECUTE TASK load_src_customer;

SELECT COUNT(*) FROM src_customer; -- 376,000 (added 1,000 rows)


-- Load Type 1 data into the Type 3 SCD
MERGE INTO dim_customers_t3 AS dc
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
    __ldts,
    account_balance_usd -- This is the Type 3 column that will NOT be updated going forward
)
WHEN MATCHED -- Record exists, so check for changes
    AND dc.diff_hash != sc.diff_hash -- check for the changes in the Type 1 SCD
THEN UPDATE
SET
    dc.account_balance_usd = sc.account_balance_usd,
    dc.__ldts = sc.__ldts, -- To indicate when last updated
    dc.diff_hash = sc.diff_hash
;
-- Should see ~750 records inserted (3/4 new records) and ~100 rows updated (10% were changed)

SELECT * FROM dim_customers_t3 WHERE account_balance_usd != original_account_balance_usd;
-- Should return the same number of rows as updated from the MERGE + INSERT/UPDATE command above
