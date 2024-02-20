USE ROLE accountadmin;

-- Set up the warehouse and environment
CREATE WAREHOUSE IF NOT EXISTS demo_wh WAREHOUSE_SIZE = XSMALL;

USE DATABASE demo_tpch;
CREATE OR REPLACE SCHEMA ch13_dims;

---------------------------------------------------------------------------------------------------------------------
-- Prepare the 3 base tables and the task
---------------------------------------------------------------------------------------------------------------------

-- source_system_customer will simulate the day one snapshot of the DW raw/source schema
-- - Represents the initial first load of the source data into the DW
CREATE OR REPLACE TABLE source_system_customer (
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
COMMENT = 'Loaded from snowflake_sample_data.tpch_sf10.customer'
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

-- Created simulated SRC_CUSTOMER table to represent the landing area of the DW
-- By default, will contain one quarter of the 1.5 million records of the sample CUSTOMER table
CREATE OR REPLACE TABLE src_customer (
    customer_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar(15) NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    market_segment varchar(10) NOT NULL,
    comment varchar COMMENT 'Base load of one fourth of total records',
    __ldts timestamp_ntz NOT NULL DEFAULT current_timestamp(),

    CONSTRAINT pk_customer PRIMARY KEY (customer_id, __ldts)
)
COMMENT = 'Source customers for loading changes'
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
    current_timestamp()
FROM source_system_customer
WHERE TRUE
    AND MOD(customer_id, 4) = 0 -- Load in just 1/4 of total customer data
;

-- Create a clone of src_customer for future exercises
CREATE OR REPLACE TABLE src_customer_bak CLONE src_customer;

-- Create a TASK to randomly load 1,000 records into the SRC_CUSTOMER table
-- https://docs.snowflake.com/en/user-guide/tasks-intro
CREATE OR REPLACE TASK load_src_customer
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
AS
INSERT INTO src_customer (
    SELECT
        customer_id,
        name,
        address,
        location_id,
        phone,
        -- If customer_id is divisible by a 3, vary the balance amount to insert modified records
        -- IFF = Single-level if-then-else expression that is similar to CASE, but only allows a single condition
        IFF(MOD(customer_id, 3) = 0,
            -- IF
            (account_balance_usd + random() / 100000000000000000)::number(32, 2),
            -- ELSE
            account_balance_usd
            ),
        market_segment,
        comment,
        current_timestamp()
    FROM operations.customer SAMPLE (1000 ROWS)
)
;