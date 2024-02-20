--------------------------------------------------------------------
-- Setting up environments
--------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE demo_tpch;
CREATE OR REPLACE SCHEMA secret_virtual_columns;
USE WAREHOUSE demo_wh;


-- Virtual Columns = A secret feature that sits between physical and transformational modeling
-- https://community.snowflake.com/s/question/0D50Z00008ixGQKSA2/does-snowflake-supports-computed-columns-in-a-table-while-creating-table
-- They look like normal table columns, but their values are derived rather than stored on disc
-- They're an efficient way to embed simple business rules and transformational logic in a table
--      w/out the overhead of maintaining views and incurring storage costs


-- Create a table with physical, virtual, and DEFAULT columns
CREATE OR REPLACE TRANSIENT TABLE default_v_virtual_demo (
    customer_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    -- Create a virtual column as an expression
    v_Name varchar NOT NULL AS ('Hi, my name is ' || name), -- 'AS' syntax not in documentation as of 2023-04-08 but is supported
    -- Create the same load date column as a DEFAULT *and* as a virtual column
    load_dts timestamp_ltz DEFAULT CURRENT_TIMESTAMP(),
    v_load_dts timestamp_ltz AS CURRENT_TIMESTAMP()
)
;

-- Insert data
INSERT INTO default_v_virtual_demo (
    customer_id,
    name,
    load_dts
)
VALUES
    (1, 'Serge', DEFAULT), -- Use the default value for the load_dts column
    (2, 'Bill', NULL) -- Use a NULL value for the load_dts column
;

-- Check the data
SELECT * FROM default_v_virtual_demo;
-- Notice that DEFAULT columns are static, while virtual columns are dynamic (LOAD_DTS vs V_LOAD_DTS)

-- Notice that virtual columns are stored differently from physical (kind - 'VIRTUAL')
DESC TABLE default_v_virtual_demo;

-- See the same information in the information schema
SELECT
    *
FROM information_schema.columns
WHERE TRUE
    AND table_schema = 'SECRET_VIRTUAL_COLUMNS'
;



-- Sample usage of virtual columns in a physical table creation
CREATE OR REPLACE TRANSIENT TABLE customer (
    -- Add a message without using storage
    sys_message varchar AS 'Legacy data, do not use in reporting without re-mapping',
    customer_id number(38, 0) NOT NULL,
    -- Add basic business rules without duplicating data
    legacy_customer_id varchar AS ('x' || customer_id) COMMENT 'Legacy systems included an "X" prefix for customer IDs',
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar(15) NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    market_segment varchar(10) NOT NULL,
    -- Add more advanved business rules without using storage
    tax_amount_usd number(12, 2) NOT NULL AS (
        CASE
            WHEN market_segment = 'MACHINERY' THEN account_balance_usd * 0.1
            WHEN market_segment = 'AUTOMOBILE' THEN account_balance_usd * 0.15
            ELSE account_balance_usd * 0.2
        END
    ),
    comment varchar COMMENT 'User comments',

    CONSTRAINT pk_customer PRIMARY KEY (customer_id)
)
COMMENT = 'Loaded from Snowflake sample data (tpch_sf10.customer)'
AS
SELECT
    -- Notice that virtual columns are ignored in the insert column order
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
FROM snowflake_sample_data.tpch_sf10.customer
SAMPLE (1000 ROWS)
;

-- Review the resulting values
SELECT * FROM customer SAMPLE (10 ROWS);

-- Notice that the expressions are preserved in the DDL
-- Albeit in a slightly different format after being translated by Snowflake's query optimizer
SELECT GET_DDL('table', 'customer');
