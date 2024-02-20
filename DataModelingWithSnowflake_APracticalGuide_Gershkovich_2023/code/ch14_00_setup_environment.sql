-- Set up the environment
USE ROLE accountadmin;
USE DATABASE demo_tpch;
CREATE OR REPLACE SCHEMA ch14_facts;
USE SCHEMA ch14_facts;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Prepare Base tables
---------------------------------------------------------------------------------------------------------------------

-- "Source System" sample data
CREATE OR REPLACE TABLE source_system_line_item (
    line_number number(38, 0) NOT NULL,
    sales_order_id number(38, 0) NOT NULL,
    part_id number(38, 0) NOT NULL,
    supplier_id number(38, 0) NOT NULL,
    quantity number(12, 2),
    extended_price_usd number(12, 2),
    discount_percent number(12, 2),
    tax_percent number(12, 2),
    return_flag varchar(1),
    line_status varchar(1),
    ship_date date,
    commit_date date,
    receipt_date date,
    ship_instructions varchar(25),
    ship_mode varchar(10),
    comment varchar(44),

    CONSTRAINT pk_line_item PRIMARY KEY (line_number, sales_order_id) -- NOTE: No RELY property = PK constraint NOT enforced
)
COMMENT = 'Contains various line items per sales order'
AS
SELECT
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment 
FROM snowflake_sample_data.tpch_sf10.lineitem
;

-- Create Data Warehouse landing area for source system data
CREATE OR REPLACE TABLE src_line_item (
    line_number number(38, 0) NOT NULL,
    sales_order_id number(38, 0) NOT NULL,
    part_id number(38, 0) NOT NULL,
    supplier_id number(38, 0) NOT NULL,
    quantity number(12, 2),
    extended_price_usd number(12, 2),
    discount_percent number(12, 2),
    tax_percent number(12, 2),
    return_flag varchar(1),
    line_status varchar(1),
    ship_date date,
    commit_date date,
    receipt_date date,
    ship_instructions varchar(25),
    ship_mode varchar(10),
    comment varchar(44),
    __ldts timestamp_ntz,
    __load_type varchar, -- For testing

    CONSTRAINT pk_line_item PRIMARY KEY (line_number, sales_order_id, __ldts) -- NOTE: No RELY property = PK constraint NOT enforced
)
COMMENT = 'Contains various line items per sales order'
AS
-- These examples require line item orders to be loaded + processed in their entirety (containing all constituent line items for a given order)
-- To accomplish this, order IDs are first selected at random from the sample set, and then all related line items are loaded
WITH complete_orders AS (
    SELECT
        DISTINCT sales_order_id
    FROM source_system_line_item SAMPLE (10000 ROWS)
)
SELECT
    src.line_number,
    src.sales_order_id,
    src.part_id,
    src.supplier_id,
    src.quantity,
    src.extended_price_usd,
    src.discount_percent,
    src.tax_percent,
    src.return_flag,
    src.line_status,
    src.ship_date,
    src.commit_date,
    src.receipt_date,
    src.ship_instructions,
    src.ship_mode,
    src.comment,
    current_timestamp(),
    'initial'
FROM source_system_line_item AS src
-- Load all related line items to the ones randomly sampled
INNER JOIN complete_orders AS co
    ON src.sales_order_id = co.sales_order_id
;

-- Create backup of Data Warehouse landing area for running examples
CREATE OR REPLACE TABLE src_line_item_bak CLONE src_line_item;