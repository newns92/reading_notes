-- Set up the environment
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE DATABASE data_vault;
USE DATABASE data_vault;
CREATE OR REPLACE SCHEMA L0_src COMMENT = 'Schema for landing area objects';
CREATE OR REPLACE SCHEMA L1_rdv COMMENT = 'Schema for Raw Vault objects';
USE WAREHOUSE demo_wh;

--------------------------------------------------------------------
-- Set up the landing area
--------------------------------------------------------------------
USE SCHEMA L0_src;

CREATE OR REPLACE TABLE src_nation (
    iso2_code varchar(2) NOT NULL,
    n_nationkey number(38, 0) NOT NULL,
    n_name varchar(25) NOT NULL,
    n_regionkey number(38, 0) NOT NULL,
    n_comment varchar(152),
    load_dts timestamp_ntz NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_src_nation PRIMARY KEY (n_nationkey),
    CONSTRAINT ak_src_nation_n_name UNIQUE (n_name),
    CONSTRAINT ak_src_nation_iso2_code UNIQUE (iso2_code)
)
COMMENT = 'ISO 3166 2-letter country codes'
AS
SELECT
    codes.code,
    codes.n_nationkey,
    codes.n_name,
    codes.n_regionkey,
    codes.n_comment,
    CURRENT_TIMESTAMP(),
    'sys 1'
FROM (
    SELECT
        iso.code,
        nation.n_nationkey,
        nation.n_name,
        nation.n_regionkey,
        nation.n_comment
    FROM snowflake_sample_data.tpch_sf10.nation AS nation
    INNER JOIN (
        SELECT
            $1 AS id,
            $2 AS code
        FROM VALUES
            (0, 'AL'),
            (1, 'AR'),
            (2, 'BR'),
            (3, 'CA'),
            (4, 'EG'),
            (5, 'ET'),
            (6, 'FR'),
            (7, 'DE'),
            (8, 'IN'),
            (9, 'ID'),
            (10, 'IR'),
            (11, 'IQ'),
            (12, 'JP'),
            (13, 'JO'),
            (14, 'KE'),
            (15, 'MA'),
            (16, 'MZ'),
            (17, 'PE'),
            (18, 'CN'),
            (19, 'RO'),
            (20, 'SA'),
            (21, 'VN'),
            (22, 'RU'),
            (23, 'GB'),
            (24, 'US')        
    ) AS iso
        ON nation.n_nationkey = iso.id
) AS codes
;


CREATE OR REPLACE TABLE src_customer (
    c_custkey number(38, 0) NOT NULL,
    c_name varchar(25),
    c_address varchar(40),
    iso2_code varchar(2) NOT NULL,
    c_phone varchar(15),
    c_acctbal number(12, 2),
    c_mktsegment varchar(10),
    c_comment varchar,
    load_dts timestamp_ntz NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_src_customer PRIMARY KEY (c_custkey)
)
COMMENT = 'Registered customers with or without previous orders from source system 1'
;

CREATE OR REPLACE TABLE src_orders (
    o_orderkey number(38, 0) NOT NULL,
    o_custkey number(38, 0) NOT NULL,
    o_orderstatus varchar(1),
    o_totalprice number(12, 2),
    o_orderdate date,
    o_orderpriority varchar(15),
    o_clerk varchar(15),
    o_shippriority number(38, 0),
    o_comment varchar,
    load_dts timestamp_ntz NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_src_orders PRIMARY KEY (o_orderkey)
)
COMMENT = 'Customer order headers'
;


--------------------------------------------------------------------
-- Simulate data loads from source systems
--------------------------------------------------------------------
-- Create streams for outboud loads to the Raw Vault
CREATE OR REPLACE STREAM src_customer_strm ON TABLE src_customer;
CREATE OR REPLACE STREAM src_orders_strm ON TABLE src_orders;

-- Task to simulate a subset of daily records
CREATE OR REPLACE TASK load_daily_init
WAREHOUSE = demo_wh
SCHEDULE = '10 minutes'
AS
CREATE OR REPLACE TRANSIENT TABLE current_load
    AS
        SELECT DISTINCT
            c_custkey AS custkey
        FROM snowflake_sample_data.tpch_sf10.customer
        SAMPLE (1000 ROWS)
;

CREATE OR REPLACE TASK load_src_customer
WAREHOUSE = demo_wh
AFTER load_daily_init
AS
INSERT INTO src_customer (
    SELECT
        c_custkey,
        c_name,
        c_address,
        iso2_code,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment,
        CURRENT_TIMESTAMP(),
        'sys 1'
    FROM snowflake_sample_data.tpch_sf10.customer AS customer
    INNER JOIN current_load
        ON customer.c_custkey = current_load.custkey
    INNER JOIN src_nation AS nation
        ON customer.c_nationkey = nation.n_nationkey
)
;

CREATE OR REPLACE TASK load_src_orders
WAREHOUSE = demo_wh
AFTER load_daily_init
AS
INSERT INTO src_orders (
    SELECT
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment,
        CURRENT_TIMESTAMP(),
        'sys 1'
    FROM snowflake_sample_data.tpch_sf10.orders AS orders
    INNER JOIN current_load
        ON orders.o_custkey = current_load.custkey
)
;

-- Start the tasks
ALTER TASK load_src_customer RESUME;
ALTER TASK load_src_orders RESUME;


-- Do a load
EXECUTE TASK load_daily_init;

-- Save a trip to the task history page
SELECT
    *
FROM TABLE(information_schema.task_history())
ORDER BY scheduled_time DESC
;

-- Verify that records are loaded from a "source" system
SELECT
    'order' AS tbl,
    COUNT(DISTINCT load_dts) AS loads,
    COUNT(*) AS count
FROM src_orders
GROUP BY tbl
UNION ALL
SELECT
    'customer' AS tbl,
    COUNT(DISTINCT load_dts) AS loads,
    COUNT(*) AS count
FROM src_customer
GROUP BY tbl
;

SELECT n_nationkey, iso2_code, n_name FROM src_nation LIMIT 5;



--------------------------------------------------------------------
-- Create views for loading the Raw Vault
--------------------------------------------------------------------
CREATE OR REPLACE VIEW src_customer_strm_outbound
AS
SELECT
    -- Source columns
    c_custkey,
    c_name,
    c_address,
    iso2_code,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    load_dts,
    rec_src,
    metadata$action,
    metadata$isupdate,
    metadata$row_id,
    -- Business key hash
    SHA1_BINARY(UPPER(TRIM(c_custkey))) AS hub_customer_hk,
    -- Record hash diff
    SHA1_BINARY(UPPER(
                    -- Returns an input array converted to a string by casting all values to strings (using TO_VARCHAR) 
                    --  and concatenating them (using the string from the second argument to separate the elements)
                    ARRAY_TO_STRING(
                        -- Return an array constructed from zero, one, or more inputs
                        -- https://docs.snowflake.com/en/sql-reference/functions/array_construct
                        ARRAY_CONSTRUCT(
                            -- Replace NULLs with 'x'
                            NVL(TRIM(c_name), 'x'),
                            NVL(TRIM(c_address), 'x'),
                            NVL(TRIM(iso2_code), 'x'),
                            NVL(TRIM(c_phone), 'x'),
                            NVL(TRIM(c_acctbal), 'x'),
                            NVL(TRIM(c_mktsegment), 'x'),
                            NVL(TRIM(c_comment), 'x')
                        )
                    -- Concatenation delimiter for ARRAY_TO_STRING()
                    , '^')
                )
            ) AS customer_hash_diff
FROM src_customer_strm
;

CREATE OR REPLACE VIEW src_order_strm_outbound
AS
SELECT
    -- Source columns
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    load_dts,
    rec_src,
    metadata$action,
    metadata$isupdate,
    metadata$row_id,
    -- Business key hashes
    SHA1_BINARY(UPPER(TRIM(o_orderkey))) AS hub_order_hk,
    SHA1_BINARY(UPPER(TRIM(o_custkey))) AS hub_customer_hk,
    SHA1_BINARY(UPPER(
                    -- Returns an input array converted to a string by casting all values to strings (using TO_VARCHAR) 
                    --  and concatenating them (using the string from the second argument to separate the elements)
                    ARRAY_TO_STRING(
                        -- Return an array constructed from zero, one, or more inputs
                        -- https://docs.snowflake.com/en/sql-reference/functions/array_construct
                        ARRAY_CONSTRUCT(
                            -- Replace NULLs with 'x'
                            NVL(TRIM(o_orderkey), 'x'),
                            NVL(TRIM(o_custkey), 'x')                            
                        )
                    , '^')
                )
            ) AS lnk_customer_order_hk,
    -- Record hash diff
    SHA1_BINARY(UPPER(
                    -- Returns an input array converted to a string by casting all values to strings (using TO_VARCHAR) 
                    --  and concatenating them (using the string from the second argument to separate the elements)
                    ARRAY_TO_STRING(
                        -- Return an array constructed from zero, one, or more inputs
                        -- https://docs.snowflake.com/en/sql-reference/functions/array_construct
                        ARRAY_CONSTRUCT(
                            -- Replace NULLs with 'x'
                            NVL(TRIM(o_orderstatus), 'x'),
                            NVL(TRIM(o_totalprice), 'x'),
                            NVL(TRIM(o_orderdate), 'x'),
                            NVL(TRIM(o_orderpriority), 'x'),
                            NVL(TRIM(o_clerk), 'x'),
                            NVL(TRIM(o_shippriority), 'x'),
                            NVL(TRIM(o_comment), 'x')
                        )
                    -- Concatenation delimiter for ARRAY_TO_STRING()
                    , '^')
                )
            ) AS order_hash_diff
FROM src_orders_strm
;


--------------------------------------------------------------------
-- Set up Raw Vault
--------------------------------------------------------------------
USE SCHEMA L1_rdv;

-- Hub 1 (collection of business keys belonging to a business entity, such as customer and order)
CREATE OR REPLACE TABLE hub_customer (
    hub_customer_hk binary NOT NULL,
    c_custkey number(38, 0) NOT NULL,
    load_dts timestamp_ntz(9) NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_hub_customer PRIMARY KEY (hub_customer_hk)
)
;

-- Hub 2 (collection of business keys belonging to a business entity, such as customer and order)
CREATE OR REPLACE TABLE hub_order (
    hub_order_hk binary NOT NULL,
    o_orderkey number(38, 0) NOT NULL,
    load_dts timestamp_ntz(9) NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_hub_order PRIMARY KEY (hub_order_hk)
)
;

-- Reference table (descriptive data about information in satellites or other Data Vault objects but does NOT warrant a business key)
CREATE OR REPLACE TABLE ref_nation (
    iso2_code varchar(2) NOT NULL,
    n_nationkey number(38, 0) NOT NULL,
    n_regionkey number(38, 0) NOT NULL,
    n_name varchar,
    n_comment varchar,
    load_dts timestamp_ntz(9) NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_ref_nation PRIMARY KEY (iso2_code),
    CONSTRAINT ak_ref_nation UNIQUE (n_nationkey)
)
AS
SELECT
    iso2_code,
    n_nationkey,
    n_regionkey,
    n_name,
    n_comment,
    load_dts,
    rec_src
FROM L0_src.src_nation
;

-- Satellite 1 (store the attributes in a Data Vault and provide change history)
CREATE OR REPLACE TABLE sat_sys1_customer (
    hub_customer_hk binary NOT NULL,
    load_dts timestamp_ntz(9) NOT NULL,
    c_name varchar,
    c_address varchar,    
    c_phone varchar,
    c_acctbal number(38, 0),
    c_mktsegment varchar,
    c_comment varchar,
    iso2_code varchar(2) NOT NULL,
    hash_diff binary NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_sat_sys1_customer PRIMARY KEY (hub_customer_hk, load_dts),
    CONSTRAINT fk_sat_sys1_customer_hcust FOREIGN KEY (hub_customer_hk) REFERENCES hub_customer (hub_customer_hk),
    CONSTRAINT fk_set_customer_rnation FOREIGN KEY (iso2_code) REFERENCES ref_nation (iso2_code)
)
;

-- Satellite 2 (store the attributes in a Data Vault and provide change history)
CREATE OR REPLACE TABLE sat_sys1_order (
    hub_order_hk binary NOT NULL,
    load_dts timestamp_ntz(9) NOT NULL,
    o_orderstatus varchar,
    o_totalprice number(38, 0),
    o_orderdate date,
    o_orderpriority varchar,
    o_clerk varchar,
    o_shippriority number(38, 0),
    o_comment varchar,
    hash_diff binary NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_sat_sys1_order PRIMARY KEY (hub_order_hk, load_dts),
    CONSTRAINT fk_sat_sys1_order_horder FOREIGN KEY (hub_order_hk) REFERENCES hub_order (hub_order_hk)
)
;

-- Link (store the intersection of business keys (or the FKs) from related hubs)
--  - Like fact tables without facts/attributes, just business keys
CREATE OR REPLACE TABLE lnk_customer_order (
    lnk_customer_order_hk binary NOT NULL,
    hub_customer_hk binary NOT NULL,
    hub_order_hk binary NOT NULL,
    load_dts timestamp_ntz(9) NOT NULL,
    rec_src varchar NOT NULL,

    CONSTRAINT pk_lnk_customer_order PRIMARY KEY (lnk_customer_order_hk),
    CONSTRAINT fk_link_customer_order_hcust FOREIGN KEY (hub_customer_hk) REFERENCES hub_customer (hub_customer_hk),
    CONSTRAINT fk_link_customer_order_horder FOREIGN KEY (hub_order_hk) REFERENCES hub_order (hub_order_hk)
)
;



--------------------------------------------------------------------
-- Load the Raw Vault using multi-table inserts
--------------------------------------------------------------------
-- Allows us to take a single subquery that selects from the source table and load the associated links, hubs, and satellites in parallel
CREATE OR REPLACE TASK customer_strm_tsk
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
WHEN
    SYSTEM$STREAM_HAS_DATA('L0_SRC.SRC_CUSTOMER_STRM')
AS
INSERT ALL
-- Make sure record does not already exist in the hub by checking the business key
WHEN (
        SELECT
            COUNT(1)
        FROM hub_customer AS target
        WHERE target.hub_customer_hk = src_hub_customer_hk
    ) = 0
-- If it's a new business key, insert it into the hub
THEN INTO hub_customer (
    hub_customer_hk,
    c_custkey,
    load_dts,
    rec_src
)
VALUES (
    src_hub_customer_hk,
    src_c_custkey,
    src_load_dts,
    src_rec_src
)
-- Make sure record does not already exist in the satellite by checking the business key
WHEN (
        SELECT
            COUNT(1)
        FROM sat_sys1_customer AS target
        WHERE target.hub_customer_hk = src_hub_customer_hk
            -- If it's a new business key, insert it into the satellite ONLY if there are changed based on the hash diff
            AND target.hash_diff = src_customer_hash_diff
    ) = 0
-- If it's a new, unchanged business key OR there are changes detected (based on hash_diff), insert it into the satellite
THEN INTO sat_sys1_customer (
    hub_customer_hk,
    load_dts,
    c_name,
    c_address,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    iso2_code,
    hash_diff,
    rec_src
)
VALUES (
    src_hub_customer_hk,
    src_load_dts,
    src_c_name,
    src_c_address,
    src_c_phone,
    src_c_acctbal,
    src_c_mktsegment,
    src_c_comment,
    src_iso2_code,
    src_customer_hash_diff,
    src_rec_src
)
-- Final subquery
SELECT
    hub_customer_hk AS src_hub_customer_hk,
    c_custkey AS src_c_custkey,
    c_name AS src_c_name,
    c_address AS src_c_address,
    iso2_code AS src_iso2_code,
    c_phone AS src_c_phone,
    c_acctbal AS src_c_acctbal,
    c_mktsegment AS src_c_mktsegment,
    c_comment AS src_c_comment,
    customer_hash_diff AS src_customer_hash_diff,
    load_dts AS src_load_dts,
    rec_src AS src_rec_src
FROM l0_src.src_customer_strm_outbound AS src
;


CREATE OR REPLACE TASK order_strm_tsk
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
WHEN
    SYSTEM$STREAM_HAS_DATA('L0_SRC.SRC_ORDERS_STRM')
AS
INSERT ALL
-- Make sure record does not already exist in the hub by checking the business key
WHEN (
        SELECT
            COUNT(1)
        FROM hub_order AS target
        WHERE target.hub_order_hk = src_hub_order_hk
    ) = 0
-- If it's a new business key, insert it into the hub
THEN INTO hub_order (
    hub_order_hk,
    o_orderkey,
    load_dts,
    rec_src
)
VALUES (
    src_hub_order_hk,
    src_o_orderkey,
    src_load_dts,
    src_rec_src
)
-- Make sure record does not already exist in the satellite by checking the business key
WHEN (
        SELECT
            COUNT(1)
        FROM sat_sys1_order AS target
        WHERE target.hub_order_hk = src_hub_order_hk
            -- If it's a new business key, insert it into the satellite ONLY if there are changed based on the hash diff
            AND target.hash_diff = src_order_hash_diff
    ) = 0
-- If it's a new, unchanged business key OR there are changes detected (based on hash_diff), insert it into the satellite
THEN INTO sat_sys1_order (
    hub_order_hk,
    load_dts,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    hash_diff,
    rec_src
)
VALUES (
    src_hub_order_hk,
    src_load_dts,
    src_o_orderstatus,
    src_o_totalprice,
    src_o_orderdate,
    src_o_orderpriority,
    src_o_clerk,
    src_o_shippriority,
    src_o_comment,
    src_order_hash_diff,
    src_rec_src
)
-- Make sure record does not already exist in the LINK
WHEN (
        SELECT
            COUNT(1)
        FROM lnk_customer_order AS target
        WHERE target.lnk_customer_order_hk = src_lnk_customer_order_hk
    ) = 0
THEN INTO lnk_customer_order (
    lnk_customer_order_hk,
    hub_customer_hk,
    hub_order_hk,
    load_dts,
    rec_src
)
VALUES (
    src_lnk_customer_order_hk,
    src_hub_customer_hk,
    src_hub_order_hk,
    src_load_dts,
    src_rec_src
)
-- Final subquery
SELECT
    hub_order_hk AS src_hub_order_hk,
    lnk_customer_order_hk AS src_lnk_customer_order_hk,
    hub_customer_hk AS src_hub_customer_hk,
    o_orderkey AS src_o_orderkey,
    o_orderstatus AS src_o_orderstatus,
    o_totalprice AS src_o_totalprice,
    o_orderdate AS src_o_orderdate,
    o_orderpriority AS src_o_orderpriority,
    o_clerk AS src_o_clerk,
    o_shippriority AS src_o_shippriority,
    o_comment AS src_o_comment,
    order_hash_diff AS src_order_hash_diff,
    load_dts AS src_load_dts,
    rec_src AS src_rec_src
FROM l0_src.src_order_strm_outbound AS src
;





-- Check the data beforehand
SELECT
    'hub_customer' AS src,
    count(1) AS cnt 
FROM hub_customer
UNION ALL
SELECT
    'hub_order',
    count(1)
FROM hub_order
UNION ALL
SELECT
    'sat_sys1_customer',
    count(1)
FROM sat_sys1_customer
UNION ALL
SELECT
    'sat_sys1_order',
    count(1)
FROM sat_sys1_order
UNION ALL
SELECT
    'ref_nation',
    count(1)
FROM ref_nation
UNION ALL 
SELECT
    'lnk_customer_order',
    count(1)
FROM lnk_customer_order
UNION ALL
SELECT
    'L0_src.src_customer_strm_outbound',
    count(1)
FROM l0_src.src_customer_strm_outbound
UNION ALL
SELECT
    'L0_src.src_order_strm_outbound',
    count(1)
FROM l0_src.src_order_strm_outbound
;


-- Execute the tasks to load the data
EXECUTE TASK customer_strm_tsk;
EXECUTE TASK order_strm_tsk;


SELECT
    *
FROM TABLE(information_schema.task_history())
ORDER BY scheduled_time DESC
;


-- Check the data afterwards
SELECT
    'hub_customer' AS src,
    count(1) AS cnt 
FROM hub_customer
UNION ALL
SELECT
    'hub_order',
    count(1)
FROM hub_order
UNION ALL
SELECT
    'sat_sys1_customer',
    count(1)
FROM sat_sys1_customer
UNION ALL
SELECT
    'sat_sys1_order',
    count(1)
FROM sat_sys1_order
UNION ALL
SELECT
    'ref_nation',
    count(1)
FROM ref_nation
UNION ALL 
SELECT
    'lnk_customer_order',
    count(1)
FROM lnk_customer_order
UNION ALL
SELECT
    'L0_src.src_customer_strm_outbound',
    count(1)
FROM l0_src.src_customer_strm_outbound
UNION ALL
SELECT
    'L0_src.src_order_strm_outbound',
    count(1)
FROM l0_src.src_order_strm_outbound
;



-- Load more source records and repeat the previous tasks to load them into the DV
EXECUTE TASK L0_src.load_daily_init;

-- Probe the task history programatically instead of using Snowsight UI
SELECT
    *
FROM table(information_schema.task_history())
ORDER BY scheduled_time DESC
;


-- Check the data afterwards
SELECT
    'hub_customer' AS src,
    count(1) AS cnt 
FROM hub_customer
UNION ALL
SELECT
    'hub_order',
    count(1)
FROM hub_order
UNION ALL
SELECT
    'sat_sys1_customer',
    count(1)
FROM sat_sys1_customer
UNION ALL
SELECT
    'sat_sys1_order',
    count(1)
FROM sat_sys1_order
UNION ALL
SELECT
    'ref_nation',
    count(1)
FROM ref_nation
UNION ALL 
SELECT
    'lnk_customer_order',
    count(1)
FROM lnk_customer_order
UNION ALL
SELECT
    'L0_src.src_customer_strm_outbound',
    count(1)
FROM l0_src.src_customer_strm_outbound
UNION ALL
SELECT
    'L0_src.src_order_strm_outbound',
    count(1)
FROM l0_src.src_order_strm_outbound
;


-- Execute the tasks to load the data
EXECUTE TASK customer_strm_tsk;
EXECUTE TASK order_strm_tsk;


SELECT
    *
FROM TABLE(information_schema.task_history())
ORDER BY scheduled_time DESC
;


-- Check the data afterwards
SELECT
    'hub_customer' AS src,
    count(1) AS cnt 
FROM hub_customer
UNION ALL
SELECT
    'hub_order',
    count(1)
FROM hub_order
UNION ALL
SELECT
    'sat_sys1_customer',
    count(1)
FROM sat_sys1_customer
UNION ALL
SELECT
    'sat_sys1_order',
    count(1)
FROM sat_sys1_order
UNION ALL
SELECT
    'ref_nation',
    count(1)
FROM ref_nation
UNION ALL 
SELECT
    'lnk_customer_order',
    count(1)
FROM lnk_customer_order
UNION ALL
SELECT
    'L0_src.src_customer_strm_outbound',
    count(1)
FROM l0_src.src_customer_strm_outbound
UNION ALL
SELECT
    'L0_src.src_order_strm_outbound',
    count(1)
FROM l0_src.src_order_strm_outbound
;