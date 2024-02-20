 
-- Foreign Key references are NOT recorded in the information schema
-- You can track them with provided Snowflake functions or even populate your own metadata table
-- https://sql.yt/show-imported-keys.html

--------------------------------------------------------------------
-- Setting up environments
--------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE demo_tpch;
CREATE OR REPLACE SCHEMA fkref;
USE WAREHOUSE demo_wh;

CREATE OR REPLACE TABLE customer (
    customer_id NUMBER(38, 0),
    name varchar,

    CONSTRAINT pk_customer PRIMARY KEY (customer_id)
)
;

CREATE OR REPLACE TABLE customer_address (
    customer_id NUMBER(38, 0),
    name varchar,

    CONSTRAINT pk_customer PRIMARY KEY (customer_id),
    CONSTRAINT fk_customer_has_address FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
)
;

CREATE OR REPLACE TABLE item (
    item_id NUMBER(38, 0),
    description varchar,

    CONSTRAINT pk_item PRIMARY KEY (item_id)
)
;

CREATE OR REPLACE TABLE orders (
    order_id NUMBER(38, 0),
    customer_id NUMBER(38, 0),
    item_id NUMBER(38, 0),

    CONSTRAINT pk_order PRIMARY KEY (order_id),
    CONSTRAINT fk_order_has_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
    CONSTRAINT fk_order_contains_item FOREIGN KEY (item_id) REFERENCES item (item_id)
)
;




-- Query the constraint metadata
-- Although import/export keys are not documented in Snowflake documentation as of 2023-05-28,
--      their parameters are identical to SHOW PRIMARY KEYS
-- https://docs.snowflake.com/en/sql-reference/sql/show-primary-keys

-- Show primary keys for a table
SHOW PRIMARY KEYS IN orders;

-- Show foreign keys referenced by a given table
SHOW IMPORTED KEYS IN orders;

-- Show where a table is referenced as foreign key
SHOW EXPORTED KEYS IN customer;
