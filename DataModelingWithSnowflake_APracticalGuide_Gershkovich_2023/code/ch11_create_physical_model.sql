CREATE DATABASE IF NOT EXISTS demo_tpch;
CREATE SCHEMA IF NOT EXISTS operations
-- Because this is an operational schema, access should be tightly controlled by the owner, so opt for managed access
WITH MANAGED ACCESS
-- 2 weeks of fail-safe is required for our data, so set at the schema level for all included tables
DATA_RETENTION_TIME_IN_DAYS = 14;


CREATE TABLE IF NOT EXISTS part (
    -- number(<precision>, <scale>)
    --  - Precision: Total number of digits allowed
    --  - Scale: Number of digits allowed to the right of the decimal point    
    part_id number(38, 0) NOT NULL,
    name varchar NOT NULL, -- default max characters is the maximum allowed length (16,777,216)
    manufacturer varchar NOT NULL,
    brand varchar NOT NULL,
    type varchar NOT NULL,
    size_centimeters number(38, 0) NOT NULL,
    container varchar NOT NULL,
    retail_price_usd number(12, 2) NOT NULL,
    comment varchar COMMENT 'varchar COMMENT ''VARCHAR(23)',

    CONSTRAINT pk_part PRIMARY KEY (part_id) RELY  -- RELY constraint eliminates unnecessary joins
)
COMMENT = 'Contains the parts that we distribute'
;

CREATE TABLE IF NOT EXISTS location (
    location_id number(38, 0) NOT NULL,
    -- varchar(<max-number-of-characters>)
    name varchar(25) NOT NULL,
    region_id number(38, 0) NOT NULL,
    comment varchar(152) COMMENT 'varchar(152) COMMENT ''VARCHAR(152)',

    CONSTRAINT pk_location PRIMARY KEY (location_id) RELY,  -- RELY constraint eliminates unnecessary joins
    CONSTRAINT ak_location_name UNIQUE (name) RELY  -- RELY constraint eliminates unnecessary joins
)
COMMENT = 'Contains the locations assigned to a customer or a supplier'
;

CREATE TABLE IF NOT EXISTS supplier (
    supplier_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    comment varchar COMMENT 'varchar COMMENT ''VARCHAR(101)',

    CONSTRAINT pk_supplier PRIMARY KEY (supplier_id) RELY,  -- RELY constraint eliminates unnecessary joins
    -- For foreign keys, use format fk_<child-table>_<logical-relationship>_<parent-table>
    CONSTRAINT fk_supplier_based_in_location FOREIGN KEY (location_id) REFERENCES location (location_id) RELY
)
COMMENT = 'Contains suppliers who we buy from'
;

CREATE TABLE IF NOT EXISTS customer (
    customer_id number(38, 0) NOT NULL,
    name varchar NOT NULL,
    address varchar NOT NULL,
    location_id number(38, 0) NOT NULL,
    phone varchar(15) NOT NULL,
    account_balance_usd number(12, 2) NOT NULL,
    market_segment varchar(10) NOT NULL,
    comment varchar COMMENT 'varchar COMMENT ''VARCHAR(117)',

    CONSTRAINT pk_customer PRIMARY KEY (customer_id) RELY,  -- RELY constraint eliminates unnecessary joins
    -- For foreign keys, use format fk_<child-table>_<logical-relationship>_<parent-table>
    CONSTRAINT fk_customer_based_in_location FOREIGN KEY (location_id) REFERENCES location (location_id) RELY
)
COMMENT = 'Contains registered customers'
;

CREATE TABLE IF NOT EXISTS sales_order (
    sales_order_id number(38, 0) NOT NULL,
    customer_id number(38, 0) NOT NULL,
    order_status varchar(1),
    total_price_usd number(12, 2),
    order_date date,
    order_priority varchar(15),
    clerk varchar(15),
    ship_priority number(38, 0),
    comment varchar(79) COMMENT 'varchar(79) COMMENT ''VARCHAR(79)',

    CONSTRAINT pk_sales_order PRIMARY KEY (sales_order_id) RELY,
    CONSTRAINT fk_sales_order_placed_by_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) RELY
)
COMMENT = 'Contains a single order per customer'
;

CREATE TABLE IF NOT EXISTS inventory (
    part_id number(38, 0) NOT NULL COMMENT 'Part of unique identifier with ps_suppkey',
    supplier_id number(38, 0) NOT NULL COMMENT 'Part of unique identifier with ps_partkey',
    available_amount number(38, 0) NOT NULL COMMENT 'Number of parts available for sale',
    supplier_cost_usd number(12, 2) NOT NULL COMMENT 'Original cost paid to supplier',
    comment varchar() COMMENT 'varchar(79) COMMENT ''VARCHAR(79)',

    CONSTRAINT pk_inventory PRIMARY KEY (part_id, supplier_id) RELY,  -- Composite/compound key
    CONSTRAINT fk_inventory_stores_part FOREIGN KEY (part_id) REFERENCES part (part_id) RELY,
    CONSTRAINT fk_inventory_supplied_by_supplier FOREIGN KEY (supplier_id) REFERENCES supplier (supplier_id) RELY
)
COMMENT = 'Contains warehouse inventory'
;

CREATE TABLE IF NOT EXISTS line_item (
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
    comment varchar(44) COMMENT 'varchar(44) COMMENT ''VARCHAR(44)',

    CONSTRAINT pk_line_item PRIMARY KEY (line_number, sales_order_id) RELY,  -- Composite/compound key
    CONSTRAINT fk_line_item_consists_of_sales_order FOREIGN KEY (sales_order_id) REFERENCES sales_order (sales_order_id) RELY,
    CONSTRAINT fk_line_item_containing_part FOREIGN KEY (part_id) REFERENCES part (part_id) RELY,
    CONSTRAINT fk_line_item_supplied_by_supplier FOREIGN KEY (supplier_id) REFERENCES supplier (supplier_id) RELY
)
COMMENT = 'Contains various line items per order'
;

-- SUBTYPE table, for subtypes of the Customer SUPERTYPE
CREATE TABLE IF NOT EXISTS loyalty_customer (
    customer_id number(38, 0) NOT NULL,
    level varchar NOT NULL COMMENT 'Customer full name',
    type varchar NOT NULL COMMENT 'Loyalty tier: Bronze, Silver, or Gold',
    points_amount number NOT NULL, -- Default precision (total number of digits allowed) is 38
    comment varchar COMMENT 'Customer loyalty status calculated from sales order volume',

    CONSTRAINT pk_loyalty_customer PRIMARY KEY (customer_id) RELY,  -- Composite/compound key
    CONSTRAINT '1' FOREIGN KEY (customer_id) REFERENCES customer (customer_id) RELY
)
COMMENT = 'Client loyalty program with gold, silver, and bronze statuses'
;
