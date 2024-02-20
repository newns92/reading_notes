-- Set up the environment
USE ROLE accountadmin;
USE DATABASE demo_tpch;
USE SCHEMA ch14_facts;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Recovering physically deleted records and performing a clean "logical" delete
---------------------------------------------------------------------------------------------------------------------

-- Reset the source and reverse tables back to baseline after the first exercise
CREATE OR REPLACE TABLE src_line_item CLONE src_line_item_bak;
CREATE OR REPLACE TABLE line_item_rb CLONE line_item_rb_bak;
CREATE OR REPLACE STREAM strm_line_item_rb ON TABLE line_item_rb;

---------------------------------------------------------------------------------------------------------------------
-- Perform simulated daily load
---------------------------------------------------------------------------------------------------------------------
-- Load the source table, eliminating some records as deletions
--  - Line numbers ending in 0, 1, and 2 will be filtered out to simulate deletions

SELECT COUNT(*) FROM src_line_item;

INSERT INTO src_line_item
WITH updates AS (
    SELECT DISTINCT sales_order_id FROM src_line_item SAMPLE (10 ROWS)
)
-- Updates
SELECT
    line_number,
    src.sales_order_id,
    part_id,
    supplier_id,
    quantity,
    extended_price_usd + 1000,
    discount_percent + 0.01,
    tax_percent,
    CASE
        WHEN return_flag = 'N' AND right(part_id, 1) > 4
        THEN 'R'
        ELSE return_flag
    END,
    line_status,
    ship_date + 1,
    commit_date + 2,
    receipt_date + 3,
    ship_instructions,
    ship_mode,
    comment,
    current_timestamp(),
    'update'
FROM source_system_line_item AS src
INNER JOIN updates AS up
    ON src.sales_order_id = up.sales_order_id
WHERE TRUE
    AND RIGHT(line_number, 1)::INT > 2 -- Simulate deleted records
;

SELECT COUNT(*) FROM src_line_item;

---------------------------------------------------------------------------------------------------------------------
-- Recover 'deleted' records
---------------------------------------------------------------------------------------------------------------------
-- Get a list of all load dates for the orders loaded (today or all time)
-- This generates a structure of today’s order IDs wtih corresponding and leading load dates
CREATE OR REPLACE TEMPORARY TABLE line_load_hist
AS (
    SELECT
        __ldts,
        sales_order_id,
        next_order_load_dt
    FROM (
        SELECT DISTINCT
            __ldts,
            sales_order_id,
            -- Calculate NEXT load date (which will be NULL in the latest load ONLY)
            LEAD(__ldts) OVER (PARTITION BY sales_order_id ORDER BY __ldts) AS next_order_load_dt
        FROM (
            SELECT DISTINCT
                __ldts,
                sales_order_id
            FROM src_line_item
            WHERE TRUE
                AND __load_type != 'deletion'
        )
    )
    WHERE TRUE
    -- Select only the records from the latest (daily) load
    -- Comment out next line for a full reload
    AND next_order_load_dt = (SELECT MAX(__ldts) FROM src_line_item)
)
;

-- SELECT * FROM line_load_hist;

-- Knowing the current and leading load dates for order IDs, we can now do the same for the associated line numbers
CREATE OR REPLACE TEMPORARY TABLE line_deletions
AS (
    SELECT
        t1.next_order_load_dt,
        t2.next_line_load_dt,
        t2.__ldts,
        t2.sales_order_id,
        t2.line_number,
        t2.__load_type
    FROM (
        SELECT
            -- Try lead load date to determine if it has been deleted
            -- NULL values means it has been deleted or is the latest load date
            LEAD(__ldts) OVER (PARTITION BY sales_order_id, line_number ORDER BY __ldts ASC) AS next_line_load_dt,
            __ldts,
            sales_order_id,
            line_number,
            __load_type
        FROM src_line_item
        WHERE TRUE
            -- AND __load_type != 'deletion'
    ) AS t2
    INNER JOIN line_load_hist AS t1
        ON (t1.sales_order_id = t2.sales_order_id AND t1.__ldts = t2.__ldts)
    WHERE TRUE
        AND t1.next_order_load_dt IS NOT NULL -- Only true for the latest records
        -- Uncomment next 2 lines if deleted line item numbers can be re-used
        --  - In THIS example, they cannot
        AND -- (
            t2.next_line_load_dt IS NULL
                -- OR a.next_order_load_dt != NVL(b.next_line_load_dt, current_timestamp()))
)
;

-- SELECT * FROM line_deletions;

-- Insert the recovered records into the source table, making sure to zero out all additive measures
-- Also, the load date must be the latest, NOT the original
INSERT INTO src_line_item (
    line_number,
    sales_order_id,
    part_id,
    supplier_id,
    quantity,
    extended_price_usd,
    discount_percent,
    tax_percent,
    return_flag,
    line_status,
    ship_date,
    commit_date,
    receipt_date,
    ship_instructions,
    ship_mode,
    comment,
    __ldts,
    __load_type
)
WITH deleted AS (
    SELECT
        del.next_order_load_dt,
        target.line_number,
        target.sales_order_id,
        target.part_id,
        target.supplier_id,
        target.quantity,
        target.extended_price_usd,
        target.discount_percent,
        target.tax_percent,
        target.return_flag,
        target.line_status,
        target.ship_date,
        target.commit_date,
        target.receipt_date,
        target.ship_instructions,
        target.ship_mode,
        target.comment,
        target.__ldts,
        target.__load_type
    FROM src_line_item AS target
    INNER JOIN line_deletions AS del
        USING (sales_order_id, line_number, __ldts)
    WHERE TRUE
        AND del.__load_type != 'deletion' -- Avoid re-inserting records that have already been logically deleted
)
-- "Logically" delete the record by inserting it with 0-value measures
--  - Remember to treat non-additive measures as attributes
SELECT
    line_number,
    sales_order_id,
    part_id,
    supplier_id,
    0, -- quantity
    0, -- extended_price_usd
    discount_percent,
    tax_percent,
    return_flag,
    line_status,
    ship_date,
    commit_date,
    receipt_date,
    ship_instructions,
    ship_mode,
    comment,
    next_order_load_dt, -- The load date when the deletion happens
    'deletion' -- Add flag to identify the logically deleted record
FROM deleted
WHERE TRUE
;
-- Should be same number of records as in line_deletions temp table


-- Load the after-image to the reverse balance table
MERGE INTO line_item_rb AS rb
USING (
    -- Get only latest records from src_line_item
    SELECT
        src1.line_number,
        src1.sales_order_id,
        src1.part_id,
        src1.supplier_id,
        src1.quantity,
        src1.extended_price_usd,
        src1.discount_percent,
        src1.tax_percent,
        src1.return_flag,
        src1.line_status,
        src1.ship_date,
        src1.commit_date,
        src1.receipt_date,
        src1.ship_instructions,
        src1.ship_mode,
        src1.comment,
        src1.__ldts,
        src1.__load_type,
        latest_as_at.max_as_at
    FROM src_line_item AS src1
    -- We only want to update the latest record for each order
    -- So, we need to know the As At Date for every sales_order_id and its corresponding line items	
    LEFT JOIN (
            SELECT
                sales_order_id,
                MAX(as_at_dts) AS max_as_at
            FROM line_item_rb
            GROUP BY sales_order_id
        ) AS latest_as_at
        ON src1.sales_order_id = latest_as_at.sales_order_id
    WHERE src1.__ldts = (SELECT MAX(__ldts) FROM src_line_item)
) AS src
ON rb.line_number = src.line_number
    AND rb.sales_order_id = src.sales_order_id
    AND rb.as_at_dts = src.max_as_at
    AND rb.is_after_image -- = TRUE
WHEN NOT MATCHED -- New records, so INSERT them
THEN INSERT VALUES (
    __load_type, -- 'insert'
    __ldts,
    TRUE,
    line_number,
    sales_order_id,
    part_id,
    supplier_id,
    quantity,
    extended_price_usd,
    discount_percent,
    tax_percent,
    return_flag,
    line_status,
    ship_date,
    commit_date,
    receipt_date,
    ship_instructions,
    ship_mode,
    comment    
)
WHEN MATCHED -- Record exists, so UPDATE record(s)
THEN UPDATE
SET
    -- Update ONLY the fields that can be changed
    rb.__load_type = src.__load_type, -- 'update'
    rb.as_at_dts = src.__ldts,
    -- After Image is already TRUE
    rb.quantity = src.quantity,
    rb.extended_price_usd = src.extended_price_usd,
    rb.discount_percent = src.discount_percent,
    rb.return_flag = src.return_flag,
    rb.ship_date = src.ship_date,
    rb.commit_date = src.commit_date,
    rb.receipt_date = src.receipt_date
;
-- Should see both inserts and updates

-- SELECT * FROM strm_line_item_rb;


-- Insert the before image from the stream
INSERT INTO line_item_rb
WITH before_records AS (
    -- Get the before image BUT append the as_at from the AFTER image
    SELECT
        bfore.__load_type, -- 'insert'
        bfore.as_at_dts,
        bfore.is_after_image,
        bfore.line_number,
        bfore.sales_order_id,
        bfore.part_id,
        bfore.supplier_id,
        bfore.quantity,
        bfore.extended_price_usd,
        bfore.discount_percent,
        bfore.tax_percent,
        bfore.return_flag,
        bfore.line_status,
        bfore.ship_date,
        bfore.commit_date,
        bfore.receipt_date,
        bfore.ship_instructions,
        bfore.ship_mode,
        bfore.comment,
        bfore.METADATA$ACTION,
        bfore.METADATA$ISUPDATE,
        bfore.METADATA$ROW_ID,
        aftr.as_at_after
    FROM strm_line_item_rb AS bfore
    INNER JOIN (
            SELECT
                line_number,
                sales_order_id,
                as_at_dts AS as_at_after
            FROM strm_line_item_rb
            WHERE METADATA$ACTION = 'INSERT'
                AND METADATA$ISUPDATE -- = TRUE
        ) AS aftr
    USING (line_number, sales_order_id)
    WHERE TRUE
        AND bfore.METADATA$ACTION = 'DELETE'
)
-- Insert the original after image that we updated
--  - No changes are required to column values
SELECT
    __load_type,
    as_at_dts,
    is_after_image, -- TRUE
    line_number,
    sales_order_id,
    part_id,
    supplier_id,
    quantity,
    extended_price_usd,
    discount_percent,
    tax_percent,
    return_flag,
    line_status,
    ship_date,
    commit_date,
    receipt_date,
    ship_instructions,
    ship_mode,
    comment
FROM before_records
UNION ALL -- no deduplication
SELECT
    __load_type,
    as_at_after, -- Use the As At of the AFTER image
    FALSE, -- FALSE because this is the BEFORE image
    line_number,
    sales_order_id,
    part_id,
    supplier_id,
    -1 * quantity, -- Reverse the balance
    -1 * extended_price_usd, -- Reverse the balance
    discount_percent, -- NON-additive, so do NOT negate
    tax_percent,
    return_flag,
    line_status,
    ship_date,
    commit_date,
    receipt_date,
    ship_instructions,
    ship_mode,
    comment
FROM before_records
;
