-- Set up the environment
USE ROLE accountadmin;
USE DATABASE demo_tpch;
USE SCHEMA ch14_facts;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Time-banded Facts Using a Type 2 Structure
---------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------
-- Prepare the base tables
---------------------------------------------------------------------------------------------------------------------
-- Sample data contains records from within the range 1992-1998
-- Use a date that allow us to load historical data for existing employees, and also give us plenty of sample data for future changes
SET today = '1995-12-01';

-- Instantiate the sample dataset for the simulated source system and the initial data warehouse landing area
CREATE OR REPLACE TABLE source_system_employee (
    employee_id number(38, 0) NOT NULL,
    department_id number(38, 0) NOT NULL,
    employee_type varchar(1) NOT NULL,
    salary_usd number(12, 2) NOT NULL,
    hire_date date NOT NULL,
    security_clearance varchar(15) NOT NULL,
    clerk varchar(15) NOT NULL,
    comment varchar(79) COMMENT 'Convert `snowflake_sample_data.tpch_sf10.orders` to employees',

    CONSTRAINT pk_source_system_employee PRIMARY KEY (employee_id)
)
COMMENT = 'Employee sample data'
AS
SELECT
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_comment
FROM snowflake_sample_data.tpch_sf10.orders
;

-- Create initial load of day 1 data warehouse landing area
CREATE OR REPLACE TABLE src_employee (
    employee_id number(38, 0) NOT NULL,
    department_id number(38, 0) NOT NULL,
    is_contractor boolean NOT NULL,
    salary_usd number(12, 2) NOT NULL,
    hire_date date NOT NULL,
    termination_date date NOT NULL,
    security_clearance varchar(15) NOT NULL,
    clerk varchar(15) NOT NULL,
    is_active boolean NOT NULL,
    last_change varchar NOT NULL,
    comment varchar NOT NULL,
    __load_date date NOT NULL,

    CONSTRAINT pk_src_employee PRIMARY KEY (employee_id, __load_date)
)
COMMENT = 'Base table at day 1 load'
AS
SELECT
    employee_id,
    department_id,
    RIGHT(clerk, 1)::INT > 7, -- IS a contractor
    salary_usd,
    hire_date,
    CASE
        WHEN RIGHT(clerk, 1)::INT < 3
        THEN '9999-12-31'::DATE -- accepted "current employee" date
        ELSE DATEADD('DAY', RIGHT(salary_usd::INT, 3), hire_date)
    END AS termination_date, -- name column to use later
    security_clearance,
    clerk,
    IFF(termination_date >= $today, TRUE, FALSE), -- Like a single-line CASE statement
    IFF(termination_date >= $today, 'Hire', 'Leaver'),
    comment,
    $today
FROM source_system_employee
SAMPLE (50000 ROWS)
WHERE TRUE
    AND hire_date < $today
;

SELECT * FROM src_employee SAMPLE (10 ROWS);

-- Create the Type 2 fact table
CREATE OR REPLACE TABLE employee_t2 (
    employee_id number(38, 0) NOT NULL,
    department_id number(38, 0) NOT NULL,
    is_contractor boolean NOT NULL,
    salary_usd number(12, 2) NOT NULL,
    hire_date date NOT NULL,
    termination_date date NOT NULL,
    security_clearance varchar(15) NOT NULL,
    clerk varchar(15) NOT NULL,
    is_active boolean NOT NULL,
    last_change varchar NOT NULL,
    comment varchar NOT NULL COMMENT 'HR Feedback',
    __load_date date NOT NULL COMMENT 'Load date from src_employee table',
    from_date date NOT NULL,
    to_date date NOT NULL,
    diff_hash varchar(32) NOT NULL,

    CONSTRAINT pk_employee_t2 PRIMARY KEY (employee_id, from_date)
)
COMMENT = 'Create and instantiate the Type 2 employee fact table at day 1'
AS
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    last_change,
    comment,
    __load_date, -- Source load date
    '1900-01-01', -- from_date
    '9999-12-31', -- to_date
    MD5(salary_usd || hire_date || termination_date || comment || last_change || __load_date || is_active)
FROM src_employee
;

-- Create backups for re-running the exercise
CREATE OR REPLACE TABLE src_employee_bak CLONE src_employee;
CREATE OR REPLACE TABLE src_employee CLONE src_employee_bak;
CREATE OR REPLACE STREAM strm_employee_t2 ON TABLE employee_t2;


---------------------------------------------------------------------------------------------------------------------
-- Simulate a daily load
---------------------------------------------------------------------------------------------------------------------
-- Increment the `today` variable by 1 and start to insert changes into the landing area
--  - Changes such as new hires, terminations, and promotions for some existing employees
SELECT $today;

SET today = $today::DATE + 1;

SELECT $today;

INSERT INTO src_employee
-- Get changes
WITH existing AS (
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        IFF(RIGHT(salary_usd ,1)::INT > 6, 'leaver', 'update') AS update_type
    FROM src_employee
    SAMPLE (150 ROWS)
    WHERE TRUE
        AND is_active
        AND hire_date < $today::DATE - 2
)
-- Load 100 new hires
SELECT
    employee_id,
    department_id,
    RIGHT(clerk, 1)::INT > 7, -- IS a contractor
    salary_usd,
    hire_date,
    CASE
        WHEN RIGHT(clerk, 1)::INT <= 7
        THEN '9999-12-31'::DATE
        ELSE DATEADD('DAY', RIGHT(salary_usd::INT, 3), hire_date)
    END,
    security_clearance,
    clerk,
    TRUE,
    'Hire',
    comment,
    $today
FROM source_system_employee
SAMPLE (100 ROWS)
WHERE TRUE
    AND hire_date = $today::DATE - 1

UNION ALL -- no deduplication

-- Load the Leavers
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    $today::DATE - 1,
    security_clearance,
    clerk,
    FALSE,
    'Leaver',
    'Left ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'leaver'

UNION ALL -- no deduplication

-- Load updates to existing employee
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd * 1.1, -- 10% raise
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    'Promoted',
    '+10% ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'update'
;
-- Should see 250 rows inserted



-- Update the fact table with the latest records
MERGE INTO employee_t2 AS trgt
USING (
    -- Get only latest records from src_employee
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        MD5(salary_usd || hire_date || termination_date || comment || last_change || __load_date || is_active) AS diff_hash
    FROM src_employee
    -- Get the max load date from the target and load the next date from source
    WHERE __load_date = (SELECT MAX(__load_date)::DATE + 1 FROM employee_t2)
) AS src
    ON trgt.employee_id = src.employee_id
        AND trgt.to_date = '9999-12-31'
WHEN NOT MATCHED -- New records, so insert them
THEN INSERT VALUES (
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        __load_date, -- from_date
        '9999-12-31', -- to_date
        diff_hash
)
WHEN MATCHED -- Record exists, so update them
-- This example uses source delta loads, but real-world scenarios could be full loads
--  - Therefore, would need to check diff_hash to determine if changes happened
    AND trgt.diff_hash != src.diff_hash -- Check for change in the Type 2 fact
THEN UPDATE
SET
    trgt.salary_usd = src.salary_usd,
    trgt.hire_date = src.hire_date,
    trgt.termination_date = src.termination_date,
    trgt.comment = src.comment,
    trgt.last_change = src.last_change,
    trgt.from_date = src.__load_date,
    trgt.is_active = src.is_active,
    trgt.__load_date = src.__load_date,
    trgt.diff_hash = MD5(src.salary_usd || src.hire_date || src.termination_date || src.comment || src.last_change || src.__load_date || src.is_active)
;
-- Should see 100 rows (hired) inserted, 150 updated (leaving and promotions)

-- Insert the original value from stream
-- SELECT * FROM strm_employee_t2;
INSERT INTO employee_t2
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    last_change,
    comment,
    __load_date,
    from_date, -- Original from_date
    DATEADD('DAY', -1, new_to_date), -- Delimit new to_date to be less than the inserted datetimestamp
    diff_hash
FROM strm_employee_t2 AS strm
INNER JOIN ((SELECT MAX(__load_date) AS new_to_date FROM employee_t2)) -- Get the to_date for the current load
    ON TRUE
        AND strm.metadata$action = 'DELETE' -- Get the before-image for updated records
WHERE TRUE
;


--------------------------------------------------------------------------------------------------------------------------------------
-- Now, increment the $today variable and simulate several more days of changes to generate some meaningful data to query
--------------------------------------------------------------------------------------------------------------------------------------
-- DAY 1
SET today = $today::DATE + 1;
-- SELECT $today;

INSERT INTO src_employee
-- Get changes
WITH existing AS (
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        IFF(RIGHT(salary_usd ,1)::INT > 6, 'leaver', 'update') AS update_type
    FROM src_employee
    SAMPLE (150 ROWS)
    WHERE TRUE
        AND is_active
        AND hire_date < $today::DATE - 2
)
-- Load 100 new hires
SELECT
    employee_id,
    department_id,
    RIGHT(clerk, 1)::INT > 7, -- IS a contractor
    salary_usd,
    hire_date,
    CASE
        WHEN RIGHT(clerk, 1)::INT <= 7
        THEN '9999-12-31'::DATE
        ELSE DATEADD('DAY', RIGHT(salary_usd::INT, 3), hire_date)
    END,
    security_clearance,
    clerk,
    TRUE,
    'Hire',
    comment,
    $today
FROM source_system_employee
SAMPLE (100 ROWS)
WHERE TRUE
    AND hire_date = $today::DATE - 1

UNION ALL -- no deduplication

-- Load the Leavers
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    $today::DATE - 1,
    security_clearance,
    clerk,
    FALSE,
    'Leaver',
    'Left ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'leaver'

UNION ALL -- no deduplication

-- Load updates to existing employee
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd * 1.1, -- 10% raise
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    'Promoted',
    '+10% ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'update'
;
-- Should see 250 rows inserted



-- Update the fact table with the latest records
MERGE INTO employee_t2 AS trgt
USING (
    -- Get only latest records from src_employee
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        MD5(salary_usd || hire_date || termination_date || comment || last_change || __load_date || is_active) AS diff_hash
    FROM src_employee
    -- Get the max load date from the target and load the next date from source
    WHERE __load_date = (SELECT MAX(__load_date)::DATE + 1 FROM employee_t2)
) AS src
    ON trgt.employee_id = src.employee_id
        AND trgt.to_date = '9999-12-31'
WHEN NOT MATCHED -- New records, so insert them
THEN INSERT VALUES (
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        __load_date, -- from_date
        '9999-12-31', -- to_date
        diff_hash
)
WHEN MATCHED -- Record exists, so update them
-- This example uses source delta loads, but real-world scenarios could be full loads
--  - Therefore, would need to check diff_hash to determine if changes happened
    AND trgt.diff_hash != src.diff_hash -- Check for change in the Type 2 fact
THEN UPDATE
SET
    trgt.salary_usd = src.salary_usd,
    trgt.hire_date = src.hire_date,
    trgt.termination_date = src.termination_date,
    trgt.comment = src.comment,
    trgt.last_change = src.last_change,
    trgt.from_date = src.__load_date    ,
    trgt.is_active = src.is_active,
    trgt.__load_date = src.__load_date,
    trgt.diff_hash = MD5(src.salary_usd || src.hire_date || src.termination_date || src.comment || src.last_change || src.__load_date || src.is_active)
;
-- Should see 100 rows (hired) inserted, 150 updated (leaving and promotions)

-- Insert the original value from stream
-- SELECT * FROM strm_employee_t2;
INSERT INTO employee_t2
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    last_change,
    comment,
    __load_date,
    from_date, -- Original from_date
    DATEADD('DAY', -1, new_to_date), -- Delimit new to_date to be less than the inserted datetimestamp
    diff_hash
FROM strm_employee_t2 AS strm
INNER JOIN ((SELECT MAX(__load_date) AS new_to_date FROM employee_t2)) -- Get the to_date for the current load
    ON TRUE
        AND strm.metadata$action = 'DELETE' -- Get the before-image for updated records
WHERE TRUE
;

-- DAY 2
SET today = $today::DATE + 1;
SELECT $today;

INSERT INTO src_employee
-- Get changes
WITH existing AS (
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        IFF(RIGHT(salary_usd ,1)::INT > 6, 'leaver', 'update') AS update_type
    FROM src_employee
    SAMPLE (150 ROWS)
    WHERE TRUE
        AND is_active
        AND hire_date < $today::DATE - 2
)
-- Load 100 new hires
SELECT
    employee_id,
    department_id,
    RIGHT(clerk, 1)::INT > 7, -- IS a contractor
    salary_usd,
    hire_date,
    CASE
        WHEN RIGHT(clerk, 1)::INT <= 7
        THEN '9999-12-31'::DATE
        ELSE DATEADD('DAY', RIGHT(salary_usd::INT, 3), hire_date)
    END,
    security_clearance,
    clerk,
    TRUE,
    'Hire',
    comment,
    $today
FROM source_system_employee
SAMPLE (100 ROWS)
WHERE TRUE
    AND hire_date = $today::DATE - 1

UNION ALL -- no deduplication

-- Load the Leavers
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    $today::DATE - 1,
    security_clearance,
    clerk,
    FALSE,
    'Leaver',
    'Left ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'leaver'

UNION ALL -- no deduplication

-- Load updates to existing employee
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd * 1.1, -- 10% raise
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    'Promoted',
    '+10% ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'update'
;
-- Should see 250 rows inserted



-- Update the fact table with the latest records
MERGE INTO employee_t2 AS trgt
USING (
    -- Get only latest records from src_employee
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        MD5(salary_usd || hire_date || termination_date || comment || last_change || __load_date || is_active) AS diff_hash
    FROM src_employee
    -- Get the max load date from the target and load the next date from source
    WHERE __load_date = (SELECT MAX(__load_date)::DATE + 1 FROM employee_t2)
) AS src
    ON trgt.employee_id = src.employee_id
        AND trgt.to_date = '9999-12-31'
WHEN NOT MATCHED -- New records, so insert them
THEN INSERT VALUES (
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        __load_date, -- from_date
        '9999-12-31', -- to_date
        diff_hash
)
WHEN MATCHED -- Record exists, so update them
-- This example uses source delta loads, but real-world scenarios could be full loads
--  - Therefore, would need to check diff_hash to determine if changes happened
    AND trgt.diff_hash != src.diff_hash -- Check for change in the Type 2 fact
THEN UPDATE
SET
    trgt.salary_usd = src.salary_usd,
    trgt.hire_date = src.hire_date,
    trgt.termination_date = src.termination_date,
    trgt.comment = src.comment,
    trgt.last_change = src.last_change,
    trgt.from_date = src.__load_date    ,
    trgt.is_active = src.is_active,
    trgt.__load_date = src.__load_date,
    trgt.diff_hash = MD5(src.salary_usd || src.hire_date || src.termination_date || src.comment || src.last_change || src.__load_date || src.is_active)
;
-- Should see 100 rows (hired) inserted, 150 updated (leaving and promotions)

-- Insert the original value from stream
-- SELECT * FROM strm_employee_t2;
INSERT INTO employee_t2
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    last_change,
    comment,
    __load_date,
    from_date, -- Original from_date
    DATEADD('DAY', -1, new_to_date), -- Delimit new to_date to be less than the inserted datetimestamp
    diff_hash
FROM strm_employee_t2 AS strm
INNER JOIN ((SELECT MAX(__load_date) AS new_to_date FROM employee_t2)) -- Get the to_date for the current load
    ON TRUE
        AND strm.metadata$action = 'DELETE' -- Get the before-image for updated records
WHERE TRUE
;

-- DAY 3
SET today = $today::DATE + 1;
SELECT $today;

INSERT INTO src_employee
-- Get changes
WITH existing AS (
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        IFF(RIGHT(salary_usd ,1)::INT > 6, 'leaver', 'update') AS update_type
    FROM src_employee
    SAMPLE (150 ROWS)
    WHERE TRUE
        AND is_active
        AND hire_date < $today::DATE - 2
)
-- Load 100 new hires
SELECT
    employee_id,
    department_id,
    RIGHT(clerk, 1)::INT > 7, -- IS a contractor
    salary_usd,
    hire_date,
    CASE
        WHEN RIGHT(clerk, 1)::INT <= 7
        THEN '9999-12-31'::DATE
        ELSE DATEADD('DAY', RIGHT(salary_usd::INT, 3), hire_date)
    END,
    security_clearance,
    clerk,
    TRUE,
    'Hire',
    comment,
    $today
FROM source_system_employee
SAMPLE (100 ROWS)
WHERE TRUE
    AND hire_date = $today::DATE - 1

UNION ALL -- no deduplication

-- Load the Leavers
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    $today::DATE - 1,
    security_clearance,
    clerk,
    FALSE,
    'Leaver',
    'Left ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'leaver'

UNION ALL -- no deduplication

-- Load updates to existing employee
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd * 1.1, -- 10% raise
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    'Promoted',
    '+10% ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'update'
;
-- Should see 250 rows inserted



-- Update the fact table with the latest records
MERGE INTO employee_t2 AS trgt
USING (
    -- Get only latest records from src_employee
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        MD5(salary_usd || hire_date || termination_date || comment || last_change || __load_date || is_active) AS diff_hash
    FROM src_employee
    -- Get the max load date from the target and load the next date from source
    WHERE __load_date = (SELECT MAX(__load_date)::DATE + 1 FROM employee_t2)
) AS src
    ON trgt.employee_id = src.employee_id
        AND trgt.to_date = '9999-12-31'
WHEN NOT MATCHED -- New records, so insert them
THEN INSERT VALUES (
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        __load_date, -- from_date
        '9999-12-31', -- to_date
        diff_hash
)
WHEN MATCHED -- Record exists, so update them
-- This example uses source delta loads, but real-world scenarios could be full loads
--  - Therefore, would need to check diff_hash to determine if changes happened
    AND trgt.diff_hash != src.diff_hash -- Check for change in the Type 2 fact
THEN UPDATE
SET
    trgt.salary_usd = src.salary_usd,
    trgt.hire_date = src.hire_date,
    trgt.termination_date = src.termination_date,
    trgt.comment = src.comment,
    trgt.last_change = src.last_change,
    trgt.from_date = src.__load_date    ,
    trgt.is_active = src.is_active,
    trgt.__load_date = src.__load_date,
    trgt.diff_hash = MD5(src.salary_usd || src.hire_date || src.termination_date || src.comment || src.last_change || src.__load_date || src.is_active)
;
-- Should see 100 rows (hired) inserted, 150 updated (leaving and promotions)

-- Insert the original value from stream
-- SELECT * FROM strm_employee_t2;
INSERT INTO employee_t2
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    last_change,
    comment,
    __load_date,
    from_date, -- Original from_date
    DATEADD('DAY', -1, new_to_date), -- Delimit new to_date to be less than the inserted datetimestamp
    diff_hash
FROM strm_employee_t2 AS strm
INNER JOIN ((SELECT MAX(__load_date) AS new_to_date FROM employee_t2)) -- Get the to_date for the current load
    ON TRUE
        AND strm.metadata$action = 'DELETE' -- Get the before-image for updated records
WHERE TRUE
;

-- DAY 4
SET today = $today::DATE + 1;
SELECT $today;

INSERT INTO src_employee
-- Get changes
WITH existing AS (
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        IFF(RIGHT(salary_usd ,1)::INT > 6, 'leaver', 'update') AS update_type
    FROM src_employee
    SAMPLE (150 ROWS)
    WHERE TRUE
        AND is_active
        AND hire_date < $today::DATE - 2
)
-- Load 100 new hires
SELECT
    employee_id,
    department_id,
    RIGHT(clerk, 1)::INT > 7, -- IS a contractor
    salary_usd,
    hire_date,
    CASE
        WHEN RIGHT(clerk, 1)::INT <= 7
        THEN '9999-12-31'::DATE
        ELSE DATEADD('DAY', RIGHT(salary_usd::INT, 3), hire_date)
    END,
    security_clearance,
    clerk,
    TRUE,
    'Hire',
    comment,
    $today
FROM source_system_employee
SAMPLE (100 ROWS)
WHERE TRUE
    AND hire_date = $today::DATE - 1

UNION ALL -- no deduplication

-- Load the Leavers
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    $today::DATE - 1,
    security_clearance,
    clerk,
    FALSE,
    'Leaver',
    'Left ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'leaver'

UNION ALL -- no deduplication

-- Load updates to existing employee
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd * 1.1, -- 10% raise
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    'Promoted',
    '+10% ' || comment,
    $today
FROM existing
WHERE TRUE
    AND update_type = 'update'
;
-- Should see 250 rows inserted



-- Update the fact table with the latest records
MERGE INTO employee_t2 AS trgt
USING (
    -- Get only latest records from src_employee
    SELECT
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        MD5(salary_usd || hire_date || termination_date || comment || last_change || __load_date || is_active) AS diff_hash
    FROM src_employee
    -- Get the max load date from the target and load the next date from source
    WHERE __load_date = (SELECT MAX(__load_date)::DATE + 1 FROM employee_t2)
) AS src
    ON trgt.employee_id = src.employee_id
        AND trgt.to_date = '9999-12-31'
WHEN NOT MATCHED -- New records, so insert them
THEN INSERT VALUES (
        employee_id,
        department_id,
        is_contractor,
        salary_usd,
        hire_date,
        termination_date,
        security_clearance,
        clerk,
        is_active,
        last_change,
        comment,
        __load_date,
        __load_date, -- from_date
        '9999-12-31', -- to_date
        diff_hash
)
WHEN MATCHED -- Record exists, so update them
-- This example uses source delta loads, but real-world scenarios could be full loads
--  - Therefore, would need to check diff_hash to determine if changes happened
    AND trgt.diff_hash != src.diff_hash -- Check for change in the Type 2 fact
THEN UPDATE
SET
    trgt.salary_usd = src.salary_usd,
    trgt.hire_date = src.hire_date,
    trgt.termination_date = src.termination_date,
    trgt.comment = src.comment,
    trgt.last_change = src.last_change,
    trgt.from_date = src.__load_date    ,
    trgt.is_active = src.is_active,
    trgt.__load_date = src.__load_date,
    trgt.diff_hash = MD5(src.salary_usd || src.hire_date || src.termination_date || src.comment || src.last_change || src.__load_date || src.is_active)
;
-- Should see 100 rows (hired) inserted, 150 updated (leaving and promotions)

-- Insert the original value from stream
-- SELECT * FROM strm_employee_t2;
INSERT INTO employee_t2
SELECT
    employee_id,
    department_id,
    is_contractor,
    salary_usd,
    hire_date,
    termination_date,
    security_clearance,
    clerk,
    is_active,
    last_change,
    comment,
    __load_date,
    from_date, -- Original from_date
    DATEADD('DAY', -1, new_to_date), -- Delimit new to_date to be less than the inserted datetimestamp
    diff_hash
FROM strm_employee_t2 AS strm
INNER JOIN ((SELECT MAX(__load_date) AS new_to_date FROM employee_t2)) -- Get the to_date for the current load
    ON TRUE
        AND strm.metadata$action = 'DELETE' -- Get the before-image for updated records
WHERE TRUE
;


---------------------------------------------------------------------------------------------------------------------
-- Experiment with queries that pivot on varying intervals
---------------------------------------------------------------------------------------------------------------------
-- The surrogate high date (to_date) will always return the latest version of a fact
-- Use it to see how many employees are currently active
SELECT
    COUNT(*) AS active_employee_count
FROM employee_t2
WHERE TRUE
    AND is_active
    AND to_date = '9999-12-31' -- Currently
;
-- 27431


-- What about how many employees were currently active at a point in the past?
-- Here, we must consider a RANGE of dates: records with an effective date valid before the target date, and then after
-- See the number of employees active on 12/1/1995
SELECT
    COUNT(DISTINCT employee_id) AS active_employee_count
FROM employee_t2
WHERE TRUE
    AND is_active
    AND from_date <= '1995-12-01'
    AND to_date >= '1995-12-01'
;
-- 27156


-- Instead of a single point in time, the query can use a range as well
-- Calculate (without double-counting changes) the number of active employees for the entire year of 1995
SELECT
    COUNT(DISTINCT employee_id) AS active_employee_count
FROM employee_t2
WHERE TRUE
    AND is_active
    AND YEAR(from_date) <= 1995
    AND YEAR(to_date) >= 1995
;
-- 27656


-- Using various time criteria, we can mix current and historical values to ask targeted questions
-- Questions could be "who was hired in Q1 1994 last year and was still active on the date of 1995-12-01?"
SELECT
    COUNT(DISTINCT employee_id) AS active_employee_count
FROM employee_t2
WHERE TRUE
    AND is_active
    AND hire_date BETWEEN '1994-01-01' AND '1994-03-31'
    AND from_date <= '1995-12-01'
    AND to_date >= '1995-12-01'
;
-- 1642


-- Now, only show me those who received a promotion
WITH promotions AS (
    SELECT
        DISTINCT employee_id
    FROM employee_t2
    WHERE TRUE
        AND last_change = 'Promoted'
)
SELECT
    COUNT(DISTINCT employee_id) AS active_employee_count
FROM employee_t2
INNER JOIN promotions
    USING (employee_id)
WHERE TRUE
    AND is_active
    AND hire_date BETWEEN '1994-01-01' AND '1994-03-31'
    AND from_date <= '1995-12-01'
    AND to_date >= '1995-12-01'
;
-- 24


-- Whether it's capturing distinct groupings by date (as seen in the following example) or aggregating totals over a range of dates,
--  the Type 2 fact table can handle any range-based query that users throw at it
-- What are the total changes per day by change type since the first load ( excluding 1995-12-01)
--  - Or, show the result of daily headcount movements since starting the exercise
SELECT
    from_date,
    last_change,
    COUNT(employee_id) AS employee_count
FROM employee_t2
WHERE TRUE
    AND from_date > '1995-12-01'
    AND to_date = '9999-12-31' -- Currently
GROUP BY
    from_date,
    last_change
ORDER BY
    from_date,
    last_change
;
