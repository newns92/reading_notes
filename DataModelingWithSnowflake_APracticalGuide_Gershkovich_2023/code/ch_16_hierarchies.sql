-- Set up the environment
USE ROLE accountadmin;
USE DATABASE demo_tpch;
CREATE OR REPLACE SCHEMA ch16_hier;
USE SCHEMA ch16_hier;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Create the base table and load data 
---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE pirate (
    pirate_id number(38, 0) NOT NULL,
    name varchar(50) NOT NULL,
    rank varchar(50) NOT NULL,
    superior_id number(38, 0),

    CONSTRAINT pk_pirate PRIMARY KEY (pirate_id),
    CONSTRAINT pk_pirate_has_superior FOREIGN KEY (superior_id) REFERENCES pirate (pirate_id)
)
COMMENT = 'Contains pirate rank hierarchy'
;

INSERT INTO pirate (
    pirate_id,
    name,
    rank,
    superior_id
)
VALUES
    (1, 'Blackbeard', 'Captain', NULL),
    (2, 'Calico Jack', 'First Mate', 1),
    (3, 'Anne Bonny', 'Second Mate', 1),
    (4, 'Mary Read', 'Navigator', 2),
    (5, 'Israel Hands', 'Boatswain', 2),
    (6, 'John Silver', 'Carpenter', 3),
    (7, 'Long John', 'Gunner', 3),
    (8, 'Billy Bones', 'Cook', 4),
    (9, 'Tom Morgan', 'Sailor', 5),
    (10, 'Harry Hawkins', 'Sailor', 5),
    (11, 'Black Dog', 'Sailor', 6),
    (12, 'Dick Johnson', 'Sailor', 6),
    (13, 'Roger Pew', 'Sailor', 7),
    (14, 'Dirk van der Heide', 'Sailor', 7),
    (15, 'Ned Low', 'Sailor', 9),
    (16, 'Edward England', 'Sailor', 9),
    (17, 'Stede Bonnet', 'Sailor', 10),
    (18, 'Charles Vane', 'Sailor', 10),
    (19, 'James Kidd', 'Sailor', 11),
    (20, 'William Kidd', 'Sailor', 11)
;

SELECT * FROM pirate;



---------------------------------------------------------------------------------------------------------------------
-- Determine the depth and hierarchy path using CONNECT BY
---------------------------------------------------------------------------------------------------------------------
SELECT
    name,
    pirate_id,
    superior_id,
    rank,
    SYS_CONNECT_BY_PATH(rank, '-> ') AS path,
    -- Level = pseudo-column returned by CONNECT BY clause
    --  - It indicates the current level of the hierarchy
    level
FROM pirate
-- https://docs.snowflake.com/en/sql-reference/constructs/connect-by
-- CONNECT BY joins a table to *itself* to process hierarchical data in the table
START WITH rank = 'Captain'
CONNECT BY superior_id = PRIOR pirate_id
ORDER BY level
;

---------------------------------------------------------------------------------------------------------------------
-- Separate the hierarchy into multiple root branches
---------------------------------------------------------------------------------------------------------------------
-- Label hierarchy branches to make future analysis easier via the CONNECT_BY_ROOT function 
-- This returns the top-level root node of the branch we are traversing
SELECT
    name,
    pirate_id,
    superior_id,
    rank,
    SYS_CONNECT_BY_PATH(rank, '-> ') AS path,
    CONNECT_BY_ROOT rank AS crew_branch, -- display the root node of the CONNECT BY
    level + 1 AS level -- add 1 to the level since we're starting at the 'Mates' roots
FROM pirate
-- Tokenize the rank via the space delimiter and return the 2nd part with STRTOK()
START WITH STRTOK(rank, ' ', 2) = 'Mate' -- start with the first and second mates as "roots"
CONNECT BY superior_id = PRIOR pirate_id
-- ORDER BY level
-- NOTE: This does NOT return our top-level of “Captain”
-- Add in the captain
UNION ALL -- without duplicate elimination
SELECT
    name,
    pirate_id,
    superior_id,
    rank,
    rank AS path,
    rank AS crew_branch, -- display the root node of the CONNECT BY
    1 AS level -- add 1 to the level since we're starting at the 'Mates' roots
FROM pirate
WHERE rank = 'Captain'
-- Order the results
ORDER BY crew_branch, level
;

---------------------------------------------------------------------------------------------------------------------
-- Calculate and flag edge nodes
---------------------------------------------------------------------------------------------------------------------
WITH hier AS (
    SELECT
        name,
        pirate_id,
        superior_id,
        rank,
        SYS_CONNECT_BY_PATH(rank, '-> ') AS path,
        CONNECT_BY_ROOT rank AS crew_branch, -- display the root node of the CONNECT BY
        level + 1 AS level -- add 1 to the level since we're starting at the 'Mates' roots
    FROM pirate
    -- Tokenize the rank via the space delimiter and return the 2nd part with STRTOK()
    START WITH STRTOK(rank, ' ', 2) = 'Mate' -- start with the first and second mates as "roots"
    CONNECT BY superior_id = PRIOR pirate_id
    -- ORDER BY level
    -- NOTE: This does NOT return our top-level of “Captain”
    -- Add in the captain
    UNION ALL -- without duplicate elimination
    SELECT
        name,
        pirate_id,
        superior_id,
        rank,
        rank AS path,
        rank AS crew_branch, -- display the root node of the CONNECT BY
        1 AS level -- add 1 to the level since we're starting at the 'Mates' roots
    FROM pirate
    WHERE rank = 'Captain'
    -- Order the results
    ORDER BY crew_branch, level
),
-- JOIN the above query to a list of distinct superiors
super AS (
    SELECT DISTINCT
        superior_id
    FROM hier
)
-- SELECT * FROM super;
SELECT
    hier.name,
    hier.pirate_id,
    hier.superior_id,
    hier.rank,
    hier.path,
    hier.crew_branch,
    hier.level,
    -- super.superior_id,
    IFF(super.superior_id IS NULL, TRUE, FALSE) AS is_edge_node
FROM hier
LEFT JOIN super
    -- JOIN only if current pirate is a superior (has direct reports)
    ON hier.pirate_id = super.superior_id
;
