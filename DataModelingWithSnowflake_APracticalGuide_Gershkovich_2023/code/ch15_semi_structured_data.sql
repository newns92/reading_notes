-- Set up the environment
USE ROLE accountadmin;
USE DATABASE demo_tpch;
CREATE OR REPLACE SCHEMA ch15_semistruct;
USE SCHEMA ch15_semistruct;
USE WAREHOUSE demo_wh;

---------------------------------------------------------------------------------------------------------------------
-- Create a table and load semi-structured data
---------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE pirate_json (
    __load_id number NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    __load_name varchar NOT NULL,
    __load_dts timestamp_ntz NOT NULL,
    v variant NOT NULL,

    CONSTRAINT pirate_json___load_id PRIMARY KEY (__load_id)
)
COMMENT = 'Table with a VARIANT for pirate data, with meta-ELT fields (__load_*)'
;

INSERT INTO pirate_json (
    -- NOTE: __load_id is omitted to allow autoincrement
    __load_name,
    __load_dts,
    v
)
SELECT
    'ad-hoc load',
    current_timestamp(),
    PARSE_JSON($1)
FROM
    VALUES ('
        {
        "name": "Edward Teach",
        "nickname": "Blackbeard",
        "years_active": [
            1716,
            1717,
            1718
        ],
        "born": 1680,
        "died": 1718,
        "cause_of_death": "Killed in action",
        "crew": [
            {
            "name": "Stede Bonnet",
            "nickname": "Gentleman pirate",
            "weapons": [
                "blunderbuss"
            ],
            "years_active": [
                1717,
                1718
            ]
            },
            {
            "name": "Israel Hands",
            "nickname": null,
            "had_bird": true,
            "weapons": [
                "flintlock pistol",
                "cutlass",
                "boarding axe"
            ],
            "years_active": [
                1716,
                1717,
                1718
            ]
            }
        ],
        "ship": {
            "name": "Queen Anne\'s Revenge",
            "type": "Frigate",
            "original_name": "La Concorde",
            "year_captured": 1717
        }
        }        
    ')
;


---------------------------------------------------------------------------------------------------------------------
-- SCHEMA ON READ
-- Read from semi-structured data
---------------------------------------------------------------------------------------------------------------------
-- Extract some basic attributes for our pirate 
SELECT * FROM pirate_json;

-- To access information stored inside VARIANT, Snowflake uses the colon operator
-- Cast and alias the basic attributes using this operator
SELECT
    v:name AS pirate_name_json,
    v:name::STRING AS pirate_name_string,
    v:nickname::STRING AS pirate_nickname_string
FROM pirate_json
;

-- Move down a level in the JSON and query a sub-column using a familiar dot notation
SELECT
    v:name::STRING AS pirate_name,
    v:ship.name::STRING AS pirate_ship_name
FROM pirate_json
;

-- When we query a column that doesn’t exist, there’s no error, just a NULL
SELECT
    v:name::STRING AS pirate_name,
    v:loc_buried_treasure::STRING AS pirate_treasure_location
FROM pirate_json
;

---------------------------------------------------------------------------------------------------------------------
-- Arrays
---------------------------------------------------------------------------------------------------------------------
-- Individual elements of an array can be selected using familiar square-bracket notation
-- Common functions such as ARRAY_SIZE/CONTAINS/ADD are available
SELECT
    v:name::STRING AS pirate_name,
    v:years_active AS years_active,
    v:years_active[0] AS active_from,
    -- access the last element of the array using the array
    v:years_active[ARRAY_SIZE(v:years_active) - 1] AS active_to,
FROM pirate_json
;

-- Although arrays can be queried directly, their nested values remain in VARIANT format and are NOT treated as rows
-- Try to query multiple elements and see that the result is not human-readable
SELECT
    v:name::STRING AS pirate_name,
    v:crew::variant AS pirate_crew
FROM pirate_json
;

-- If we’d like to PIVOT the elements of an array into intelligible rows, we must use 2 Snowflake features in conjunction: LATERAL and FLATTEN
-- LATERAL (join) behaves similarly to a loop in a correlated subquery and can reference columns from a table expression: 
--  - 1) SELECT ... FROM <left_hand_table_expression>, LATERAL (<inline_view>) 
--  - 2) for each row in left_hand_table LHT:
--          execute right_hand_subquery RHS using values from current row in LHT
-- FLATTEN a table function that takes a VARIANT, OBJECT, or ARRAY column and produces a lateral view
--  - i.e., an inline view that contains correlation referring to other tables that precede it in the FROM clause
--  - This resulting inline view is used as the input to LATERAL

-- Transform crew members into individual rows
SELECT
    v:name::STRING AS pirate_name,
    -- -- Grab the VALUE output column from the unseated/flattened v:crew sub-column
    -- c.VALUE AS value,
    -- Grab the VALUE output column's fields
    c.VALUE:name::STRING AS crew_member_name,
    c.VALUE:nickname::STRING AS crew_member_nickname
FROM pirate_json,
    -- FLATTEN contains a reference to the crew sub-column from the VARIANT column from in pirate_json table that precedes the command
    -- v:crew is the input that will be unseated into rows
    LATERAL FLATTEN(v:crew) AS c
;

-- Using the same technique as just above, we can handle MULTIPLE nested arrays
-- For example, what weapons did each of Blackbeard’s crew mates employ?
SELECT
    v:name::STRING AS pirate_name,
    c.VALUE:name::STRING AS crew_member_name,
    c.VALUE:nickname::STRING AS crew_member_nickname,
    w.VALUE::STRING AS crew_member_weapons
FROM pirate_json,
    -- FLATTEN contains a reference to the crew sub-column from the VARIANT column from in pirate_json table that precedes the command
    -- v:crew is the input that will be unseated into rows
    LATERAL FLATTEN(v:crew) AS c,
        -- FLATTEN contains a reference to the VALUE property of the flattened crew sub-column from the prior LATERAL FLATTEN command result
        LATERAL FLATTEN(c.VALUE:weapons) AS w
;

-- If we wish to know how many different weapons Israel Hands employed, we can turn to familiar SQL filters and aggregates
SELECT
    COUNT(crew_member_weapons) AS weapons_count
FROM (
    SELECT
        v:name::STRING AS pirate_name,
        c.VALUE:name::STRING AS crew_member_name,
        w.VALUE::STRING AS crew_member_weapons
    FROM pirate_json,
        LATERAL FLATTEN(v:crew) AS c,
            LATERAL FLATTEN(c.VALUE:weapons) AS w
)
WHERE LOWER(crew_member_name) = 'israel hands'
;


---------------------------------------------------------------------------------------------------------------------
-- Determine the depth and levels of semi-structured data
---------------------------------------------------------------------------------------------------------------------
-- The only way to fully know ALL the levels and attributes of a semi-structured file is to scan it to the very end
-- Even then, there is NO guarantee that new attributes won’t appear tomorrow
-- The good news is that the process of determining the depth of a semi-structured file can be automated through Snowflake functions
-- Using the FLATTEN function, we can set the RECURSIVE parameter to automatically expand every element to its ultimate depth
SELECT
    f.*
FROM pirate_json,
    LATERAL FLATTEN(v, RECURSIVE => TRUE) AS f
;

-- Using the PATH column, we can calculate the depth by counting the number of dots and end-level array elements 
--  to give us a complete list of elements and their level in the semi-structured hierarchy
SELECT
    -- Get the size of the array of tokens from the given path
    ARRAY_SIZE(
        -- Tokenize (split) the given string using (via) the given delimiters of '.' and return the tokens as an array
        STRTOK_TO_ARRAY(
            IFF(
                -- If the path ends with a '[x]', then...
                STARTSWITH(RIGHT(f.path, 3), '['),
                -- Replace that opening [ with a period
                LEFT(f.path, LENGTH(f.path) - 3) || '.' || SUBSTR(f.path, LENGTH(f.path) - 1, 2),
                -- Otherwise return the unchanged path
                f.path
            )
        , '.'
        )
    ) AS depth,
    f.key,
    f.path,
    f.value
FROM pirate_json,
    LATERAL FLATTEN(v, RECURSIVE => TRUE) AS f
ORDER BY depth ASC
;



---------------------------------------------------------------------------------------------------------------------
-- Create and load the relational schema	
---------------------------------------------------------------------------------------------------------------------
-- Now that we have the structure, we can deconstruct the semi-structured data into normalized relational tables
-- Starting with the max depth (weapons at depth 3), we can create a dimension with metadata columns and a surrogate key
--  - Some elements, such as account numbers, contain natural keys, which can be used instead
-- Start by creating a table for the weapon dimension, using a sequence to generate a surrogate key (but a hash or a natural key can be used instead)
CREATE OR REPLACE TABLE weapon (
    weapon_id number(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    name varchar NOT NULL,
    __load_name varchar NOT NULL,
    __load_dts timestamp_ntz NOT NULL,

    CONSTRAINT pk_weapon PRIMARY KEY (weapon_id),
    CONSTRAINT ak_weapon_name UNIQUE (name)
)
COMMENT = 'Weapons used by pirates'
;

-- Now, merge the weapon values from the JSON in the latest load and insert them if they don't already exist
MERGE INTO weapon AS w
USING (
    SELECT
        w.value::STRING AS weapon_name,
        p.__load_dts AS __load_dts
    FROM pirate_json AS p,
        LATERAL FLATTEN(v:crew) AS c,
            LATERAL FLATTEN(c.VALUE:weapons) AS w
    WHERE TRUE
        AND __load_dts = (SELECT MAX(__load_dts) FROM pirate_json)
) AS s
ON w.name = s.weapon_name
WHEN NOT MATCHED
THEN INSERT (
    -- weapon_id is generated from a SEQUENCE
    name,
    __load_name,
    __load_dts
)
VALUES (
    weapon_name,
    'ad-hoc load',
    __load_dts
)
;

SELECT * FROM weapon;

-- Repeat the process for the elements at depth 2
-- Ship dimension poses no challenge because it has no child relationships
CREATE OR REPLACE TABLE ship (
    ship_id number(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    name varchar NOT NULL,
    type varchar,
    original_name varchar,
    year_captured number(38, 0),
    __load_name varchar NOT NULL,
    __load_dts timestamp_ntz NOT NULL,

    CONSTRAINT pk_ship PRIMARY KEY (ship_id),
    CONSTRAINT ak_ship_name UNIQUE (name)
)
COMMENT = 'Ships used by pirates'
;

-- Now, merge the ship attributes and their values from the JSON in the latest load and insert them if they don't already exist
MERGE INTO ship
USING (
    SELECT
        v:ship.name::STRING AS ship_name,
        v:ship.type::STRING AS ship_type,
        v:ship.original_name::STRING AS ship_original_name,
        v:ship.year_captured::STRING AS ship_year_captured,
        p.__load_dts
    FROM pirate_json AS p
    WHERE TRUE
        AND __load_dts = (SELECT MAX(__load_dts) FROM pirate_json)
) AS s
ON ship.name = s.ship_name
WHEN NOT MATCHED
THEN INSERT (
    -- ship_id is generated from a SEQUENCE
    name,
    type,
    original_name,
    year_captured,
    __load_name,
    __load_dts
)
VALUES (
    ship_name,
    ship_type,
    ship_original_name,
    ship_year_captured,
    'ad-hoc load',
    __load_dts
)
;

SELECT * FROM ship;


-- Some questions need to be answered before being able to model `crew` and `years_active`, which DO have child relationships
--  - Is a crew member a separate entity from a pirate captain or is it a subtype?
--  - Do years_active refer to how long a crew member has worked under the current captain, or how long they have been active over their entire pirating career?

-- Suppose our domain experts confirm that a crew member is a subtype of pirate and all attributes, 
--  including years_active, are shared as part of a single pirate dimension
-- This is the logical-to-physical rollup scenario
--  - As part of the pirate rollup, we must make sure to include an FK reference to itself to store the relationship between the crew and captain

-- First, we create the `pirate` table structure by fusing a level-2 entity (crew) with its supertype (pirate)
CREATE OR REPLACE TABLE pirate (
    pirate_id number(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    ship_id number(38, 0) NOT NULL,
    crew_of number(38, 0),
    name varchar NOT NULL,
    nickname varchar,
    had_parrot boolean,
    year_born number(38, 0),
    year_died number(38, 0),
    cause_of_death varchar,
    __load_name varchar NOT NULL,
    __load_dts timestamp_ntz NOT NULL,

    CONSTRAINT pk_pirate PRIMARY KEY (pirate_id),
    CONSTRAINT ak_pirate_name UNIQUE (name),
    CONSTRAINT fk_pirate_owns_ship FOREIGN KEY (ship_id) REFERENCES ship (ship_id),
    -- FK reference to itself to store the relationship between the crew and captain
    CONSTRAINT fk_pirate_crew_reports_to FOREIGN KEY (crew_of) REFERENCES pirate (pirate_id)
)
COMMENT = 'Contains pirate information'
;

-- Then we load starting with the top-level dimension (i.e., the level 1 `pirate` entities, since `crew` will need an existing `pirate_id` to report to)
-- So, insert top level pirates from current load
MERGE INTO pirate AS p
USING (
    SELECT
        s.ship_id AS ship_id,
        NULL::INT AS crew_of,
        v:name::STRING AS name,
        v:nickname::STRING AS nickname,
        v:born::STRING AS year_born,
        v:died::STRING AS year_died,
        v:cause_of_death::STRING AS cause_of_death,
        p.__load_dts AS __load_dts
    FROM pirate_json AS p
    INNER JOIN ship AS s
        ON v:ship.name::STRING = s.name
    WHERE TRUE
        AND p.__load_dts = (SELECT MAX(__load_dts) FROM pirate_json)
) AS s
    ON p.name = s.name
WHEN NOT MATCHED
THEN INSERT (
    -- pirate_id is generated from a SEQUENCE
    ship_id,
    crew_of,
    name,
    nickname,
    had_parrot,
    year_born,
    year_died,
    cause_of_death,
    __load_name,
    __load_dts
)
VALUES (
    s.ship_id,
    NULL,
    s.name,
    s.nickname,
    NULL,
    s.year_born,
    s.year_died,
    s.cause_of_death,
    'ad-hoc load',
    s.__load_dts
)
;


-- Once the top-level object (pirate) has been loaded, we can load the crew details from the current load,
--  referencing their captain’s surrogate key in the crew_of column
MERGE INTO pirate AS p
USING (
    SELECT
        s.ship_id AS ship_id,
        pc.pirate_id AS crew_of,
        c.VALUE:name::STRING AS name,
        c.VALUE:nickname::STRING AS nickname,
        c.VALUE:had_bird::boolean AS had_bird,
        p.__load_dts AS __load_dts
    FROM pirate_json AS p
    INNER JOIN ship AS s
        ON v:ship.name::STRING = s.name
    INNER JOIN pirate AS pc
        ON pc.name = v:name::STRING,
    LATERAL FLATTEN (v:crew) AS c
    WHERE TRUE
        AND p.__load_dts = (SELECT MAX(__load_dts) FROM pirate_json)
) AS s
    ON p.name = s.name
WHEN NOT MATCHED
THEN INSERT (
    -- pirate_id is generated from a SEQUENCE
    ship_id,
    crew_of,
    name,
    nickname,
    had_parrot,
    year_born,
    year_died,
    cause_of_death,
    __load_name,
    __load_dts
)
VALUES (
    s.ship_id,
    s.crew_of,
    s.name,
    s.nickname,
    s.had_bird,
    NULL,
    NULL,
    NULL,
    'ad-hoc load',
    s.__load_dts
)
;

-- Verify the results
SELECT * FROM pirate;


-- Notice that one of the attributes, years_active, is missing from the table
-- Because this data is multi-valued, it cannot be included in the pirate dimension without violating 1NF
-- For this, we must create and load a separate pirate_years_active entity
CREATE OR REPLACE TABLE pirate_years_active (
    pirate_id number(38, 0) NOT NULL,
    year_active number(38, 0) NOT NULL,
    __load_name varchar NOT NULL,
    __load_dts timestamp_ntz NOT NULL,

    CONSTRAINT pk_pirate_years_active PRIMARY KEY (pirate_id, year_active),
    CONSTRAINT fk_pirate_references_pirate FOREIGN KEY (pirate_id) REFERENCES pirate (pirate_id)
)
COMMENT = 'Contains pirate years active'
;

-- Again, start by inserting top-level pirates from current load
MERGE INTO pirate_years_active AS pya
USING (
    SELECT
        pc.pirate_id AS pirate_id,
        y.VALUE::INT AS year_active,
        p.__load_name AS __load_name,
        p.__load_dts AS __load_dts
    FROM pirate_json AS p
    INNER JOIN pirate AS pc
        ON pc.name = v:name::STRING,
    LATERAL FLATTEN (v:years_active) AS y
    WHERE TRUE
        AND p.__load_dts = (SELECT MAX(__load_dts) FROM pirate_json)
) AS s
    ON pya.pirate_id = s.pirate_id
WHEN NOT MATCHED
THEN INSERT (
    pirate_id,
    year_active,
    __load_name,
    __load_dts
)
VALUES (
    s.pirate_id,
    s.year_active,
    'ad-hoc load',
    s.__load_dts
)
;

SELECT * FROM pirate_years_active;

-- Then insert crew members from current load
MERGE INTO pirate_years_active AS pya
USING (
    SELECT
        pc.pirate_id AS pirate_id,
        -- crew.pirate_name AS pirate_name,
        crew.year_active AS year_active,
        crew.__load_name AS __load_name,
        crew.__load_dts AS __load_dts
    FROM (
        SELECT
            c.VALUE:name::STRING AS pirate_name,
            y.VALUE::INT AS year_active,
            p.__load_name AS __load_name,
            p.__load_dts AS __load_dts
        FROM pirate_json AS p,
        LATERAL FLATTEN (v:crew) AS c,
        LATERAL FLATTEN (c.VALUE:years_active) AS y
        WHERE TRUE
            AND p.__load_dts = (SELECT MAX(__load_dts) FROM pirate_json)
    ) AS crew
    INNER JOIN pirate AS pc
        ON pc.name = crew.pirate_name
) AS s
ON pya.pirate_id = s.pirate_id
    AND pya.year_active = s.year_active
WHEN NOT MATCHED
THEN INSERT (
    pirate_id,
    year_active,
    __load_name,
    __load_dts
)
VALUES (
    s.pirate_id,
    s.year_active,
    'ad-hoc load',
    s.__load_dts
)
;

SELECT * FROM pirate_years_active;


-- The last missing attribute is pirate weapons
-- In this scenario, we have a many-to-many relationship between the pirate and weapon dimensions
-- Modeling many-to-many relationships requires an associative table in the physical layer
-- This table holds the PKs for the associated entities (i.e., pirate and weapon),
--  as well as the metadata fields that tell us when a certain relationship was first loaded
CREATE OR REPLACE TABLE pirate_weapons (
    pirate_id number(38, 0) NOT NULL,
    weapon_id number(38, 0) NOT NULL,
    __load_name varchar NOT NULL,
    __load_dts timestamp_ntz NOT NULL,

    CONSTRAINT pk_pirate_weapon PRIMARY KEY (pirate_id, weapon_id),
    CONSTRAINT fk_pirate_references_pirate FOREIGN KEY (pirate_id) REFERENCES pirate (pirate_id),
    CONSTRAINT fk_weapon_references_weapon FOREIGN KEY (weapon_id) REFERENCES weapon (weapon_id)
)
COMMENT = 'Contains pirates and their weapons via their IDs'
;

-- Insert pirate weapons from current load
MERGE INTO pirate_weapons AS pw
USING (
    SELECT
        pc.pirate_id AS pirate_id,
        wc.weapon_id AS weapon_id,
        crew.crew_name AS crew_name,
        crew.crew_weapon_name AS crew_weapon_name,
        crew.__load_name AS __load_name,
        crew.__load_dts AS __load_dts
    FROM (
        SELECT
            c.VALUE:name::STRING AS crew_name,
            w.VALUE::STRING AS crew_weapon_name,
            p.__load_name AS __load_name, 
            p.__load_dts AS __load_dts
        FROM pirate_json AS p,
            LATERAL FLATTEN(v:crew) AS c,
            LATERAL FLATTEN(c.VALUE:weapons) AS w
        WHERE TRUE
            AND p.__load_dts = (SELECT MAX(__load_dts) FROM pirate_json)
    ) AS crew
    INNER JOIN pirate pc
        ON pc.name = crew.crew_name
    INNER JOIN weapon AS wc
        ON wc.name = crew.crew_weapon_name
) AS s
ON pw.pirate_id = s.pirate_id
    AND pw.weapon_id = s.weapon_id
WHEN NOT MATCHED
THEN INSERT (
    pirate_id,
    weapon_id,
    __load_name,
    __load_dts
)
VALUES (
    s.pirate_id,
    s.weapon_id,
    s.__load_name,
    s.__load_dts
)
;

SELECT * FROM pirate_weapons;



-- Once our entire schema is loaded, we are free to analyze the data using traditional relational methods
SELECT
    p.name AS pirate_name,
    NVL(p.nickname, 'None') AS nickname,
    s.type AS ship_type,
    NVL(w.name, 'None') AS weapon_name
FROM pirate AS p
INNER JOIN ship AS s
    USING (ship_id)
LEFT JOIN pirate_weapons AS pw
    USING (pirate_id)
LEFT JOIN weapon AS w
    USING (weapon_id)
;