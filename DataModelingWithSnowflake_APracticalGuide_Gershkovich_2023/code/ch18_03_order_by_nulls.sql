-- NULLs are considered 'higher' than non-NULL values
SELECT
    column1
FROM VALUES 
    (1),
    (null),
    (2),
    (null),
    (3),
    (0.01)
ORDER BY column1 DESC
;

SELECT
    column1
FROM VALUES
    ('1'),
    (null),
    ('2'),
    (null),
    ('3'),
    ('0.01'),
    ('A'),
    ('a'),
    ('$')
ORDER BY column1 DESC
;


-- To place NULLs at the END of the result in DESC, use NULLS LAST 
SELECT
    column1
FROM VALUES
    (1),
    (null),
    (2),
    (null),
    (3),
    (0.01)
ORDER BY column1 DESC NULLS LAST
;


-- To place NULLs at the START of the result in ASC, use NULLS FIRST
SELECT
    column1
FROM VALUES
    (1),
    (null),
    (2),
    (null),
    (3),
    (0.01)
ORDER BY column1 ASC NULLS FIRST
;