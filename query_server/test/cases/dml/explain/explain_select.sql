EXPLAIN
    SELECT * FROM
    (VALUES  (9, 'nine'),(2, 'two'), (1, 'one'), (3, 'three')) AS t (num,letter)
    order by num desc limit 2;

EXPLAIN
    SELECT * FROM
    (VALUES  (9, 'nine'),(2, 'two'), (1, 'one'), (3, 'three')) AS t (num,letter)
    order by num desc, letter limit 3;