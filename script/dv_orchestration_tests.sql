BEGIN;

CREATE TEMPORARY TABLE newvals(id integer, somedata text);

INSERT INTO newvals(id, somedata) VALUES (2, 'Joe'), (3, 'Alan');

LOCK TABLE testtable IN EXCLUSIVE MODE;

UPDATE testtable
SET somedata = newvals.somedata
FROM newvals
WHERE newvals.id = testtable.id;

INSERT INTO testtable
SELECT newvals.id, newvals.somedata
FROM newvals
LEFT OUTER JOIN testtable ON (testtable.id = newvals.id)
WHERE testtable.id IS NULL;

COMMIT;

-- test example
CREATE TABLE dv.source
(
  ids       INT,
  ids_desc  VARCHAR(20),
  ids_extra INT
);

CREATE TABLE dv.target
(
  id          INT,
  description VARCHAR(20),
  cnt         INT,
  is_current  INT,
  dv_change   TIMESTAMP
);

INSERT INTO dv.source
  SELECT
    1,
    'one',
    0
  UNION ALL
  SELECT
    2,
    'second',
    1
  UNION ALL
  SELECT
    3,
    'three',
    2
  UNION ALL
  SELECT
    4,
    'forth',
    3;

delete from dv.target;

INSERT INTO dv.target
  SELECT
    1,
    'one',
    0,
    1,
    now()
  UNION ALL
  SELECT
    3,
    'third',
    4,1, now()
  ;

CREATE UNIQUE INDEX unique_target ON dv.target (id, is_current) WHERE is_current=1;

select * from dv.source;
select * from dv.target;


CREATE TABLE target
(
    id INTEGER,
    description VARCHAR(20),
    cnt INTEGER,
    is_current INTEGER,
    dv_change TIMESTAMP
);
CREATE UNIQUE INDEX unique_target ON target (id, is_current);


-- option 1 not working - no insert after update
WITH src AS (SELECT k.*, 1 as is_current
             FROM dv.source k)
INSERT INTO dv.target (id, description, cnt, is_current, dv_change)
  SELECT
    src.ids,
    src.ids_desc,
    src.ids_extra,
    src.is_current,
    now()
  FROM src
ON CONFLICT (id,is_current)  DO NOTHING;

ON CONFLICT (id)
  WHERE dst.description <> src.ids_desc OR dst.cnt <> src.ids_extra
  DO UPDATE SET
    dst.is_current = 0,
    dst.dv_change  = now()
-- RETURNING src.*;

SET search_path TO DV;

WITH src AS (SELECT
               k.*,
               1 AS is_current
             FROM dv.source k),
    updates AS (
    UPDATE dv.target t
    SET is_current = 0, dv_change = now()
    FROM src
    WHERE src.ids = id and t.is_current=1
          AND (cnt <> src.ids_extra OR description <> src.ids_desc)
    RETURNING *
  )
INSERT INTO dv.target (id, description, cnt, is_current, dv_change)
  SELECT
    src.ids,
    src.ids_desc,
    src.ids_extra,
    src.is_current,
    now()
  FROM src
ON CONFLICT (id,is_current) DO NOTHING;


select * from dv.source;
select * from dv.target;

--------------------------------------------------

-- second option

WITH src AS (SELECT
               DISTINCT
               k.*,
               1     AS is_current,
               now() AS dv_change
             FROM dv.source k),
    updates AS ( -- delta load
    UPDATE dv.target t
    SET is_current = 0, dv_change = now()
    FROM src
    WHERE src.ids = id AND t.is_current = 1 -- and 'delta_load'
          AND (cnt <> src.ids_extra OR description <> src.ids_desc)
    RETURNING src.*
  )
  /*,
  deleted as (
    -- in case of full load
    delete -- if havent found anything
    from dv.target
    where
  )
*/
-- select s.*, now() from  src s
-- union ALL
 INSERT INTO dv.target (id, description, cnt, is_current, dv_change)
  SELECT DISTINCT r.*
  FROM
    (
      SELECT u.*
      FROM updates u
     UNION ALL
    SELECT *
    FROM src
    ) r
ON CONFLICT (id, is_current) where is_current=1
  DO NOTHING;
;

INSERT INTO dv.source
  SELECT
    3,
    'one',
    0
  UNION ALL
  SELECT
    3,
    'second',
    1
  ;

select * from dv.source;

select * from dv.target;

-- option 3 need somehow to process duplicates
-- if more than 1 row of the same = take only one and report duplicates

drop index unique_target;

CREATE UNIQUE INDEX unique_target ON target (id, is_current) where dv.target.is_current=1;

-- duplicate check

-- duplicates found
-- check threshold for satellite
select ids, count(*) from dv.source
group by ids
having count(*)>1;


-- need to load only one record
select * from (
select t.*, row_number() over(partition by ids ) as rn from dv.source t) S
where s.rn=1;



-----------------------------------------------------

CREATE TABLE dv.account
(
  id bigserial,
  name varchar,
  surname varchar,
  address varchar,
  PRIMARY KEY (id),
  CONSTRAINT unique_person UNIQUE (name, surname, address)
);

select * from dv.account;

INSERT INTO dv.account (id, name, surname, address)
VALUES (1, 'Вася', 'Пупкин', 'Москва, Кремль')
ON CONFLICT (id) DO NOTHING;

INSERT INTO dv.account (id, name, surname, address)
VALUES (DEFAULT, 'Вася', 'Пупкин', 'Москва, Кремль')
ON CONFLICT (name, surname, address) DO NOTHING;


INSERT INTO dv.account (id, name, surname, address)
VALUES (DEFAULT, 'Вася', 'Пупкин', 'Москва, Кремль')
ON CONFLICT ON CONSTRAINT unique_person DO NOTHING;


INSERT INTO dv.account (id, name, surname, address)
VALUES (DEFAULT, 'Вася', 'Пупкин', 'Москва, Кремль')
ON CONFLICT (name,surname, address) WHERE name='Вася' DO NOTHING
returning *;

INSERT INTO dv.account (id, name, surname, address)
VALUES (1, 'Петя', 'Петров', 'Москва, Кремль')
ON CONFLICT (id)
DO UPDATE SET
name='Петя',
surname='Петров';

INSERT INTO dv.account AS a (id, name, surname, address)
VALUES (1, 'Вася', 'Пупкин', 'Москва, Кремль')
ON CONFLICT (id) DO UPDATE SET
name=EXCLUDED.name
WHERE a.name not like '%Кремль%';


WITH s AS (SELECT
             1                AS id,
             'Вася'           AS name,
             'Пупкин'         AS surname,
             'Москва, Кремль' AS address)
INSERT INTO dv.account (id, name, surname, address)
  SELECT
    s.id,
    s.name,
    s.surname,
    s.address
  FROM s
ON CONFLICT (id)
  WHERE name = 'Петя'
  DO UPDATE SET name = s.name

returning *;

BEGIN;
LOCK TABLE spider_count IN SHARE ROW EXCLUSIVE MODE;
WITH upsert AS ($upsert RETURNING *) $insert WHERE NOT EXISTS (SELECT * FROM upsert);
COMMIT;