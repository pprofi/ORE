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
          --AND (cnt <> src.ids_extra OR description <> src.ids_desc          )
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
             FROM dv.source k
      except select id, description,cnt, is_current, now()  from dv.target
  where is_current=1
),
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


select * from dv.target

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


-- load hub


-- load stage table into hub = basically loading keys

select stage_table_schema, stage_table_name, stc.column_name, hkc.hub_key_column_name, h.hub_schema, h.hub_name
hkc
from dv_stage_table st join dv_stage_table_column stc on st.stage_table_key=stc.stage_table_key
  join dv_hub_column hc on hc.column_key=stc.column_key
  join dv_hub_key_column hkc on hc.hub_key_column_key=hkc.hub_key_column_key
  join dv_hub h on hkc.hub_key=h.hub_key
where coalesce(stc.is_retired, cast (0 as BOOLEAN)) <> cast (1 as BOOLEAN);

select * from ore_config.dv_hub_column;

select * from ore_config.dv_hub_key_column;

select * from dv_stage_table_column;

/*
WITH  source ()
INSERT INTO hub_table ( column_name )
     VALUES ( )
     ON CONFLICT (list of all hub_business_keys)  do nothing;

-- get hub name
-- load time
-- source name
key_column_key, defaults_col,

  select * from fn_get_dv_object_default_columns('customer','hub');

-- might be several sources to load into one hub table
*/




CREATE TYPE dv_column_match AS
(
  src_column_name VARCHAR(128),
  src_column_type VARCHAR(50),
  dst_column_name VARCHAR(128),
  dst_column_type VARCHAR(50),
  is_conflict     INT
);

CREATE OR REPLACE FUNCTION dv_config_dv_load_table(

  src_schema_in   VARCHAR(128),
  src_table_in    VARCHAR(128),
  dst_schema_in   VARCHAR(128),
  dst_table_in    VARCHAR(128),
  column_match_in REFCURSOR,
  chk_schema_in   VARCHAR(128) default null,
  chk_table_in    VARCHAR(128) default null,
  chk_column_in   VARCHAR(128) default null,
  action_in char(10) default 'do_nothing'-- update or do nothing
)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_block_v      TEXT;
  sql_source_v     TEXT;
  sql_source_from_v     TEXT;
  sql_lookup_v text;
  sql_column_mismatch_v text;
  sql_target_v     TEXT;
  sql_target_val_v TEXT;
  sql_conflict_v   TEXT;
  sql_update_v text;
  sql_action_v     VARCHAR(30);
  delimiter_v      CHAR(2) :=',';
  newline_v        CHAR(3) :=E'\n';
  load_time_v      TIMESTAMPTZ;
BEGIN

  -- code snippets
  sql_block_v:='DO $$' || newline_v || 'declare' || newline_v;
  sql_source_v:='begin' || newline_v || 'with source (' || newline_v || 'select ';
  sql_source_from_v:='from '||src_schema_in||'.' ||src_table_in|| newline_v;
  sql_target_v:='insert into ' || dst_schema_in || '.' || dst_table_in  || ' ';
  sql_target_val_v:='values (';
  sql_conflict_v:='on conflict (';

  if action_in='do_nothing' then
   sql_action_v:=' do nothing;';
  else
    sql_action_v:=' where '||'target.'||chk_column_in||'='||'source.'||chk_column_in||' and ';
  end if
    ;


END
$BODY$
LANGUAGE plpgsql;



DO $$
declare
begin

  with src as(
select stc.column_name,
st.stage_table_schema||'.'st.stage_table_name
from dv_stage_table st join dv_stage_table_column stc on st.stage_table_key=stc.stage_table_key
  join dv_hub_column hc on hc.column_key=stc.column_key
  join dv_hub_key_column hkc on hc.hub_key_column_key=hkc.hub_key_column_key
  join dv_hub h on hkc.hub_key=h.hub_key
where coalesce(stc.is_retired, cast (0 as BOOLEAN)) <> cast (1 as BOOLEAN)
  and stage_table_schema=:stage_tbl_sch
  and stage_table_name=:stage_tbl_name
 and h.hub_name=:hub_name
    and h.hub_schema=:hub_schema

  )
  insert into  hub_schema.hub_name as tg (:<list of columns>)
select :list_of_columns from src
  on conflict (h_key) do nothing;
end$$;


select * from dv_stage_table;
select * from dv_stage_table_column;

create table dv.customer_info
(
CustomerID	varchar(30),
last_name	varchar(50),
first_name	varchar(50),
phone_number	varchar(50)

);


SELECT
  :stage_table_schema,
  :stage_table_name,
  stc.column_name,
  hkc.hub_key_column_name,
  h.hub_schema,
  h.hub_name
FROM dv_stage_table st
  JOIN dv_stage_table_column stc ON st.stage_table_key = stc.stage_table_key
  JOIN dv_hub_column hc ON hc.column_key = stc.column_key
  JOIN dv_hub_key_column hkc ON hc.hub_key_column_key = hkc.hub_key_column_key
  JOIN dv_hub h ON hkc.hub_key = h.hub_key
WHERE coalesce(stc.is_retired, cast(0 AS BOOLEAN)) <> cast(1 AS BOOLEAN)
      AND stage_table_schema = :stage_table_schema
      AND stage_table_name = :stage_table_name
   --   AND h.hub_name = :hub_name
      AND h.hub_schema = :hub_schema
UNION ALL
-- get defaults
SELECT
  :stage_table_schema,
  :stage_table_name,
  CASE WHEN column_name = 'dv_load_date_time'
    THEN cast(now() AS VARCHAR)
  ELSE :stage_table_schema || '.' || :stage_table_name END,
  column_name,
  :hub_schema,
  :hub_name
FROM fn_get_dv_object_default_columns(:object_name_in, 'hub')
WHERE is_key = 0;



-- block structure

sql_bstart_v:='do $$'||E'\n' || 'begin'||E'\n';
sql_trans_v:='';
sql_bend_v:=E'\n'||' end$$;'

-- dynamic upsert statement
WITH sql AS (
  SELECT
    :stage_table_schema AS stage_table_schema,
    :stage_table_name   AS stage_table_name,
    stc.column_name,
    hkc.hub_key_column_name,
    hkc.hub_key_column_type as column_type,
    H.hub_schema,
    H.hub_name
  FROM dv_stage_table st
    JOIN dv_stage_table_column stc ON st.stage_table_key = stc.stage_table_key
    JOIN dv_hub_column hc ON hc.column_key = stc.column_key
    JOIN dv_hub_key_column hkc ON hc.hub_key_column_key = hkc.hub_key_column_key
    JOIN dv_hub H ON hkc.hub_key = H.hub_key
  WHERE COALESCE(stc.is_retired, CAST(0 AS BOOLEAN)) <> CAST(1 AS BOOLEAN)
  --AND stage_table_schema = :stage_table_schema
  --AND stage_table_name = :stage_table_name
  --   AND h.hub_name = :hub_name
  --AND H.hub_schema = :hub_schema
  UNION ALL
  -- get defaults
  SELECT
    :stage_table_schema,
    :stage_table_name,
    CASE WHEN column_name = 'dv_load_date_time'
      THEN 'now()'
    ELSE quote_literal('DV.customer_info')
    --:stage_table_schema || '.' || :stage_table_name
    END,
    column_name AS hub_key_column_name,
    column_type,
    :hub_schema,
    :hub_name
  FROM fn_get_dv_object_default_columns('customer', 'hub')
  WHERE is_key = 0
)
SELECT array_to_string(array_agg(t.ssql), E'\n')
FROM (
       SELECT 'with src as ' || '( select ' || array_to_string(array_agg('cast('||sql.column_name||' as '||sql.column_type||')'), ', ') AS ssql
       FROM sql
       UNION ALL
       SELECT DISTINCT ' from ' || sql.stage_table_schema || '.' || stage_table_name || ')'
       FROM sql
       UNION ALL
       SELECT 'insert into ' || sql.hub_schema || '.' || fn_get_object_name(sql.hub_name,'hub') || '(' ||
              array_to_string(array_agg(sql.hub_key_column_name), ', ') || ')'
       FROM sql
       GROUP BY sql.hub_schema, fn_get_object_name(sql.hub_name,'hub')
       UNION ALL
       SELECT DISTINCT 'select * from src'||E'\n'|| 'on conflict(' || (SELECT column_name
                                                            FROM fn_get_dv_object_default_columns(:object_name_in,
                                                                                                  'hub',
                                                                                                  'Object_Key')) ||
                       ') ' || 'do nothing;' || E'\n'
       FROM sql) t;

/*
WITH src AS ( SELECT
                cast(CustomerID AS VARCHAR),
                cast('DV.customer_info' AS VARCHAR),
                cast(now() AS TIMESTAMP)
              FROM DV.customer_info)
INSERT INTO DV.h_customer (CustomerID, dv_record_source, dv_load_date_time)
  SELECT *
  FROM src
ON CONFLICT (h_customer_key)
  DO NOTHING;



SELECT array_to_string(array_agg('cast(' || quote_literal(column_name) || ' as ' || column_type || ')'), ', ') FROM
                       fn_get_dv_object_default_columns(:object_name_in, 'hub');

*/
select dv_config_dv_load_hub(
  'DV',
  'customer_info',
  'DV',
  'customer'
);

DO $$
BEGIN
  UPDATE DV.customer_info
  SET status = 'PROCESSING'
  WHERE status = 'RAW';

  WITH src AS ( SELECT
                  cast(CustomerID AS VARCHAR),
                  cast('DV.customer_info' AS VARCHAR),
                  cast(now() AS TIMESTAMP)
                FROM DV.customer_info
                WHERE status = 'PROCESSING')
  INSERT INTO DV.h_customer (CustomerID, dv_record_source, dv_load_date_time)
    SELECT *
    FROM src
  ON CONFLICT (h_customer_key)
    DO NOTHING;

  UPDATE DV.customer_info
  SET status = 'PROCESSED'
  WHERE status = 'PROCESSING';

END$$;

 select * from dv_defaults;

select fn_get_object_name('customer','satsurrogate');

-- load hub

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

SELECT
      stc.column_name            stage_col_name,
      hkc.hub_key_column_name    hub_col_name,
      hkc.hub_key_column_type AS column_type
    FROM dv_stage_table st
      JOIN dv_stage_table_column stc ON st.stage_table_key = stc.stage_table_key
      JOIN dv_hub_column hc ON hc.column_key = stc.column_key
      JOIN dv_hub_key_column hkc ON hc.hub_key_column_key = hkc.hub_key_column_key
      JOIN dv_hub H ON hkc.hub_key = H.hub_key
    WHERE COALESCE(stc.is_retired, CAST(0 AS BOOLEAN)) <> CAST(1 AS BOOLEAN)
          AND stage_table_schema = stage_table_schema_in
          AND stage_table_name = stage_table_name_in
          AND h.hub_name = hub_name_in
          AND H.hub_schema = hub_schema_in
          AND st.owner_key = h.owner_key
    UNION ALL
    -- get defaults
    SELECT
      CASE WHEN column_name = 'dv_load_date_time'
        THEN 'now()'
      ELSE quote_literal(stage_table_schema_in || '.' || stage_table_name_in)
      END         AS stage_col_name,
      column_name AS hub_col_name,
      column_type
    FROM fn_get_dv_object_default_columns('customer', 'hub')
    WHERE is_key = 0;

select * from dv_satellite_column sc join dv_satellite s on s.satellite_key=sc.satellite_key ;



select * from dv_default_column where object_type='satellite';

SELECT
  hkc.hub_key,
  hkc.owner_key,
  hkc.hub_key_column_name,
  hc.column_key
FROM
  dv_hub_key_column hkc
  LEFT JOIN dv_hub_column hc ON hc.hub_key_column_key = hkc.hub_key_column_key;

-- columns from staging

WITH sql AS (
  SELECT
    stc.column_name AS stage_col_name,
    stc.column_name AS sat_col_name,
    stc.column_type,
    0               AS is_surrogate_key,
    CASE WHEN hkc.hub_key_column_name IS NULL
      THEN 1
    ELSE 0 END      AS is_change_check,
    hkc.hub_key_column_name,
    hkc.hub_key_column_type,
    0                  is_default
  FROM dv_stage_table st
    JOIN dv_stage_table_column stc ON st.stage_table_key = stc.stage_table_key
    JOIN dv_satellite_column sc ON sc.column_key = stc.column_key
    JOIN dv_satellite s ON s.satellite_key = sc.satellite_key
    LEFT JOIN (SELECT
                 hkc.hub_key,
                 hkc.owner_key,
                 hkc.hub_key_column_name,
                 hc.column_key,
                 hkc.hub_key_column_type
               FROM
                 dv_hub_key_column hkc
                 JOIN dv_hub_column hc ON hc.hub_key_column_key = hkc.hub_key_column_key) hkc
      ON hkc.column_key = stc.column_key
         AND s.hub_key = hkc.hub_key AND s.owner_key = hkc.owner_key
  WHERE COALESCE(s.is_retired, CAST(0 AS BOOLEAN)) <> CAST(1 AS BOOLEAN)
        AND stage_table_schema = :stage_table_schema_in
        AND stage_table_name = :stage_table_name_in
        AND s.satellite_name = :satellite_name_in
        AND s.satellite_schema = :satellite_schema_in
        AND st.owner_key = s.owner_key
  -- default columns
  UNION ALL
  SELECT
    CASE WHEN column_name IN ('dv_source_date_time', 'dv_rowstartdate', 'dv_rowstartdate')
      THEN 'now()'
    WHEN column_name = 'dv_record_source'
      THEN quote_literal(:stage_table_schema_in || '.' || :stage_table_name_in)
    WHEN column_name = 'dv_row_is_current'
      THEN '1'
    WHEN column_name = 'dv_rowenddate'
      THEN quote_literal(to_date('01-01-2100 00:00:00', 'dd-mm-yyyy hh24:mi:ss'))
    ELSE column_name
    END         AS stage_col_name,
    column_name AS hub_col_name,
    column_type,
    0,
    0,
    NULL,
    NULL,
    1
  FROM fn_get_dv_object_default_columns(:satellite_name_in, 'satellite')
  WHERE is_key = 0
  -- related hub key
  UNION ALL
  SELECT
    c.column_name,
    c.column_name AS sat_col_name,
    c.column_type,
    1             AS is_surrogate_key,
    0,
    NULL,
    NULL,
    0
  FROM dv_satellite s
    JOIN dv_hub h ON h.hub_key = s.hub_key
    JOIN fn_get_dv_object_default_columns(h.hub_name, 'hub') c ON 1 = 1
  WHERE s.owner_key = h.owner_key
        AND c.is_key = 1)
SELECT array_to_string(array_agg(t.ssql), E'\n')
FROM (
       SELECT 'with src as ' || '( select distinct ' ||
              array_to_string(
                  array_agg('cast(' || (CASE WHEN sql.is_default = 1
                    THEN ' '
                                        ELSE ' s.' END) || sql.stage_col_name || ' as ' || sql.column_type || ') as ' ||
                            sql.sat_col_name),
                  ', ') AS ssql
       FROM sql
       UNION ALL
       SELECT DISTINCT
         ' from ' || :stage_table_schema_in || '.' || :stage_table_name_in || ' as s left join ' || :hub_schema_in ||
         '.' ||
         :resolved_hub_name_in ||
         ' as h '
         ' on s.' || sql.sat_col_name || '=h.' || sql.sat_col_name ||
         ' where s.status=' ||
         quote_literal('PROCESSING')
       FROM sql
       WHERE sql.is_surrogate_key = 1
       UNION ALL
       -- except statement
       SELECT ' except  select ' || array_to_string(
           array_agg(CASE WHEN sql.is_default = 0
             THEN sql.sat_col_name
                     ELSE sql.stage_col_name END),
           ', ') || ' from ' || :satellite_schema_in || '.' || :satellite_name_in || ' where ' ||
              ' dv_row_is_current=1 ),'
       FROM sql
       UNION ALL
       SELECT ' updates as ( update ' || :satellite_schema_in || '.' || :satellite_name_in
       UNION ALL
       SELECT 'set dv_row_is_current=0,dv_rowenddate=now() '
       UNION ALL
       SELECT ' from src '
       UNION ALL
       SELECT
         ' where ' || sql.sat_col_name || '=src.' || sql.sat_col_name || ' and dv_row_is_current=1  returning src.* )'
       FROM sql
       WHERE sql.is_surrogate_key = 1

       UNION ALL
       SELECT DISTINCT CASE WHEN :load_type = 'delta'
         THEN ' '
                       ELSE
                         ' , deleted as ( update ' || :satellite_schema_in || '.' || :satellite_name_in
                         ||
                         ' as s  set s.dv_row_is_current=0,s.dv_rowenddate=now() from src  where s.' || sql.sat_col_name
                         || '=src.' || sql.sat_col_name ||

                         ' and src.hub_SK is null and s.dv_row_is_current=1 ) '
                       END
       FROM sql
        where sql.is_surrogate_key = 1
       UNION ALL
       SELECT ' insert into ' || :satellite_schema_in || '.' || :satellite_name_in || '(' ||
              array_to_string(array_agg(sql.sat_col_name),
                              ', ') || ')'
       FROM sql
       UNION ALL
       SELECT 'select distinct r.* from (select u.* from updates u union all select src.* from src ) r '
       UNION ALL
       SELECT ' on conflict (' || sql.sat_col_name || ', dv_row_is_current' || ') where dv_row_is_current=1 do nothing;'
       FROM sql
       WHERE sql.is_surrogate_key = 1) t;


WITH src AS ( SELECT DISTINCT
                cast(s.CustomerID AS VARCHAR)       AS CustomerID,
                cast(s.last_name AS VARCHAR)        AS last_name,
                cast(s.first_name AS VARCHAR)       AS first_name,
                cast(s.phone_number AS VARCHAR)     AS phone_number,
                cast(1 AS BIT)                      AS dv_row_is_current,
                cast(dv_is_tombstone AS BIT)        AS dv_is_tombstone,
                cast('DV.customer_info' AS VARCHAR) AS dv_record_source,
                cast('2100-01-01' AS TIMESTAMP)     AS dv_rowenddate,
                cast(now() AS TIMESTAMP)            AS dv_rowstartdate,
                cast(now() AS TIMESTAMP)            AS dv_source_date_time,
                cast(s.h_customer_key AS INT)       AS h_customer_key
              FROM DV.customer_info AS s LEFT JOIN DV.customer AS h ON s.h_customer_key = h.h_customer_key
              WHERE s.status = 'PROCESSING'
              EXCEPT SELECT
                       CustomerID,
                       last_name,
                       first_name,
                       phone_number,
                       1,
                       dv_is_tombstone,
                       'DV.customer_info',
                       '2100-01-01',
                       now(),
                       now(),
                       h_customer_key
                     FROM DV.customer_detail
                     WHERE dv_row_is_current = 1 ),
    updates AS (
    UPDATE DV.customer_detail
  SET dv_row_is_current = 0, dv_rowenddate = now()
  FROM src
  WHERE h_customer_key = src.h_customer_key AND dv_row_is_current = 1
  RETURNING src.* )
  , deleted AS ( UPDATE DV.customer_detail AS s
SET s.dv_row_is_current = 0, s.dv_rowenddate = now() FROM src
WHERE sh_customer_key = src.h_customer_key AND src.hub_SK IS NULL AND s.dv_row_is_current = 1 )
INSERT INTO DV.customer_detail (CustomerID, last_name, first_name, phone_number, dv_row_is_current, dv_is_tombstone, dv_record_source, dv_rowenddate, dv_rowstartdate, dv_source_date_time, h_customer_key)
  SELECT DISTINCT r.*
  FROM (SELECT u.*
        FROM updates u
        UNION ALL SELECT src.*
                  FROM src) r
ON CONFLICT (h_customer_key, dv_row_is_current)
  WHERE dv_row_is_current = 1
  DO NOTHING;


select dv_config_dv_load_satellite(
  'DV',
  'customer_info',
  'DV',
  'customer_detail',
  'full'
)
;

-- full load

-- mark as deleted all records not found in stage
DO $$
BEGIN
  UPDATE DV.customer_info
  SET status = 'PROCESSING'
  WHERE status = 'RAW';
  WITH src AS ( SELECT DISTINCT
                  cast(s.CustomerID AS VARCHAR)       AS CustomerID,
                  cast(s.last_name AS VARCHAR)        AS last_name,
                  cast(s.first_name AS VARCHAR)       AS first_name,
                  cast(s.phone_number AS VARCHAR)     AS phone_number,
                  cast(1 AS BIT)                      AS dv_row_is_current,
                  cast(dv_is_tombstone AS BIT)        AS dv_is_tombstone,
                  cast('DV.customer_info' AS VARCHAR) AS dv_record_source,
                  cast('2100-01-01' AS TIMESTAMP)     AS dv_rowenddate,
                  cast(now() AS TIMESTAMP)            AS dv_rowstartdate,
                  cast(now() AS TIMESTAMP)            AS dv_source_date_time,
                  cast(s.h_customer_key AS INT)       AS h_customer_key,
                  h.h_customer_key                    AS hub_SK
                FROM DV.customer_info AS s LEFT JOIN DV.h_customer AS h ON s.h_customer_key = h.h_customer_key
                WHERE s.status = 'PROCESSING'
                EXCEPT SELECT
                         CustomerID,
                         last_name,
                         first_name,
                         phone_number,
                         1,
                         dv_is_tombstone,
                         'DV.customer_info',
                         '2100-01-01',
                         now(),
                         now(),
                         h_customer_key,
                         h_customer_key
                       FROM DV.s_customer_detail
                       WHERE dv_row_is_current = 1 ),
      deleted AS ( UPDATE DV.s_customer_detail AS s
    SET s.dv_row_is_current = 0, s.dv_rowenddate = now() FROM src
    WHERE s.CustomerID = src.CustomerID AND src.hub_SK IS NULL AND s.dv_row_is_current = 1 ),
      updates AS ( UPDATE DV.s_customer_detail
      AS u
    SET u.dv_row_is_current = 0, u.dv_rowenddate = now()
    FROM src
    WHERE u.h_customer_key = src.h_customer_key AND src.hub_SK IS NOT NULL AND u.dv_row_is_current = 1
    RETURNING src.* )
  INSERT INTO DV.s_customer_detail (CustomerID, last_name, first_name, phone_number, dv_row_is_current, dv_is_tombstone, dv_record_source, dv_rowenddate, dv_rowstartdate, dv_source_date_time, h_customer_key)
    SELECT DISTINCT r.*
    FROM (SELECT
            CustomerID,
            last_name,
            first_name,
            phone_number,
            dv_row_is_current,
            dv_is_tombstone,
            dv_record_source,
            dv_rowenddate,
            dv_rowstartdate,
            dv_source_date_time,
            h_customer_key
          FROM updates u
          UNION ALL SELECT
                      CustomerID,
                      last_name,
                      first_name,
                      phone_number,
                      dv_row_is_current,
                      dv_is_tombstone,
                      dv_record_source,
                      dv_rowenddate,
                      dv_rowstartdate,
                      dv_source_date_time,
                      h_customer_key
                    FROM src
                    WHERE src.hub_SK IS NOT NULL) r;
  UPDATE DV.customer_info
  SET status = 'PROCESSED'
  WHERE status = 'PROCESSING';

END$$;
