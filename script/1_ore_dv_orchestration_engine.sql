/*-------------------------------------------
        OPTIMAL REPORTING ENGINE
        DATA VAULT ORCHESTRATION SCRIPTS
        loading data
-------------------------------------------*/
SET search_path TO ore_config;

-- load hub
-- load satellite
-- load stage - combined hub & satellite load

-- update dates into defaults


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

CREATE OR REPLACE FUNCTION dv_config_dv_load_hub(
  source_system_in  VARCHAR(128),
  stage_table_schema_in VARCHAR(128),
  stage_table_in    VARCHAR(128),
  hub_name_in       VARCHAR(128),
  hub_schema_in  VARCHAR(128)
)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_block_v text;
  sql_source_v text;
  sql_target_v text;
  sql_target_val_v text;
  sql_conflict_v text;
  sql_action_v varchar(30);
  delimiter_v char(2):=',';
  newline_v          CHAR(3) :=E'\n';
  load_time_v TIMESTAMPTZ;
BEGIN

  -- code snippets
  sql_block_v:='DO $$'||newline_v||'declare'||newline_v;
  sql_source_v:='begin'||newline_v||'with source ('||newline_v||'select ';
  sql_target_v:='insert into '||hub_schema_in||'.'||hub_name_in||' ';
  sql_target_val_v:='values (';
  sql_conflict_v:='on conflict (';
  sql_action_v:=' do nothing;';


END
$BODY$
LANGUAGE plpgsql;


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


-- dynamic upsert statement
WITH sql AS (
  SELECT
    :stage_table_schema AS stage_table_schema,
    :stage_table_name   AS stage_table_name,
    stc.column_name,
    hkc.hub_key_column_name,
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
    :hub_schema,
    :hub_name
  FROM fn_get_dv_object_default_columns('customer', 'hub')
  WHERE is_key = 0
)
SELECT array_to_string(array_agg(t.ssql), E'\n')
FROM (
       SELECT 'with src as ' || '( select ' || array_to_string(array_agg(sql.column_name), ', ') AS ssql
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


WITH src AS ( SELECT
                CustomerID,
                'DV.customer_info',
                now()
              FROM DV.customer_info)
INSERT INTO DV.h_customer (CustomerID, dv_record_source, dv_load_date_time)
  SELECT *
  FROM src
ON CONFLICT (h_customer_key)
  DO NOTHING;






select column_name from fn_get_dv_object_default_columns(:object_name_in, 'hub','Object_Key')

