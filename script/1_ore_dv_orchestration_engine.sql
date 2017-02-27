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


CREATE OR REPLACE FUNCTION dv_config_dv_load_hub(
  stage_table_schema_in VARCHAR(128),
  stage_table_name_in   VARCHAR(128),
  hub_schema_in         VARCHAR(128),
  hub_name_in           VARCHAR(128)
)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_block_start_v    TEXT;
  sql_block_end_v      TEXT;
  sql_block_body_v     TEXT;
  sql_process_start_v  TEXT;
  sql_process_finish_v TEXT;
  delimiter_v          CHAR(2) :=',';
  newline_v            CHAR(3) :=E'\n';
  load_time_v          TIMESTAMPTZ;
BEGIN
/*-----TO DO add error handling generation if load failed checks on counts
  */

  -- code snippets
  sql_block_start_v:='DO $$' || newline_v || 'begin' || newline_v;
  sql_block_end_v:=newline_v || 'end$$;';

  -- update processing status stage
  sql_process_start_v:=
  'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' || quote_literal('PROCESSING') ||
  ' where status=' ||
  quote_literal('RAW')||';'||newline_v;
  sql_process_finish_v:=
  newline_v || 'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' ||
  quote_literal('PROCESSED') || ' where status=' ||
  quote_literal('PROCESSING')||';'||newline_v;

  -- dynamic upsert statement
  -- add process status select-update in transaction
  WITH sql AS (
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
    FROM fn_get_dv_object_default_columns(hub_name_in, 'hub')
    WHERE is_key = 0
  )
  SELECT array_to_string(array_agg(t.ssql), E'\n')
  FROM (
         SELECT 'with src as ' || '( select ' ||
                array_to_string(array_agg('cast(' || sql.stage_col_name || ' as ' || sql.column_type || ')'),
                                ', ') AS ssql
         FROM sql
         UNION ALL
         SELECT DISTINCT ' from ' || stage_table_schema_in || '.' || stage_table_name_in || ' where status=' ||
                         quote_literal('PROCESSING') || ')'
         FROM sql
         UNION ALL
         SELECT 'insert into ' || hub_schema_in || '.' || fn_get_object_name(hub_name_in, 'hub') || '(' ||
                array_to_string(array_agg(sql.hub_col_name), ', ') || ')'
         FROM sql
         -- GROUP BY sql.hub_schema, fn_get_object_name(sql.hub_name, 'hub')
         UNION ALL
         SELECT DISTINCT 'select * from src' || E'\n' || 'on conflict(' || (SELECT column_name
                                                                            FROM fn_get_dv_object_default_columns(
                                                                                hub_name_in,
                                                                                'hub',
                                                                                'Object_Key')) ||
                         ') ' || 'do nothing;' || E'\n'
         FROM sql) t
  INTO sql_block_body_v;

  RETURN sql_block_start_v || sql_process_start_v || sql_block_body_v || sql_process_finish_v || sql_block_end_v;

END
$BODY$
LANGUAGE plpgsql;


-- load satellite table
-- full and delta load
-- need to lookup hub keys first

CREATE OR REPLACE FUNCTION dv_config_dv_load_satellite(
  stage_table_schema_in VARCHAR(128),
  stage_table_name_in   VARCHAR(128),
  satellite_schema_in   VARCHAR(128),
  satellite_name_in     VARCHAR(128),
  load_type_in          VARCHAR(10) DEFAULT 'delta'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_block_start_v    TEXT;
  sql_block_end_v      TEXT;
  sql_block_body_v     TEXT;
  sql_process_start_v  TEXT;
  sql_process_finish_v TEXT;
  delimiter_v          CHAR(2) :=',';
  newline_v            CHAR(3) :=E'\n';
  load_time_v          TIMESTAMPTZ;
  hub_name_v           VARCHAR(50);
  hub_schema_v         VARCHAR(50);
  load_date_time_v     VARCHAR(10) :='now()';
  default_enddate_v    VARCHAR(50) :=quote_literal(to_date('01-01-2100 00:00:00', 'dd-mm-yyyy hh24:mi:ss'));
  satellite_name_v     VARCHAR(50);
BEGIN

  -- code snippets
  -- block
  sql_block_start_v:='DO $$' || newline_v || 'begin' || newline_v;
  sql_block_end_v:=newline_v || 'end$$;';

  -- update status of records in stage table
  sql_process_start_v:=
  'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' || quote_literal('PROCESSING') ||
  ' where status=' ||
  quote_literal('RAW') || ';' || newline_v;
  sql_process_finish_v:=
  newline_v || 'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' ||
  quote_literal('PROCESSED') || ' where status=' ||
  quote_literal('PROCESSING') || ';' || newline_v;

  -- get related hub name
  SELECT
    fn_get_object_name(h.hub_name, 'hub'),
    h.hub_schema
  INTO
    hub_name_v, hub_schema_v
  FROM dv_satellite s
    JOIN dv_hub h ON h.hub_key = s.hub_key AND h.owner_key = s.owner_key
  WHERE s.satellite_schema = satellite_schema_in AND s.satellite_name = satellite_name_in;

  -- get satellite name
  satellite_name_v:=fn_get_object_name(satellite_name_in, 'sat');

  -- dynamic upsert statement
  -- full load means that records for whose keys in staging not found will be marked as deleted
  -- lookup keys in hub
  -- insert records for new keys
  -- update changed records for existing keys, insert new record with changed values

  WITH sql AS (
    -- list of stage table- satellite match and hub key lookup column
    SELECT
      stc.column_name AS stage_col_name,
      stc.column_name AS sat_col_name,
      stc.column_type,
      0               AS is_surrogate_key,
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
          AND stage_table_schema = stage_table_schema_in
          AND stage_table_name = stage_table_name_in
          AND s.satellite_name = satellite_name_in
          AND s.satellite_schema = satellite_schema_in
          AND st.owner_key = s.owner_key
    -- list of default columns
    UNION ALL
    SELECT
      CASE WHEN column_name IN ('dv_source_date_time', 'dv_rowstartdate', 'dv_rowstartdate')
        THEN load_date_time_v
      WHEN column_name = 'dv_record_source'
        THEN quote_literal(stage_table_schema_in || '.' || stage_table_name_in)
      WHEN column_name = 'dv_row_is_current'
        THEN '1'
      WHEN column_name = 'dv_rowenddate'
        THEN default_enddate_v
      ELSE column_name
      END         AS stage_col_name,
      column_name AS hub_col_name,
      column_type,
      0,
      NULL,
      NULL,
      1
    FROM fn_get_dv_object_default_columns(satellite_name_in, 'satellite')
    WHERE is_key = 0
    -- related hub surrogate key
    UNION ALL
    SELECT
      c.column_name,
      c.column_name AS sat_col_name,
      c.column_type,
      1             AS is_surrogate_key,
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
                    array_agg(
                        'cast(' || (CASE WHEN sql.is_default = 1
                          THEN ' '
                                    ELSE ' s.' END) || sql.stage_col_name || ' as ' || sql.column_type || ') as ' ||
                        sql.sat_col_name),
                    ', ') AS ssql
         FROM sql
         UNION ALL
         SELECT DISTINCT ', h.' || sql.sat_col_name || ' as hub_SK ' || ' from ' || stage_table_schema_in || '.' ||
                         stage_table_name_in
                         || ' as s left join ' || hub_schema_v ||
                         '.' ||
                         hub_name_v ||
                         ' as h '
                         ' on s.' || sql.sat_col_name || '=h.' || sql.sat_col_name ||
                         ' where s.status=' ||
                         quote_literal('PROCESSING')
         FROM sql
         WHERE sql.is_surrogate_key = 1
         UNION ALL
         -- except statement : checking source to exclude duplicates in time series
         SELECT ' except  select ' || array_to_string(
             array_agg(CASE WHEN sql.is_default = 0
               THEN sql.sat_col_name
                       ELSE sql.stage_col_name END),
             ', ')
         FROM sql
         UNION ALL
         SELECT ', ' || sat_col_name || ' from ' || satellite_schema_in || '.' || satellite_name_v || ' where ' ||
                ' dv_row_is_current=1 ),'
         FROM sql
         WHERE
           sql.is_surrogate_key = 1
         UNION ALL
         -- full load - mark all keys that not found in stage as deleted
         -- lookup key values
         SELECT DISTINCT CASE WHEN load_type_in = 'delta'
           THEN ' '
                         ELSE
                           '  deleted as ( update ' || satellite_schema_in || '.' || satellite_name_v
                           ||
                           ' as s  set s.dv_rowenddate=' || load_date_time_v ||
                           ' from src  where ' ||
                           -- list of lookup columns
                           array_to_string(array_agg(' s.' || sql.sat_col_name || '=src.' || sql.stage_col_name),
                                           ' and ')
                           || ' and src.hub_SK is null and s.dv_row_is_current=1 ), '
                         END
         FROM sql
         WHERE sql.hub_key_column_name IS NOT NULL
         UNION ALL
         -- update row if key is found
         SELECT ' updates as ( update ' || satellite_schema_in || '.' || satellite_name_v
         UNION ALL
         SELECT 'as u  set u.dv_row_is_current=0,u.dv_rowenddate=' || load_date_time_v
         UNION ALL
         SELECT ' from src '
         UNION ALL
         SELECT ' where u.' || sql.sat_col_name || '=src.' || sql.sat_col_name ||
                ' and src.hub_SK is not null and u.dv_row_is_current=1 ' || E'\n returning src.* )'
         FROM sql
         WHERE sql.is_surrogate_key = 1
         UNION ALL

         -- if new record insert
         SELECT ' insert into ' || satellite_schema_in || '.' || satellite_name_v || '(' ||
                array_to_string(array_agg(sql.sat_col_name),
                                ', ') || ')'
         FROM sql
         UNION ALL
         SELECT 'select distinct r.* from (select ' || array_to_string(array_agg(sql.sat_col_name), ', ')
                ||
                ' from updates u union all select ' || array_to_string(array_agg(sql.sat_col_name), ', ') ||
                ' from src where src.hub_SK is not null ) r '
         FROM sql
         UNION ALL
         SELECT ' ;'
         --
         /*  SELECT
             ' on conflict (' || sql.sat_col_name || ', dv_row_is_current' || ') where dv_row_is_current=1 do nothing; '
           FROM sql
           WHERE sql.is_surrogate_key = 1*/
       ) t
  INTO sql_block_body_v;


  RETURN sql_block_start_v || sql_process_start_v || sql_block_body_v || sql_process_finish_v || sql_block_end_v;

END
$BODY$
LANGUAGE plpgsql;

