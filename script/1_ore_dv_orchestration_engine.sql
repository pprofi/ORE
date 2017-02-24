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
  sql_block_start_v TEXT;
  sql_block_end_v   TEXT;
  sql_block_body_v  TEXT;
  delimiter_v       CHAR(2) :=',';
  newline_v         CHAR(3) :=E'\n';
  load_time_v       TIMESTAMPTZ;
BEGIN


  -- code snippets
  sql_block_start_v:='DO $$' || newline_v || 'begin' || newline_v;
  sql_block_end_v:=newline_v || 'end$$;';

  -- dynamic upsert statement
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
    FROM fn_get_dv_object_default_columns('customer', 'hub')
    WHERE is_key = 0
  )
  SELECT array_to_string(array_agg(t.ssql), E'\n')
  FROM (
         SELECT 'with src as ' || '( select ' ||
                array_to_string(array_agg('cast(' || sql.stage_col_name || ' as ' || sql.column_type || ')'),
                                ', ') AS ssql
         FROM sql
         UNION ALL
         SELECT DISTINCT ' from ' || stage_table_schema_in || '.' || stage_table_name_in || ')'
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

  RETURN sql_block_start_v || sql_block_body_v || sql_block_end_v;

END
$BODY$
LANGUAGE plpgsql;


