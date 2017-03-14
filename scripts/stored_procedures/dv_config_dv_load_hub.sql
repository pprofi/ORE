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
  load_date_time_v          VARCHAR(10):='now()';
  hub_name_v varchar(50);
BEGIN
/*-----TO DO add error handling generation if load failed checks on counts
  */

  -- hub name check
  hub_name_v:= fn_get_object_name(hub_name_in, 'hub');

  IF COALESCE(hub_name_v, '') = ''
  THEN
    RAISE NOTICE 'Not valid hub name --> %', hub_name_in;
    RETURN NULL;
  END IF;


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
        THEN load_date_time_v
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
         SELECT 'insert into ' || hub_schema_in || '.' || hub_name_v || '(' ||
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
