CREATE OR REPLACE FUNCTION dv_config_dv_create_stage_table(
  object_name_in   VARCHAR(128),
  object_schema_in VARCHAR(128),
  recreate_flag_in CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_v              TEXT;
  sql_create_table_v TEXT;
  sql_create_index_v TEXT;
  rec CURSOR FOR
    SELECT *
                   FROM fn_get_dv_object_default_columns(object_name_in, 'stage_table')
                   UNION ALL
                   SELECT
                     sc.column_name,
                     sc.column_type,
                     sc.column_length,
                     sc.column_precision,
                     sc.column_scale,
                     0 AS is_nullable,
                     0 AS is_key,
                     0 AS is_indexed
                   FROM dv_stage_table t
                     INNER JOIN dv_stage_table_column sc
                       ON t.stage_table_key = sc.stage_table_key
                   WHERE t.stage_table_schema = object_schema_in
                         AND t.stage_table_name = object_name_in

  ;
BEGIN

  OPEN rec;

  -- create statement

  SELECT ore_config.dv_config_dv_table_create(object_name_in,
                                              object_schema_in,
                                              'rec',
                                              recreate_flag_in
  )
  INTO sql_create_table_v;

  CLOSE rec;

  RETURN sql_create_table_v;

END
$BODY$
LANGUAGE 'plpgsql';