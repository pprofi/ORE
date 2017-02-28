-- create hub table
CREATE OR REPLACE FUNCTION dv_config_dv_create_hub(
  object_name_in      VARCHAR(128),
  object_schema_in    VARCHAR(128),
  recreate_flag_in    CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_v              TEXT;
  sql_create_table_v TEXT;
  sql_create_index_v TEXT;
  hub_name_v varchar(200);

    rec CURSOR FOR SELECT *
                   FROM fn_get_dv_object_default_columns(object_name_in, 'hub')
                   UNION ALL
                   SELECT
                     hkc.hub_key_column_name      AS column_name,
                     hkc.hub_key_column_type      AS column_type,
                     hkc.hub_key_column_length    AS column_length,
                     hkc.hub_key_column_precision AS column_precision,
                     hkc.hub_key_column_scale     AS column_scale,
                     1                            AS is_nullable,
                     0                            AS is_key,
                     1 as is_indexed
                   FROM ore_config.dv_hub h
                     INNER JOIN ore_config.dv_hub_key_column hkc
                       ON h.hub_key = hkc.hub_key
                   WHERE h.hub_schema = object_schema_in
                         AND h.hub_name = object_name_in
                      ;
BEGIN

  OPEN rec;

  -- create statement
  -- generate hub name
  hub_name_v:=fn_get_object_name(object_name_in,'hub');

  SELECT ore_config.dv_config_dv_table_create(hub_name_v,
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