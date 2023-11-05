-- create satellite
CREATE OR REPLACE FUNCTION dv_config_dv_create_satellite(
  object_name_in   VARCHAR(128),
  object_schema_in VARCHAR(128),
  object_type_in   CHAR(1) DEFAULT 'H',
  recreate_flag_in CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_v              TEXT;
  sql_create_table_v TEXT;
  sql_create_index_v TEXT;
  sat_name_v         VARCHAR(200);
  hub_name_v         VARCHAR(30);

  -- get columns
    rec CURSOR (hub_name VARCHAR ) FOR SELECT *
                                       FROM fn_get_dv_object_default_columns(object_name_in, 'satellite')
                                       UNION ALL
                                       -- get hub surrogate key columns
                                       SELECT
                                         column_name,
                                         column_type,
                                         column_length,
                                         column_precision,
                                         column_scale,
                                         1 AS is_nullable,
                                         0 AS is_key,
                                         1 AS is_indexed
                                       FROM fn_get_dv_object_default_columns(hub_name, 'hub', 'Object_Key')
                                       UNION ALL
                                       SELECT
                                         stc.column_name      AS column_name,
                                         stc.column_type      AS column_type,
                                         stc.column_length    AS column_length,
                                         stc.column_precision AS column_precision,
                                         stc.column_scale     AS column_scale,
                                         1                    AS is_nullable,
                                         0                    AS is_key,
                                         0                    AS is_indexed

                                       FROM ore_config.dv_satellite s
                                         INNER JOIN ore_config.dv_satellite_column sc
                                           ON s.satellite_key = sc.satellite_key
                                         JOIN dv_stage_table_column stc ON sc.column_key = stc.column_key
                                       WHERE s.satellite_schema = object_schema_in
                                             AND s.satellite_name = object_name_in;
BEGIN

  -- it will be easy to find link name as well using the same query for column definition selection
  -- just add 2d parameter to cursor, type of object : link or hub
  -- find related hub name
  SELECT h.hub_name
  INTO hub_name_v
  FROM dv_satellite s
    JOIN dv_hub h ON s.hub_key = h.hub_key
  WHERE s.satellite_name = object_name_in AND s.satellite_schema = object_schema_in
        AND s.link_hub_satellite_flag = 'H';

  -- generate satellite name
  sat_name_v:=fn_get_object_name(object_name_in, 'satellite');


  RAISE NOTICE ' Sat name %-->', sat_name_v;
  RAISE NOTICE ' Hub name %-->', hub_name_v;

  OPEN rec (hub_name:=hub_name_v);
  -- create statement


  SELECT ore_config.dv_config_dv_table_create(sat_name_v,
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
