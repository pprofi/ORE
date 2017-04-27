SET SEARCH_PATH TO ore_config;

-- model design

SELECT
  object_type,
  object_schema,
  object_name,
  object_relationship,
  is_parent
FROM
  (
    SELECT
      'hub'   AS object_type,
      hub_schema as object_schema,
      hub_name as object_name,
      hub_key AS object_relationship,
      1       AS is_parent
    FROM dv_hub
    UNION ALL
    SELECT
      'satellite',
      satellite_schema,
      satellite_name,
      hub_key,
      0
    FROM dv_satellite
    UNION ALL
    SELECT
      'source_system',
      source_system_schema,
      source_system_name,
      source_system_key,
      1
    FROM dv_source_system
    UNION ALL
    SELECT
      'source_table',
      source_table_schema,
      source_table_name,
      system_key,
      0
    FROM dv_source_table
    UNION ALL
    SELECT
      'stage_table',
      stage_table_schema,
      stage_table_name,
      system_key,
      0
    FROM dv_stage_table
  ) t;

-- model contents

SELECT
  object_type,
  object_name,
  object_schema,
  column_name,
  column_type,
  column_length,
  column_precision,
  column_scale
FROM (
       SELECT
         'stage_table'        AS object_type,
         s.stage_table_name   AS object_name,
         s.stage_table_schema AS object_schema,
         st.column_name       AS column_name,
         st.column_type,
         st.column_length,
         st.column_precision,
         st.column_scale
       FROM dv_stage_table_column st
         JOIN dv_stage_table s ON s.stage_table_key = st.stage_table_key
       UNION ALL
       SELECT
         'hub',
         h.hub_name,
         h.hub_schema,
         hk.hub_key_column_name,
         hk.hub_key_column_type,
         hk.hub_key_column_length,
         hk.hub_key_column_precision,
         hk.hub_key_column_scale
       FROM dv_hub_key_column hk
         JOIN dv_hub h ON h.hub_key = hk.hub_key

     ) t;

-- mapping

SELECT
  mapping_type,
  object_name_in,
  object_schema_in,
  column_name_in,
  object_name_out,
  object_schema_out,
  column_name_out
FROM
  (
    SELECT
      'satellite'           AS mapping_type,
      s.satellite_name      AS object_name_in,
      s.satellite_schema    AS object_schema_in,
      stc.column_name       AS column_name_in,
      st.stage_table_name   AS object_name_out,
      st.stage_table_schema AS object_schema_out,
      stc.column_name       AS column_name_out
    FROM dv_satellite_column sc
      JOIN dv_stage_table_column stc ON sc.column_key = stc.column_key
      JOIN dv_satellite s ON s.satellite_key = sc.satellite_key
      JOIN dv_stage_table st ON st.stage_table_key = stc.stage_table_key
    UNION ALL
    SELECT
      'hub',
      h.hub_name,
      h.hub_schema,
      hkc.hub_key_column_name,
      st.stage_table_name,
      st.stage_table_schema,
      stc.column_name
    FROM dv_hub h
      JOIN dv_hub_key_column hkc ON hkc.hub_key = h.hub_key
      JOIN dv_hub_column hc ON hc.hub_key_column_key = hkc.hub_key_column_key
      JOIN dv_stage_table_column stc ON stc.column_key = hc.column_key
      JOIN dv_stage_table st ON st.stage_table_key = stc.stage_table_key
  ) t;