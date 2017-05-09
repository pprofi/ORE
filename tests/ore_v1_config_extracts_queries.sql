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
      'hub'      AS object_type,
      hub_schema AS object_schema,
      hub_name   AS object_name,
      hub_key    AS object_relationship,
      1          AS is_parent
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

-- scheduling


SELECT
  schedule_name,
  object_type,
  source_name,
  source_schema,
  source_load_type,
  business_rule_name,
  business_rule_logic,
  business_rule_load_type,
  business_rule_type,
  rn_order
FROM
  (
    SELECT
      s.schedule_name,
      m.object_type,
      m.source_name,
      m.source_schema,
      'delta'      AS source_load_type,
      br_name      AS business_rule_name,
      br_logic     AS business_rule_logic,
      br_load_type AS business_rule_load_type,
      br_type      AS business_rule_type,
      task_level   AS rn_order

    FROM dv_schedule_valid_tasks s
      JOIN


      (
        SELECT
          t.object_type,
          object_name,
          object_schema,
          source_name,
          source_schema,
          stage_table_key,
          object_key,
          br_name,
          br_logic,
          br_load_type,
          br_type
        FROM
          (
            /*
                        SELECT DISTINCT
                          'satellite'           AS object_type,
                          s.satellite_name      AS object_name,
                          s.satellite_schema    AS object_schema,
                          st.stage_table_name   AS source_name,
                          st.stage_table_schema AS source_schema,
                          st.stage_table_key,
                          s.satellite_key       AS object_key,
                          ''                    AS br_name,
                          ''                    AS br_logic,
                          ''                    AS br_load_type,
                          ''                    AS br_type

                        FROM dv_satellite_column sc
                          JOIN dv_stage_table_column stc ON sc.column_key = stc.column_key
                          JOIN dv_satellite s ON s.satellite_key = sc.satellite_key
                          JOIN dv_stage_table st ON st.stage_table_key = stc.stage_table_key
                        UNION ALL
                        SELECT DISTINCT
                          'hub',
                          h.hub_name,
                          h.hub_schema,
                          st.stage_table_name,
                          st.stage_table_schema,
                          st.stage_table_key,
                          h.hub_key,
                          '',
                          '',
                          '',
                          ''
                        FROM dv_hub h
                          JOIN dv_hub_key_column hkc ON hkc.hub_key = h.hub_key
                          JOIN dv_hub_column hc ON hc.hub_key_column_key = hkc.hub_key_column_key
                          JOIN dv_stage_table_column stc ON stc.column_key = hc.column_key
                          JOIN dv_stage_table st ON st.stage_table_key = stc.stage_table_key

                        UNION ALL*/
            SELECT
              'source' AS object_type,
              ,
              ''       AS object_name,
              ''       AS object_schema,
              source_table_name,
              source_table_schema,
              source_table_key,
              source_table_key,
              '',
              '',
              '',
              ''
            FROM dv_source_table
            UNION ALL
            SELECT
              CASE WHEN b.business_rule_type = 'procedure'
                THEN 'business_rule_proc'
              ELSE 'business_rule' END,
              '',
              '',
              s.stage_table_name,
              stage_table_schema,
              b.stage_table_key,
              b.business_rule_key,
              b.business_rule_name,
              b.business_rule_logic,
              b.load_type,
              b.business_rule_type

            FROM dv_business_rule b
              JOIN dv_stage_table s ON s.stage_table_key = b.stage_table_key
          ) t
      ) m ON s.object_key = CASE WHEN m.object_type IN ('hub', 'satellite')
        THEN m.stage_table_key
                            ELSE m.object_key END AND s.object_type = m.object_type
  ) x
ORDER BY 1, 10;

SELECT *
FROM dv_schedule_valid_tasks
ORDER BY schedule_name, task_level;


SELECT *
FROM dv_business_rule;

--------------------

SELECT
  schedule_name,
  object_type,
  source_name,
  source_schema,
  source_load_type,
  business_rule_name,
  business_rule_logic,
  business_rule_load_type,
  business_rule_type,
  rn_order
FROM
  (
    SELECT
      s.schedule_name,
      s.object_type,
      m.source_name,
      m.source_schema,
      'delta'      AS source_load_type,
      br_name      AS business_rule_name,
      br_logic     AS business_rule_logic,
      br_load_type AS business_rule_load_type,
      br_type      AS business_rule_type,
      task_level   AS rn_order

    FROM dv_schedule_valid_tasks s
      JOIN


      (
        SELECT
          t.object_type,
          source_name,
          source_schema,
          object_key,
          br_name,
          br_logic,
          br_load_type,
          br_type
        FROM
          (

            SELECT
              'source'            AS object_type,
              source_table_name   AS source_name,
              source_table_schema AS source_schema,
              source_table_key    AS object_key,
              ''                  AS br_name,
              ''                  AS br_logic,
              ''                  AS br_load_type,
              ''                  AS br_type
            FROM dv_source_table
            UNION ALL
            SELECT
              'stage'            AS object_type,
              stage_table_name   AS source_name,
              stage_table_schema AS source_schema,
              stage_table_key    AS object_key,
              ''                 AS br_name,
              ''                 AS br_logic,
              ''                 AS br_load_type,
              ''                 AS br_type
            FROM dv_stage_table
            UNION ALL
            SELECT
              CASE WHEN b.business_rule_type = 'procedure'
                THEN 'business_rule_proc'
              ELSE 'business_rule' END,
              s.stage_table_name,
              stage_table_schema,
              b.business_rule_key,
              b.business_rule_name,
              b.business_rule_logic,
              b.load_type,
              b.business_rule_type

            FROM dv_business_rule b
              JOIN dv_stage_table s ON s.stage_table_key = b.stage_table_key
          ) t
      ) m ON s.object_key = m.object_key AND CASE WHEN s.object_type IN ('hub', 'satellite')
        THEN 'stage'
                                             ELSE s.object_type END = m.object_type
  ) x
ORDER BY 1, 10;