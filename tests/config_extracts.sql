set SEARCH_PATH to ore_config;

-- model design

select object_type, object_schema, object_name, object_relationship, is_parent
FROM
  (
    select 'hub' as object_type, hub_schema as object_schema, hub_name as object_name, hub_key as object_relationship, 1 as is_parent from dv_hub
    union ALL
    select 'satellite', satellite_schema, satellite_name, hub_key, 0 from dv_satellite
    union ALL
    select 'source_system', source_system_schema, source_system_name, source_system_key, 1 from dv_source_system
    union all
    select 'source_table', source_table_schema, source_table_name, system_key, 0 from dv_source_table
    union all
    select 'stage_table', stage_table_schema, stage_table_name, system_key, 0 from dv_stage_table
  )t;

-- model contents

  select
    object_type ,
    object_name ,
    object_schema ,
    column_name ,
    column_type ,
    column_length ,
    column_precision ,
    column_scale
from (
select 'stage_table' as object_type, s.stage_table_name as object_name, s.stage_table_schema as object_schema,
  st.column_name as column_name, st.column_type , st.column_length, st.column_precision, st.column_scale
from dv_stage_table_column st join dv_stage_table s on s.stage_table_key=st.stage_table_key
  union all
select 'hub', h.hub_name, h.hub_schema, hk.hub_key_column_name, hk.hub_key_column_type, hk.hub_key_column_length, hk.hub_key_column_precision, hk.hub_key_column_scale
from dv_hub_key_column hk join dv_hub h on h.hub_key=hk.hub_key

) t;

-- mapping

select
    mapping_type ,
    object_name_in ,
    object_schema_in ,
    column_name_in ,
    object_name_out ,
    object_schema_out ,
    column_name_out
from
(
select 'satellite' as mapping_type, s.satellite_name as object_name_in, s.satellite_schema as object_schema_in ,
stc.column_name as column_name_in, st.stage_table_name as object_name_out, st.stage_table_schema as object_schema_out,
  stc.column_name as column_name_out
from dv_satellite_column sc join dv_stage_table_column stc on sc.column_key=stc.column_key
  join dv_satellite s on s.satellite_key=sc.satellite_key
  join dv_stage_table st on st.stage_table_key=stc.stage_table_key
 union ALL
 select 'hub', h.hub_name, h.hub_schema, hkc.hub_key_column_name, st.stage_table_name, st.stage_table_schema, stc.column_name from dv_hub h join dv_hub_key_column hkc on hkc.hub_key=h.hub_key
  join dv_hub_column hc on hc.hub_key_column_key=hkc.hub_key_column_key
  join dv_stage_table_column stc on stc.column_key=hc.column_key
  join dv_stage_table st on st.stage_table_key=stc.stage_table_key
)t;

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
              'source' AS object_type,,
              ''  AS object_name,
              '' AS object_schema,
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
      ) m ON s.object_key = case when m.object_type in ('hub','satellite') then m.stage_table_key  else m.object_key end AND s.object_type = m.object_type
  ) x
ORDER BY 1, 10;

select * from dv_schedule_valid_tasks
order by schedule_name, task_level;


select * from dv_business_rule;



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
              'source' AS object_type,
              source_table_name as source_name,
              source_table_schema AS source_schema,
              source_table_key as object_key,
              '' AS br_name,
              '' AS br_logic,
              '' AS br_load_type,
              '' AS br_type
            FROM dv_source_table
            union ALL
              SELECT
              'stage' AS object_type,
              stage_table_name as source_name,
              stage_table_schema AS source_schema,
              stage_table_key as object_key,
              '' AS br_name,
              '' AS br_logic,
              '' AS br_load_type,
              '' AS br_type
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
      ) m ON s.object_key =  m.object_key  AND case when s.object_type in ('hub','satellite') then 'stage' else s.object_type end = m.object_type
  ) x
ORDER BY 1, 10;