CREATE FUNCTION fn_get_dv_object_load_script (object_key_in integer, object_type_in character varying, load_type_in character varying, owner_key_in integer) RETURNS text
	LANGUAGE plpgsql
AS $$
DECLARE
  sql_v TEXT;
BEGIN

  CASE
    WHEN object_type_in in ('business_rule' , 'business_rule_proc')
    THEN
      -- 1. business_rule/ stage table
      -- if it is stored procedure then different
      SELECT business_rule_logic
      INTO sql_v
      FROM dv_business_rule
      WHERE business_rule_key = object_key_in
            AND is_retired = false
            AND owner_key = owner_key_in;

    WHEN object_type_in='hub'
    THEN
      -- 2. hub
      SELECT DISTINCT
        'select ore_config.dv_config_dv_load_hub(''' || st.stage_table_schema || ''',''' || st.stage_table_name || ''',''' ||
        h.hub_schema || ''',''' ||
        h.hub_name || ''');'
      INTO sql_v
      FROM dv_hub h
        JOIN dv_hub_key_column hk ON h.hub_key = hk.hub_key
        JOIN dv_hub_column hc ON hc.hub_key_column_key = hk.hub_key_column_key
        JOIN dv_stage_table_column sc ON sc.column_key = hc.column_key
        JOIN dv_stage_table st ON st.stage_table_key = sc.stage_table_key
      WHERE h.owner_key = owner_key_in AND h.is_retired = false AND st.is_retired = false AND sc.is_retired = false
            AND st.stage_table_key = object_key_in;
    WHEN object_type_in='satellite'
    THEN
      -- 3. satellite
      SELECT DISTINCT
        'select ore_config.dv_config_dv_load_satellite(''' || st.stage_table_schema || ''',''' || st.stage_table_name || ''',''' ||
        s.satellite_schema || ''',''' || s.satellite_name || ''',''' || load_type_in || ''');'
      INTO sql_v
      FROM dv_satellite s
        JOIN dv_satellite_column sc ON sc.satellite_key = s.satellite_key
        JOIN dv_stage_table_column stc ON stc.column_key = sc.column_key
        JOIN dv_stage_table st ON stc.stage_table_key = st.stage_table_key
      WHERE s.is_retired = false AND st.is_retired = false AND stc.is_retired = false
            AND s.owner_key = owner_key_in
            AND st.stage_table_key = object_key_in;
  ELSE
    -- 4. source or anything else -  nothing
    sql_v:='';
  END CASE;


  RETURN sql_v;

END
$$
