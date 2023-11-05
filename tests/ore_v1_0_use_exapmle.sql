-- prior to completing exercise run release script for ore v1.0

-- set default schema
SET search_path TO ore_config;

-- to get all keys you need to do lookup in respective config tables


-- configure dv
-- add source system
SELECT dv_config_object_insert('dv_source_system',
                               '{{"source_system_name","new_system"},{"source_system_schema","dv"},
                                {"release_number","0"},{"owner_key","1"}}');
-- add stage table

SELECT dv_config_object_insert('dv_stage_table',
                               '{{"system_key","1"},{"stage_table_schema","dv"},{"stage_table_name","customer_info"},
                                {"release_number","0"},{"owner_key","1"}}');
-- add stage table columns

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","CustomerID"},
                               {"column_type","varchar"},
                               {"column_length","30"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","1"},
                               {"release_number","0"},{"owner_key","1"}}');

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","last_name"},
                               {"column_type","varchar"},
                               {"column_length","50"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","2"},
                               {"release_number","0"},{"owner_key","1"}}');

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","first_name"},
                               {"column_type","varchar"},
                               {"column_length","50"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","3"},
                               {"release_number","0"},{"owner_key","1"}}');

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","phone_number"},
                               {"column_type","varchar"},
                               {"column_length","50"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","4"},
                               {"release_number","0"},{"owner_key","1"}}');
-- add hub details
SELECT dv_config_object_insert('dv_hub',
                               '{{"hub_name","customer"},{"hub_schema","dv"},{"release_number","0"},{"owner_key","1"}}');
-- add hub key column

SELECT dv_config_object_insert('dv_hub_key_column',
                               '{{"hub_key","1"},{"hub_key_column_name","CustomerID"},
                               {"hub_key_column_type","varchar"},
                               {"hub_key_column_length","30"},
                               {"hub_key_column_precision","0"},
                               {"hub_key_column_scale","0"},
                               {"release_number","0"},{"owner_key","1"}}');
-- hook hub columns to stage table columns

SELECT dv_config_object_insert('dv_hub_column',
                               '{{"hub_key_column_key","1"},{"column_key","1"},
                                {"release_number","0"},{"owner_key","1"}}');
-- add satellite details

SELECT dv_config_object_insert('dv_satellite',
                               '{{"satellite_name","customer_detail"},{"satellite_schema","dv"},
                               {"hub_key","1"},
                               {"link_hub_satellite_flag","H"},
                               {"release_number","0"},{"owner_key","1"}}');
-- hook satellite columns to stage table columns
SELECT dv_config_object_insert('dv_satellite_column',
                               '{{"satellite_key","1"},{"column_key","1"},
                                {"release_number","0"},{"owner_key","1"}}');

SELECT dv_config_object_insert('dv_satellite_column',
                               '{{"satellite_key","1"},{"column_key","2"},
                                {"release_number","0"},{"owner_key","1"}}');

SELECT dv_config_object_insert('dv_satellite_column',
                               '{{"satellite_key","1"},{"column_key","3"},
                                {"release_number","0"},{"owner_key","1"}}');

SELECT dv_config_object_insert('dv_satellite_column',
                               '{{"satellite_key","1"},{"column_key","4"},
                                {"release_number","0"},{"owner_key","1"}}');

-- create dv schema
CREATE SCHEMA dv;

SET SEARCH_PATH TO ore_config;
-- generate create statements
-- stage table
SELECT *
FROM dv_config_dv_create_stage_table(
    'customer_info',
    'dv',
    'N'
);
-- hub
SELECT *
                   FROM fn_get_dv_object_default_columns('customer', 'hub');


                   SELECT
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN rtrim(coalesce(column_prefix, '') || replace(d.column_name, '%', 'customer') ||
                           coalesce(column_suffix, ''))
              ELSE d.column_name END AS column_name,
              column_type,
              column_length,
              column_precision,
              column_scale,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 0
              ELSE 1 END             AS is_nullable,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 1
              ELSE 0 END             AS is_key,
             d.is_indexed
            FROM dv_default_column d
            WHERE object_type = 'hub'
         --   and (d.object_column_type=object_column_type_in or object_column_type_in is null)
            ORDER BY is_key DESC


SELECT *
FROM dv_config_dv_create_hub(
    'customer',
    'dv',
    'N'
);
-- satellite
SELECT *
FROM dv_config_dv_create_satellite(
    'customer_detail',
    'DV',
    'H',
    'N'
);

-- generate load hub statement

SELECT dv_config_dv_load_hub(
    'dv',
    'customer_info',
    'dv',
    'customer'
);

-- generate load satellite statement
SELECT dv_config_dv_load_satellite(
    'dv',
    'customer_info',
    'dv',
    'customer_detail',
    'full'
);












