-- modelling helper script
-- individual modelling
-- owner - release - source system - source table - stage table - hub - satellite
-- business rules

-- separately schedule, schedule tasks, schedule hierarchy

set SEARCH_PATH to ore_config;

-- 1 model high level
-- 2 model columns
-- 3 link columns to source

create table dv_model_L1_design
(
  object_type varchar,
  object_schema varchar,
  object_name varchar,
  object_relationship varchar
);

create table dv_model_L2_contents
(
  object_type varchar,
  object_name varchar,
  object_schema varchar,
  column_name varchar,
  column_type varchar,
  column_length int,
  column_precision int,
  column_scale int
);

create table dv_model_L3_mapping
(
 mapping_type varchar,
 object_name_in varchar,
 object_schema_in varchar,
 column_name_in varchar,
 object_name_out varchar,
 object_schema_out varchar,
 column_name_out varchar
);