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
  object_relationship varchar,
  is_parent int
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

-- something for business rules
-- tasks and schedules

-- procedure loads 3 files

-- phase 1
-- 1 create owner if not exists
-- 2 create release if not exists
-- in a loop
-- 3 create all source systems and related source tables and stage tables
-- in a loop
-- 4 create hubs and related satellites
CREATE OR REPLACE FUNCTION dv_model_l1_load()
  RETURNS VOID AS
$BODY$
DECLARE
  release_key_v    INT;
  release_number_v VARCHAR;
  owner_key_v      INT;
  owner_name_v     VARCHAR;
  object_v         VARCHAR [] [];
  r                RECORD;
  rd               RECORD;
  object_key_v     INT;

BEGIN
  -- owner
  SELECT
    ow.owner_key,
    d.object_name
  INTO owner_key_v, owner_name_v
  FROM dv_model_L1_design d LEFT JOIN dv_owner ow ON ow.owner_name = d.object_name
  WHERE object_type = 'owner';

  IF owner_key_v IS NULL
  THEN
    -- add owner
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [2, 2]);
    object_v [1] [1]:='owner_name';
    object_v [1] [2]:=owner_name_v;
    object_v [2] [1]:='owner_description';
    object_v [2] [2]:=owner_name_v;

    SELECT dv_config_object_insert('dv_owner',
                                   object_v);
  END IF;

  -- release
  SELECT
    r.release_key,
    d.object_name
  INTO release_key_v, release_number_v
  FROM dv_model_L1_design d LEFT JOIN dv_release r ON r.release_number = d.object_name
  WHERE object_type = 'release';

  IF release_key_v IS NULL
  THEN
    -- add release
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [3, 2]);
    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_v;
    object_v [2] [1]:='release_description';
    object_v [2] [2]:=release_number_v;
    object_v [3] [1]:='owner_key';
    object_v [3] [2]:= owner_key_v;
    SELECT dv_config_object_insert('dv_release',
                                   object_v);
  END IF;

  -- go through the rest and add DV objects into config
  -- source_systems
  object_v:=array_fill(NULL :: VARCHAR, ARRAY [5, 2]);
  object_v [1] [1]:='release_number';
  object_v [1] [2]:=release_number_v;
  object_v [2] [1]:='owner_key';
  object_v [2] [2]:= owner_key_v;

  FOR r IN (SELECT *
            FROM dv_model_L1_design
            WHERE object_type IN ('source_system', 'hub')) LOOP

    object_v [3] [1]:=r.object_type || '_schema';
    object_v [3] [2]:=r.object_schema;
    object_v [4] [1]:=r.object_type || '_name';
    object_v [4] [2]:=r.object_name;

    SELECT dv_config_object_insert('dv_' || r.object_type,
                                   object_v);

    EXECUTE 'select ' || r.object_type || '_key from dv_' || r.object_type || ' where ' || r.object_type || '_schema='''
            || r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO object_key_v;

    -- looping through dependamt objects
    FOR rd IN (SELECT *
               FROM dv_model_L1_design
               WHERE rd.is_parent IS NULL AND r.object_relationship = rd.object_relationship)
    LOOP
      object_v [3] [1]:=r.object_type || '_schema';
      object_v [3] [2]:=r.object_schema;
      object_v [4] [1]:=r.object_type || '_name';
      object_v [4] [2]:=r.object_name;

      object_v [5] [1]:= (CASE WHEN r.object_type = 'source_system'
        THEN 'system'
                          ELSE r.object_type END) || '_key';
      object_v [5] [2]:=object_key_v;

      SELECT dv_config_object_insert('dv_' || r.object_type,
                                     object_v);

    END LOOP;

  END LOOP;


END
$BODY$
LANGUAGE plpgsql;

-- phase 2
--  file 2 add all column contents
create or replace function dv_model_l2_load()
RETURNS VOID AS
$BODY$
DECLARE
begin
END
$BODY$
LANGUAGE plpgsql;

-- phase 3
-- file 3 add mappings
create or replace function dv_model_l1_load()
RETURNS VOID AS
$BODY$
DECLARE
begin
END
$BODY$
LANGUAGE plpgsql;

-- phase 4
-- add schedules (names)

-- phase 5
-- add business rules and schedule-tasks
-- generate additional rules for stage and source update statuses and

--
select * FROM
  (
SELECT
       t.table_name,
c.column_name,
          c.is_nullable,
          replace(c.data_type, 'character varying', 'varchar') AS data_type,
          CASE WHEN c.column_name = kcu.column_name
            THEN 1
          ELSE NULL END                                        AS is_key,
          cast(0 AS INTEGER)                                   AS is_found,
          column_default
        FROM information_schema.tables t
          JOIN information_schema.columns c
            ON t.table_schema = c.table_schema AND t.table_name = c.table_name
          LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON tc.table_catalog = t.table_catalog
               AND tc.table_schema = t.table_schema
               AND tc.table_name = t.table_name
               AND tc.constraint_type = 'PRIMARY KEY'
          LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON kcu.table_catalog = tc.table_catalog
               AND kcu.table_schema = tc.table_schema
               AND kcu.table_name = tc.table_name
               AND kcu.constraint_name = tc.constraint_name
        WHERE t.table_schema = 'ore_config' -- AND t.table_name = object_type_in
              AND c.column_name NOT IN ('updated_by', 'updated_datetime')) t
where is_key is null and column_default is null
and t.table_name in ('dv_owner','dv_hub','dv_release','dv_source_system','dv_source_table','dv_stage_table'
,'dv_satellite'
)