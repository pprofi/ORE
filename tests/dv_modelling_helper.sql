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
CREATE OR REPLACE FUNCTION dv_model_l1_load_design()
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
CREATE OR REPLACE FUNCTION dv_model_l2_load_contents()
  RETURNS VOID AS
$BODY$
DECLARE
  r                RECORD;
  owner_key_v      INT;
  release_number_v INT;
  release_key_v INT;
  object_key_v     INT;
  object_v         VARCHAR [] [];
  suffix_v         VARCHAR;
BEGIN
  -- loop through all columns

  FOR r IN (SELECT *
            FROM dv_model_L2_contents) LOOP

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [8, 2]);

    EXECUTE 'select release_number, owner_key, ' || r.object_type || '_key from dv_' || r.object_type || ' where ' ||
            r.object_type || '_schema=''' ||
            r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO release_key_v, owner_key_v, object_key_v;

    select release_number
    into release_number_v
    from dv_release
      where release_key=release_key_v;

    suffix_v :=CASE WHEN r.object_type = 'hub'
      THEN 'hub_key_'
               ELSE '' END;

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_v;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:=r.object_type || '_key';
    object_v [3] [1]:=object_key_v;

    object_v [4] [1]:= suffix_v || 'column_name';
    object_v [4] [2]:= r.column_name;
    object_v [5] [1]:=suffix_v || 'column_type';
    object_v [5] [2]:= r.column_type;
    object_v [6] [1]:=suffix_v || 'column_length';
    object_v [6] [2]:= r.column_length;
    object_v [7] [1]:=suffix_v || 'column_precision';
    object_v [7] [2]:= r.column_precision;
    object_v [8] [1]:=suffix_v || 'column_scale';
    object_v [8] [2]:= r.column_scale;

    SELECT dv_config_object_insert('dv_' || CASE WHEN r.object_type = 'hub'
      THEN suffix_v
                                            ELSE r.object_type || '_' END || 'column',
                                   object_v);


  END LOOP;
END
$BODY$
LANGUAGE plpgsql;



-- phase 3
-- file 3 add mappings
CREATE OR REPLACE FUNCTION dv_model_l3_load_mappings()
  RETURNS VOID AS
$BODY$
DECLARE
  r                RECORD;
  owner_key_v      INT;
  release_number_v INT;
  release_key_v    INT;
  object_key_v     INT;
  object_v         VARCHAR [] [];
  column_key_v     INT;
BEGIN

  FOR r IN (SELECT *
            FROM dv_model_L3_mapping) LOOP

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

    -- stage table column key
    SELECT
      column_key,
      c.release_key,
      c.owner_key
    INTO column_key_v, release_key_v, owner_key_v
    FROM dv_stage_table st
      JOIN dv_stage_table_column c ON st.stage_table_key = c.stage_table_key
    WHERE stage_table_schema = r.object_schema_out AND st.stage_table_name = r.object_name_out AND
          c.column_name = r.coulumn_name_out;

    -- release number
    SELECT release_number
    INTO release_number_v
    FROM dv_release
    WHERE release_key = release_key_v;

    -- find mapping object key
    IF r.mapping_type = 'hub'
    THEN

      SELECT hub_key_column_key
      INTO object_key_v
      FROM dv_hub_key_column hkc
        JOIN dv_hub h ON h.hub_key = hkc.hub_key
      WHERE h.hub_name = r.onject_name_in AND h.hub_schema = r.object_schema_in
      and hkc.hub_key_column_name=r.column_name_in;

      ELSE
      SELECT satellite_key
      INTO object_key_v
      FROM dv_satellite
      WHERE satellite_name = r.onject_name_in AND satellite_schema = r.object_schema_in;
    END IF;

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_v;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:= CASE WHEN r.mapping_type = 'hub'
      THEN 'hub_key_column_key'
                       ELSE 'satellite_key' END;
    object_v [3] [1]:=object_key_v;
    object_v [4] [1]:='column_key';
    object_v [4] [2]:= column_key_v;

   -- add data to config
    SELECT dv_config_object_insert('dv_' || r.object_type || '_column',
                                   object_v);

  END LOOP;

END
$BODY$
LANGUAGE plpgsql;

-- phase 4
-- add schedules (names)

-- phase 5
-- add business rules and schedule-tasks
-- generate additional rules for stage and source update statuses and

create table dv_model_L4_logic
(
 schedule_name varchar,
 object_type varchar,
 source_name varchar,
 source_schema varchar,
 source_load_type varchar,
 business_rule_name varchar,
 business_rule_logic text,
 business_rule_load_type varchar,
 business_rule_type varchar ,
 rn_order int
);

-- br type - block of code, procedure

SELECT dv_config_object_insert('dv_business_rule',
                               E'{{"business_rule_name","performance_disk_usage_cleanup_stage"},{"stage_table_key","4"},
                               {"business_rule_logic","select ore_config.fn_source_cleanup(\\"moj_osa_stage\\",\\"performance_disk_usage\\");"},
                               {"business_rule_type","procedure"},
                               {"load_type","delta"},
                               {"release_number","20170316"},{"owner_key","2"}}');
SELECT dv_config_object_insert('dv_schedule',
                               '{{"schedule_name","load_metadata_file"},{"schedule_description","load_metadata_file"},
                                {"release_number","20170316"},{"owner_key","2"}}');
select dv_config_object_insert('dv_schedule_task',
                               '{{"schedule_key","1"},{"object_key","2"},
                                {"object_type","source"},{"load_type","delta"},
                                {"release_number","20170316"},{"owner_key","2"}}');
select dv_config_object_insert('dv_schedule_task_hierarchy',
                               '{{"schedule_task_key","1"},{"schedule_parent_task_key",""},
                                 {"release_number","20170316"},{"owner_key","2"}}');

-- for source - > object key from source_tables

load_metadata_file
task1
NO_parent
object_key = source_table_key
object_type=source
delta
load script


CREATE OR REPLACE FUNCTION dv_model_l4_load_logic(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r                RECORD;
  rh               RECORD;
  owner_key_v      INT;
  release_number_v INT;
  release_key_v    INT;
  object_key_v     INT;
  object_v         VARCHAR [] [];
  object2_key_v    INT;
  object3_key_v    INT;
BEGIN

  -- check if release exists
  -- find owner_key

  SELECT owner_key
  INTO owner_key_v
  FROM dv_release
  WHERE release_number = release_number_in;

  IF owner_key_v IS NULL
  THEN
    RETURN;
  END IF;

  FOR r IN (SELECT DISTINCT schedule_name AS schedule_name
            FROM dv_model_L4_logic) LOOP

    -- add schedule - 1 schedule per one - source
    -- add schedule tasks related to source load
    -- add business rules related to tasks
    -- add hierarchy of tasks
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:= 'schedule_name';
    object_v [3] [2]:= r.schedule_name;
    object_v [4] [1]:= 'schedule_description';
    object_v [4] [2]:= r.schedule_name;

    SELECT dv_config_object_insert('dv_schedule',
                                   object_v);

    -- find just inserted object_key
    SELECT schedule_key
    INTO object_key_v
    FROM dv_schedule
    WHERE schedule_name = r.schedule_name AND owner_key = owner_key_v;

    -- loop through related tasks and business rules
    FOR rh IN (SELECT *
               FROM dv_model_L4_logic
               WHERE schedule_name = r.schedule_name
               ORDER BY rn_order ASC
    ) LOOP

      object2_key_v:=NULL;
      object3_key_v:=NULL;

      -- source

      IF rh.object_type = 'source'
      THEN

        SELECT source_table_key
        INTO object2_key_v
        FROM dv_source_table
        WHERE source_table_name = rh.source_name
              AND source_table_schema = rh.source_schema
              AND owner_key = owner_key_v;

      ELSE
        -- find stage table key

        SELECT stage_table_key
        INTO object2_key_v
        FROM dv_stage_table
        WHERE stage_table_name = rh.source_name
              AND stage_table_schema = rh.source_schema
              AND owner_key = owner_key_v;
      END IF;

      -- if it is business rule - add first
      IF rh.business_rule_name IS NOT NULL
      THEN


        object_v:=array_fill(NULL :: VARCHAR, ARRAY [8, 2]);
        object_v [1] [1]:='release_number';
        object_v [1] [2]:=release_number_in;
        object_v [2] [1]:='owner_key';
        object_v [2] [2]:= owner_key_v;

        object_v [3] [1]:= 'business_rule_name';
        object_v [3] [2]:= rh.business_rule_name;

        object_v [4] [1]:= 'business_rule_logic';
        object_v [4] [2]:= rh.business_rule_logic;

        object_v [5] [1]:= 'stage_table_key';
        object_v [5] [2]:= object2_key_v;
        object_v [6] [1]:= 'business_rule_type';
        object_v [6] [2]:= rh.business_rule_type;

        object_v [7] [1]:= 'load_type';
        object_v [7] [2]:= rh.business_rule_load_type;

        -- add business rule
        SELECT dv_config_object_insert('dv_business_rule', object_v);

        SELECT business_rule_key
        INTO object3_key_v
        FROM dv_business_rule
        WHERE
          owner_key = owner_key_v AND business_rule_name = rh.business_rule_name AND stage_table_key = object2_key_v;

      END IF;


      object_v:=array_fill(NULL :: VARCHAR, ARRAY [6, 2]);

      object_v [1] [1]:='release_number';
      object_v [1] [2]:=release_number_in;
      object_v [2] [1]:='owner_key';
      object_v [2] [2]:= owner_key_v;

      object_v [3] [1]:= 'schedule_key';
      object_v [3] [2]:= object_key_v;
      object_v [4] [1]:='object_key';
      object_v [4] [2]:= coalesce(object3_key_v, object2_key_v);
      object_v [5] [1]:= 'object_type';
      object_v [5] [2]:= rh.object_type;
      object_v [6] [1]:= 'load_type';
      object_v [6] [2]:= rh.source_load_type;

      -- add schedule task
      SELECT dv_config_object_insert('dv_schedule_task', object_v);

      -- add schedule_hierarchy
      -- should be sorted by rn_order
      -- source always has no parent

    END LOOP;


  END LOOP;

END
$BODY$
LANGUAGE plpgsql;
