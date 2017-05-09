SET SEARCH_PATH TO ore_config;
/*
CREATE OR REPLACE FUNCTION dv_model_l1_load_design(owner_name_in     VARCHAR, owner_desc_in VARCHAR,
                                                   release_number_in INT,
                                                   release_desc_in   VARCHAR)
  RETURNS VOID AS
$BODY$
DECLARE
  release_key_v INT;
  owner_key_v   INT;
  object_v      VARCHAR [] [];
  r             RECORD;
  rd            RECORD;
  object_key_v  INT;
  state_v       VARCHAR;
BEGIN
  -- check if owner exists
  SELECT owner_key
  INTO owner_key_v
  FROM dv_owner ow
  WHERE owner_name = owner_name_in;

  RAISE NOTICE 'Owner_key-->%', owner_key_v;

  -- new owner
  IF owner_key_v IS NULL
  THEN
    -- add owner
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [2, 2]);
    object_v [1] [1]:='owner_name';
    object_v [1] [2]:=owner_name_in;
    object_v [2] [1]:='owner_description';
    object_v [2] [2]:=owner_desc_in;


    SELECT dv_config_object_insert('dv_owner',
                                   object_v)
    INTO state_v;


    SELECT owner_key
    INTO owner_key_v
    FROM dv_owner ow
    WHERE owner_name = owner_name_in;

    RAISE NOTICE 'Added owner -->%', owner_key_v;
  END IF;

  -- release
  SELECT release_key
  INTO release_key_v
  FROM dv_release
  WHERE release_number = release_number_in;

  -- new release
  IF release_key_v IS NULL
  THEN
    -- add release
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [3, 2]);
    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='release_description';
    object_v [2] [2]:=release_desc_in;
    object_v [3] [1]:='owner_key';
    object_v [3] [2]:= owner_key_v;

    RAISE NOTICE 'Adding release -->%', release_key_v;
    SELECT dv_config_object_insert('dv_release',
                                   object_v)
    INTO state_v;
  END IF;

  -- go through the rest and add DV objects into config
  -- source_systems


  object_v:=array_fill(NULL :: VARCHAR, ARRAY [5, 2]);
  object_v [1] [1]:='release_number';
  object_v [1] [2]:=release_number_in;
  object_v [2] [1]:='owner_key';
  object_v [2] [2]:= owner_key_v;

  FOR r IN (SELECT *
            FROM dv_model_L1_design
            WHERE object_type IN ('source_system', 'hub')) LOOP

    object_key_v:=null;

    object_v [3] [1]:=r.object_type || '_schema';
    object_v [3] [2]:=r.object_schema;
    object_v [4] [1]:=r.object_type || '_name';
    object_v [4] [2]:=r.object_name;

    SELECT dv_config_object_insert('dv_' || r.object_type,
                                   object_v)
    INTO state_v;

    EXECUTE 'select ' || r.object_type || '_key from dv_' || r.object_type || ' where ' || r.object_type || '_schema='''
            || r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO object_key_v;

    RAISE NOTICE 'Object_key inserted -->%', object_key_v;

    -- looping through dependamt objects
    FOR rd IN (SELECT *
               FROM dv_model_L1_design
               WHERE is_parent <> 1 AND r.object_relationship = object_relationship)
    LOOP
      object_v [3] [1]:=rd.object_type || '_schema';
      object_v [3] [2]:=rd.object_schema;
      object_v [4] [1]:=rd.object_type || '_name';
      object_v [4] [2]:=rd.object_name;

      object_v [5] [1]:= (CASE WHEN r.object_type = 'source_system'
        THEN 'system'
                          ELSE r.object_type END) || '_key';
      object_v [5] [2]:=object_key_v;

      -- add object to config
      SELECT dv_config_object_insert('dv_' || rd.object_type,
                                     object_v)
      INTO state_v;

    END LOOP;

  END LOOP;


END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dv_model_l2_load_contents(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r            RECORD;
  owner_key_v  INT;
  object_key_v INT;
  object_v     VARCHAR [] [];
  suffix_v     VARCHAR;
  state_v      VARCHAR;
BEGIN
  -- loop through all columns
  FOR r IN (SELECT *
            FROM dv_model_L2_contents) LOOP
    object_key_v:=NULL;

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [8, 2]);

    RAISE NOTICE 'Starting -->%', object_key_v;
    -- get keys of parent objects
    EXECUTE 'select owner_key, ' || r.object_type || '_key from dv_' || r.object_type || ' where ' ||
            r.object_type || '_schema=''' ||
            r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO owner_key_v, object_key_v;


    RAISE NOTICE 'Object to link to -->%', object_key_v;

    suffix_v :=CASE WHEN r.object_type = 'hub'
      THEN 'hub_key_'
               ELSE '' END;

    RAISE NOTICE 'Object to link to -->%', suffix_v;

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:=r.object_type || '_key';
    object_v [3] [2]:=object_key_v;

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

    -- add object into config
    SELECT dv_config_object_insert('dv_' || CASE WHEN r.object_type = 'hub'
      THEN suffix_v
                                            ELSE r.object_type || '_' END || 'column',
                                   object_v)
    INTO state_v;


  END LOOP;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dv_model_l3_load_mappings(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r            RECORD;
  owner_key_v  INT;
  object_key_v INT;
  object_v     VARCHAR [] [];
  column_key_v INT;
  state_v      VARCHAR;
BEGIN

  -- loop through columns to map
  FOR r IN (SELECT *
            FROM dv_model_L3_mapping) LOOP

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

    object_key_v:=NULL;

    -- stage table column key
    SELECT
      column_key,
      c.owner_key
    INTO column_key_v, owner_key_v
    FROM dv_stage_table st
      JOIN dv_stage_table_column c ON st.stage_table_key = c.stage_table_key
    WHERE stage_table_schema = r.object_schema_out AND st.stage_table_name = r.object_name_out AND
          c.column_name = r.column_name_out;

    RAISE NOTICE 'Column key -->%', column_key_v;

    -- find mapping object key
    IF r.mapping_type = 'hub'
    THEN

      SELECT hub_key_column_key
      INTO object_key_v
      FROM dv_hub_key_column hkc
        JOIN dv_hub h ON h.hub_key = hkc.hub_key
      WHERE h.hub_name = r.object_name_in AND h.hub_schema = r.object_schema_in
            AND hkc.hub_key_column_name = r.column_name_in;

    ELSE
      -- mapping for satellites
      SELECT satellite_key
      INTO object_key_v
      FROM dv_satellite
      WHERE satellite_name = r.object_name_in AND satellite_schema = r.object_schema_in;
    END IF;

    RAISE NOTICE 'Object key -->%', object_key_v;

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:= CASE WHEN r.mapping_type = 'hub'
      THEN 'hub_key_column_key'
                       ELSE 'satellite_key' END;
    object_v [3] [2]:=object_key_v;
    object_v [4] [1]:='column_key';
    object_v [4] [2]:= column_key_v;

    -- add data to config
    SELECT dv_config_object_insert('dv_' || r.mapping_type || '_column',
                                   object_v)
    INTO state_v;

  END LOOP;

END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dv_model_l4_load_logic(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r                 RECORD;
  rh                RECORD;
  owner_key_v       INT;
  object_key_v      INT;
  object_v          VARCHAR [] [];
  object2_key_v     INT;
  object3_key_v     INT;
  task_key_v        INT;
  parent_task_key_v INT;
  state_v           VARCHAR;
BEGIN

  -- check if release exists
  -- find owner_key

  SELECT owner_key
  INTO owner_key_v
  FROM dv_release
  WHERE release_number = release_number_in;

  RAISE NOTICE 'Owner -->%', owner_key_v;

  IF owner_key_v IS NULL
  THEN
    RETURN;
  END IF;

  FOR r IN (SELECT DISTINCT schedule_name AS schedule_name
            FROM dv_model_L4_logic) LOOP

    parent_task_key_v:=NULL;

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
                                   object_v)
    INTO state_v;

    -- find just inserted object_key
    SELECT schedule_key
    INTO object_key_v
    FROM dv_schedule
    WHERE schedule_name = r.schedule_name AND owner_key = owner_key_v;

    RAISE NOTICE 'Schedule key -->%', object_key_v;

    -- loop through related tasks and business rules
    FOR rh IN (SELECT *
               FROM dv_model_L4_logic
               WHERE schedule_name = r.schedule_name
               ORDER BY rn_order ASC
    ) LOOP

      object2_key_v:=NULL;
      object3_key_v:=NULL;

      -- load source task - find table key

      IF rh.object_type = 'source' or rh.is_stage=0
      THEN

        SELECT source_table_key
        INTO object2_key_v
        FROM dv_source_table
        WHERE source_table_name = rh.source_name
              AND source_table_schema = rh.source_schema
              AND owner_key = owner_key_v;

      ELSE
        -- find stage table key for other tasks
        SELECT stage_table_key
        INTO object2_key_v
        FROM dv_stage_table
        WHERE stage_table_name = rh.source_name
              AND stage_table_schema = rh.source_schema
              AND owner_key = owner_key_v;
      END IF;

      RAISE NOTICE 'Object key 2 source or stage tables -->%', object2_key_v;
      -- configure and add business rules
      IF rh.object_type like '%business_rule%'
      --rh.business_rule_name in ('')
      THEN

        object_v:=array_fill(NULL :: VARCHAR, ARRAY [7, 2]);

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
        SELECT dv_config_object_insert('dv_business_rule', object_v)
        INTO state_v;

        -- find newly added key
        SELECT business_rule_key
        INTO object3_key_v
        FROM dv_business_rule
        WHERE
          owner_key = owner_key_v AND business_rule_name = rh.business_rule_name AND stage_table_key = object2_key_v;

        RAISE NOTICE 'Business rule key -->%', object3_key_v;

      END IF;

      -- configure schedule task
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
      SELECT dv_config_object_insert('dv_schedule_task', object_v)
      INTO state_v;

      -- find newly added key to use in hierarchy
      SELECT schedule_task_key
      INTO task_key_v
      FROM dv_schedule_task
      WHERE owner_key = owner_key_v AND object_key = coalesce(object3_key_v, object2_key_v)
            AND object_type = rh.object_type;

      RAISE NOTICE 'Task key -->%', task_key_v;

      -- add schedule_hierarchy
      -- should be sorted by rn_order
      -- source always has no parent

      IF rh.object_type = 'source'
      THEN
        parent_task_key_v:=NULL;
      END IF;

      -- schedule task hierarchy configuration
      object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

      object_v [1] [1]:='release_number';
      object_v [1] [2]:=release_number_in;
      object_v [2] [1]:='owner_key';
      object_v [2] [2]:= owner_key_v;

      object_v [3] [1]:= 'schedule_task_key';
      object_v [3] [2]:= task_key_v;
      object_v [4] [1]:='schedule_parent_task_key';
      object_v [4] [2]:= parent_task_key_v;

      RAISE NOTICE 'Hierarchy -->%', object_v;

      SELECT dv_config_object_insert('dv_schedule_task_hierarchy', object_v)
      INTO state_v;

      RAISE NOTICE 'Inserted data into task hierarchy ...';
      -- save for a use for adding next task
      parent_task_key_v:=task_key_v;
    END LOOP;


  END LOOP;

END
$BODY$
LANGUAGE plpgsql;
*/


SELECT dv_model_l1_load_design('moj', 'Ministry of Justice',
                               2017042801,
                               'Test of modeller');

SELECT dv_model_l2_load_contents(2017042801);

SELECT dv_model_l3_load_mappings(2017042801);

SELECT dv_model_l4_load_logic(2017042801);

select * from dv_schedule_task_hierarchy