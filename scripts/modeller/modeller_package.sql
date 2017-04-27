-- modelling helper script
-- individual modelling
-- owner - release - source system - source table - stage table - hub - satellite
-- business rules
-- schedule, schedule tasks, schedule hierarchy

SET SEARCH_PATH TO ore_config;

-- 1 model high level
-- 2 model columns
-- 3 link columns to source

CREATE TABLE dv_model_L1_design
(
  object_type         VARCHAR,
  object_schema       VARCHAR,
  object_name         VARCHAR,
  object_relationship VARCHAR,
  is_parent           INT
);

CREATE TABLE dv_model_L2_contents
(
  object_type      VARCHAR,
  object_name      VARCHAR,
  object_schema    VARCHAR,
  column_name      VARCHAR,
  column_type      VARCHAR,
  column_length    INT,
  column_precision INT,
  column_scale     INT
);

CREATE TABLE dv_model_L3_mapping
(
  mapping_type      VARCHAR,
  object_name_in    VARCHAR,
  object_schema_in  VARCHAR,
  column_name_in    VARCHAR,
  object_name_out   VARCHAR,
  object_schema_out VARCHAR,
  column_name_out   VARCHAR
);

CREATE TABLE dv_model_L4_logic
(
  schedule_name           VARCHAR,
  object_type             VARCHAR,
  source_name             VARCHAR,
  source_schema           VARCHAR,
  source_load_type        VARCHAR,
  business_rule_name      VARCHAR,
  business_rule_logic     TEXT,
  business_rule_load_type VARCHAR,
  business_rule_type      VARCHAR,
  rn_order                INT
);


-- model - design
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
BEGIN
  -- check if owner exists
  SELECT owner_key
  INTO owner_key_v
  FROM dv_owner ow
  WHERE owner_name = owner_name_in;

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
                                   object_v);
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
    SELECT dv_config_object_insert('dv_release',
                                   object_v);
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

      -- add object to config
      SELECT dv_config_object_insert('dv_' || r.object_type,
                                     object_v);

    END LOOP;

  END LOOP;


END
$BODY$
LANGUAGE plpgsql;


--  model contents
CREATE OR REPLACE FUNCTION dv_model_l2_load_contents(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r            RECORD;
  owner_key_v  INT;
  object_key_v INT;
  object_v     VARCHAR [] [];
  suffix_v     VARCHAR;
BEGIN
  -- loop through all columns
  FOR r IN (SELECT *
            FROM dv_model_L2_contents) LOOP

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [8, 2]);
    -- get keys of parent objects
    EXECUTE 'select owner_key, ' || r.object_type || '_key from dv_' || r.object_type || ' where ' ||
            r.object_type || '_schema=''' ||
            r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO owner_key_v, object_key_v;


    suffix_v :=CASE WHEN r.object_type = 'hub'
      THEN 'hub_key_'
               ELSE '' END;

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
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

    -- add object into config
    SELECT dv_config_object_insert('dv_' || CASE WHEN r.object_type = 'hub'
      THEN suffix_v
                                            ELSE r.object_type || '_' END || 'column',
                                   object_v);


  END LOOP;
END
$BODY$
LANGUAGE plpgsql;

-- model mappings
CREATE OR REPLACE FUNCTION dv_model_l3_load_mappings(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r            RECORD;
  owner_key_v  INT;
  object_key_v INT;
  object_v     VARCHAR [] [];
  column_key_v INT;
BEGIN

  -- loop through columns to map
  FOR r IN (SELECT *
            FROM dv_model_L3_mapping) LOOP

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

    -- stage table column key
    SELECT
      column_key,
      c.owner_key
    INTO column_key_v, owner_key_v
    FROM dv_stage_table st
      JOIN dv_stage_table_column c ON st.stage_table_key = c.stage_table_key
    WHERE stage_table_schema = r.object_schema_out AND st.stage_table_name = r.object_name_out AND
          c.column_name = r.coulumn_name_out;

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

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
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

-- loading tasks and business rules
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

      -- load source task - find table key

      IF rh.object_type = 'source'
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

      -- configure and add business rules
      IF rh.business_rule_name IS NOT NULL
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
        SELECT dv_config_object_insert('dv_business_rule', object_v);

        -- find newly added key
        SELECT business_rule_key
        INTO object3_key_v
        FROM dv_business_rule
        WHERE
          owner_key = owner_key_v AND business_rule_name = rh.business_rule_name AND stage_table_key = object2_key_v;

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
      SELECT dv_config_object_insert('dv_schedule_task', object_v);

      -- find newly added key to use in hierarchy
      SELECT schedule_task_key
      INTO task_key_v
      FROM dv_schedule_task
      WHERE owner_key = owner_key_v AND object_key = coalesce(object3_key_v, object2_key_v)
            AND object_type = rh.object_type;

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

      SELECT dv_config_object_insert('dv_schedule_task_hierarchy', object_v);

      -- save for a use for adding next task
      parent_task_key_v:=task_key_v;
    END LOOP;


  END LOOP;

END
$BODY$
LANGUAGE plpgsql;


-- modeller
CREATE OR REPLACE FUNCTION dv_modeller(owner_name_in     VARCHAR, owner_desc_in VARCHAR,
                                               release_number_in INT,
                                               release_desc_in   VARCHAR)
  RETURNS VOID AS
$BODY$
BEGIN

  SELECT dv_model_l1_load_design(owner_name_in, owner_desc_in,
                                 release_number_in,
                                 release_desc_in);
  SELECT dv_model_l2_load_contents(release_number_in);
  SELECT dv_model_l3_load_mappings(release_number_in);
  SELECT dv_model_l4_load_logic(release_number_in);


END
$BODY$
LANGUAGE plpgsql;