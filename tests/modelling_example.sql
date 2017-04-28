SET SEARCH_PATH TO ore_config;

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
  state_v varchar;
BEGIN
  -- check if owner exists
  SELECT owner_key
  INTO owner_key_v
  FROM dv_owner ow
  WHERE owner_name = owner_name_in;

  raise notice 'Owner_key-->%',owner_key_v;

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
                                   object_v) into state_v;



     SELECT owner_key
  INTO owner_key_v
  FROM dv_owner ow
  WHERE owner_name = owner_name_in;

    raise notice 'Added owner -->%',owner_key_v;
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

     raise notice 'Adding release -->%',release_key_v;
    SELECT dv_config_object_insert('dv_release',
                                   object_v) into state_v;
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
                                   object_v) into state_v;

    EXECUTE 'select ' || r.object_type || '_key from dv_' || r.object_type || ' where ' || r.object_type || '_schema='''
            || r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO object_key_v;

    -- looping through dependamt objects
    FOR rd IN (SELECT *
               FROM dv_model_L1_design
               WHERE is_parent IS NULL AND r.object_relationship = object_relationship)
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
                                     object_v) into state_v;

    END LOOP;

  END LOOP;


END
$BODY$
LANGUAGE plpgsql;



SELECT dv_model_l1_load_design('moj', 'Ministry of Justice',
                               2017042801,
                               'Test of modeller');


SELECT dv_model_l2_load_contents(2017042801);
SELECT dv_model_l3_load_mappings(2017042801);
SELECT dv_model_l4_load_logic(2017042801);