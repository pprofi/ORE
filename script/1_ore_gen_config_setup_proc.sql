SET search_path TO ore_config;

CREATE SEQUENCE dv_config_object_list_key_seq START 1;

drop table dv_config_object_list;

CREATE TABLE dv_config_object_list
(
  object_key  INTEGER DEFAULT nextval('dv_config_object_list_key_seq'::regclass) PRIMARY KEY NOT NULL,
  object_name VARCHAR(50),
  table_name  VARCHAR(100)
);
truncate dv_config_object_list;

INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('release', 'dv_release');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('hub', 'dv_hub');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('satellite', 'dv_satellite');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('source_system', 'dv_source_system');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('source_table', 'dv_source_table');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('stage_table', 'dv_stage_table');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('stage_table_column', 'dv_stage_table_column');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('owner', 'dv_owner');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('hub_column', 'dv_hub_column');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('satellite_column', 'dv_satellite_column');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('hub_key_column', 'dv_hub_key_column');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('business_rule', 'dv_business_rule');
INSERT INTO dv_config_object_list (object_name, table_name)
VALUES ('default_column', 'dv_default_column');

select * from dv_config_object_list;
--- common procedure
-- only tables in a list
-- should be dynamically execute ddl statements
-- using parameters binding

CREATE OR REPLACE FUNCTION dv_config_object_delete
  (
    object_type_in VARCHAR(100), -- table name in a list of
    object_key_in  INTEGER
  )
RETURNS INT AS
$BODY$
DECLARE
  rowcount_v   INTEGER :=0;
  key_column_v VARCHAR(50);
  sql_v        VARCHAR(2000);
BEGIN

  -- check if table exists
  rowcount_v:=0;

  -- find primary key column and check if object exists
  SELECT kcu.column_name
  INTO key_column_v
  FROM INFORMATION_SCHEMA.TABLES t
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
  WHERE
    t.table_schema = 'ore_config' AND t.table_name = object_type_in;

  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  IF rowcount_v = 0
  THEN
    RAISE NOTICE 'Not valid object type --> %', object_type_in;
  ELSE
    -- delete record
  EXECUTE 'delete from '
          || ' '
          || quote_ident(object_type_in)
          || ' where '
          || quote_ident(key_column_v)
          || '='
          || quote_literal(object_key_in);
  END IF;

  -- check if something actually been deleted
  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  RETURN rowcount_v;

END
$BODY$
LANGUAGE plpgsql;



-- test --
INSERT INTO dv_release
(
  release_number,
  release_description,
  version_number)
VALUES (20170209, 'testing generic del func', 1);

select * from dv_release;

select dv_config_object_delete ('dv_release',1);

-- WOW working!

-- update object details

CREATE OR REPLACE FUNCTION dv_config_object_update
  (
    object_type_in     VARCHAR(100), -- table name in a list of
    object_key_in      INTEGER,
    object_settings_in VARCHAR [] [2]-- array parameters to update column_name -> value
  )
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v         INTEGER :=0;
  key_column_v       VARCHAR(50);
  column_name_v      VARCHAR(50);
  column_data_type_v VARCHAR(50);
  sql_v              VARCHAR(2000);
  array_length_v     INT;
  counter_v          INT;
  delimiter_v        CHAR = ' , ';
BEGIN

  rowcount_v:=0;

  -- find primary key column and check if object exists
  SELECT kcu.column_name
  INTO key_column_v
  FROM INFORMATION_SCHEMA.TABLES t
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
  WHERE
    t.table_schema = 'ore_config' AND t.table_name = object_type_in;

  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  IF rowcount_v = 0
  THEN
    RAISE NOTICE 'Not valid object type --> %', object_type_in;
  ELSE
    -- update statement
    sql_v:='update '
           || ' '
           || quote_ident(object_type_in)
           || ' set ';

    -- if there any columns to update
    array_length_v:=array_length(object_settings_in, 1);

    -- here should be cycle for all columns to be updated
    IF array_length_v = 0
    THEN
      RAISE NOTICE 'Nothing to update, check parameters --> %', object_settings_in;
    ELSE
      -- in case some incorrect parameters
      counter_v:=0;
      -- all columns to lookup in information schema
      FOR i IN 1..array_length_v LOOP

        SELECT
          c.column_name,
          c.data_type
        INTO column_name_v, column_data_type_v
        FROM information_schema.tables t
          JOIN information_schema.columns c
            ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE t.table_schema = 'ore_config' AND t.table_name = object_type_in
              AND c.column_name = object_settings_in [i] [1]
              AND c.column_name NOT IN ('updated_by', 'updated_datetime', key_column_v);
        GET DIAGNOSTICS rowcount_v = ROW_COUNT;

        IF rowcount_v > 0
        THEN
          counter_v:=counter_v + 1;
          IF counter_v > 1
          THEN
            sql_v:=sql_v || delimiter_v;
          END IF;

          sql_v:=sql_v || quote_ident(column_name_v)
                 || '='
                 || ' cast('
                 || quote_literal(object_settings_in [i] [2])
                 || ' as '
                 || quote_literal(column_data_type_v)
                 || ')';
        END IF;
      END LOOP;

      IF counter_v > 0
      THEN
        sql_v:=sql_v || ' where '
               || quote_ident(key_column_v)
               || '='
               || quote_literal(object_key_in);

        EXECUTE sql_v;
      ELSE
        RAISE NOTICE 'Nothing to update or parameters prohibited to update, check parameters --> %', object_settings_in;
      END IF;
    END IF;

  END IF;
  -- check if something actually been deleted
  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  RETURN rowcount_v;

END
$BODY$
LANGUAGE plpgsql;


------- TEST

INSERT INTO dv_release
(
  release_number,
  release_description,
  version_number)
VALUES (20170209, 'testing generic del func', 1);

select * from dv_release;

-- case 1 parameter, incorrect, not exists or one of forbidden

-- case 2 correct parameters

-- case 3 one not correct and rest correct

-- case 4 not found object to update - incorrect

-- case 5 not found key value 




----------------












CREATE OR REPLACE FUNCTION dv_config_object(
  operation_key_in       CHAR(1),
  object_type_in varchar(100), -- table name in a list of
  object_key_in INTEGER DEFAULT 0,


  is_retired_in          BOOLEAN DEFAULT FALSE,
  release_number_in      INT DEFAULT 0,
  owner_key_in           INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_business_rule
    WHERE business_rule_key = business_rule_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_business_rule
      SET stage_table_key   = stage_table_key_in,
        business_rule_name  = business_rule_name_in,
        business_rule_type  = business_rule_type_in,
        business_rule_logic = business_rule_logic_in,
        load_type           = load_type_in,
        is_external         = is_external_in,
        is_retired          = is_retired_in
      WHERE business_rule_key = business_rule_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE

        INSERT INTO dv_business_rule (stage_table_key,
                                      business_rule_name,
                                      business_rule_type,
                                      business_rule_logic,
                                      load_type,
                                      is_external,
                                      is_retired, release_key, owner_key)
          SELECT
            stage_table_key_in,
            business_rule_name_in,
            business_rule_type_in,
            business_rule_logic_in,
            load_type_in,
            is_external_in,
            is_retired_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;
