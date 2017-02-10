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
  delimiter_v        CHAR(10) = ' , ';
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
          replace(c.data_type,'character varying','varchar')
        INTO column_name_v, column_data_type_v
        FROM information_schema.tables t
          JOIN information_schema.columns c
            ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE t.table_schema = 'ore_config' AND t.table_name = object_type_in
              AND c.column_name = object_settings_in [i] [1]
              AND c.column_name NOT IN ('updated_by', 'updated_datetime', key_column_v,'release_key');
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
                 || column_data_type_v
                 || ')';
        END IF;
      END LOOP;

      IF counter_v > 0
      THEN
        sql_v:=sql_v || ' where '
               || quote_ident(key_column_v)
               || '='
               || quote_literal(cast(object_key_in as int));

        RAISE NOTICE 'SQL --> %', sql_v;

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




select dv_config_object_update('dv_release_1',2,'{{"hub_key","2"},{"release_key","3"}}');

select '{{"hub_key","2"},{"release_key","3"}}';

-- case 2 correct parameters
select dv_config_object_update('dv_release',2,'{{"release_description","update test case 2"}}');


-- case 3 one not correct and rest correct
select dv_config_object_update('dv_release',2,
                               '{{"release_description","update test case 3"},{"release_key","3"},{"version_number","2"}}');

-- case 4 not found object to update - incorrect
select dv_config_object_update('dv_release',3,'{{"hub_key","2"},{"release_key","3"}}');

-- case 5 nothing to update

select dv_config_object_update('dv_release',3,NULL);


----------------


-- insert object details

CREATE OR REPLACE FUNCTION dv_config_object_insert
  (
    object_type_in     VARCHAR(100), -- table name in a list of
    object_settings_in VARCHAR [] [2]-- array parameters to insert column_name -> value
  )
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v     INTEGER :=0;
  column_name_v  VARCHAR(50);
  column_value_v varchar;
  column_type_v  VARCHAR(30);
  release_key_v  INT;
  owner_key_v    INT;
  sql_v          VARCHAR(2000);
  sql_select_v   VARCHAR(2000);
  array_length_v INT;
  counter_v      INT;
  delimiter_v    CHAR(10) = ' , ';
BEGIN

  rowcount_v:=0;
  -- check if object exists
  SELECT count(*)
  INTO rowcount_v
  FROM information_schema.tables t
  WHERE t.table_schema = 'ore_config' AND t.table_name = object_type_in;

  IF rowcount_v = 0
  THEN
    RAISE NOTICE 'Not valid object type --> %', object_type_in;
    RETURN rowcount_v;
  END IF;

  -- if there any columns to update
  array_length_v:=array_length(object_settings_in, 1);

  IF array_length_v = 0
  THEN
    RAISE NOTICE 'Nothing to update, check parameters --> %', object_settings_in;
    RETURN array_length_v;
  END IF;

  -- return list of columns except from key column and audit columns
  -- also fine if column omitted and nullable
  -- need checking release and owner integrity
  CREATE TEMP TABLE columns_list_tmp ON COMMIT DROP AS
    SELECT
      column_name,
      data_type,
      is_nullable,
      is_found,
      column_default
    FROM
      (
        SELECT
          c.column_name,
          c.is_nullable,
          replace(c.data_type, 'character varying', 'varchar') AS data_type,
          CASE WHEN c.column_name = kcu.column_name
            THEN 1
          ELSE NULL END                                        AS f,
         cast( 0 as integer)                                                   AS is_found,
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
        WHERE t.table_schema = 'ore_config' AND t.table_name = object_type_in
              AND c.column_name NOT IN ('updated_by', 'updated_datetime')) r
    WHERE f IS NULL;

  -- insert statement
  sql_v:='insert into '
         || ' '
         || quote_ident(object_type_in)
         || ' ( ';

  sql_select_v:=' select ';

  -- check parameters
  -- in case some incorrect parameters
  counter_v:=0;
  -- columns to lookup in information schema
  FOR i IN 1..array_length_v LOOP

  -- checking release_number and owner_key existance
    -- excluded for dv_release

    IF object_settings_in [i] [1] = 'release_number' and object_type_in not in('dv_release')
    THEN
      SELECT release_key
      INTO release_key_v
      FROM dv_release
      WHERE release_number = cast (object_settings_in [i] [2] as integer);

      GET DIAGNOSTICS rowcount_v = ROW_COUNT;

    IF rowcount_v = 0
    THEN
      RAISE NOTICE 'Nonexistent release_number --> %', object_settings_in [i] [2];
      RETURN rowcount_v = 0;
    END IF;
    END IF;



    IF object_settings_in [i] [1] = 'owner_key' and object_type_in not in ('dv_release','dv_owner','dv_default_column')
    THEN
      SELECT owner_key
      INTO owner_key_v
      FROM dv_owner
      WHERE owner_key = cast(object_settings_in [i] [2] as integer);

    GET DIAGNOSTICS rowcount_v = ROW_COUNT;

    IF rowcount_v = 0
    THEN
      RAISE NOTICE 'Nonexistent owner_key --> %', object_settings_in [i] [2];
      RETURN rowcount_v = 0;
    END IF;
    END IF;


    -- lookup columns in temp table
    SELECT
      column_name,
      data_type,
      case when column_name='release_key' then cast(release_key_v as varchar) else object_settings_in [i] [2] end
    INTO column_name_v, column_type_v, column_value_v
    FROM columns_list_tmp
    WHERE column_name = replace(object_settings_in [i] [1],'release_number','release_key');

     RAISE NOTICE 'SQL --> %', column_name_v||';'|| column_type_v||';'||column_value_v;

    GET DIAGNOSTICS rowcount_v = ROW_COUNT;

    IF rowcount_v > 0
    THEN
      counter_v:=counter_v + 1;

      IF counter_v > 1
      THEN
        sql_v:=sql_v || delimiter_v;
        sql_select_v:=sql_select_v || delimiter_v;
      END IF;

      RAISE NOTICE 'SQL --> %', sql_v;
      RAISE NOTICE 'SQL --> %', sql_select_v;

      -- sql list of columns
      sql_v:=sql_v || quote_ident(column_name_v);
      -- sql list of values
      sql_select_v:=sql_select_v
                    ||' cast('
                    || quote_literal(column_value_v)
                    || ' as '
                    || column_type_v
                    || ')';

       RAISE NOTICE 'SQL --> %', sql_v;
      -- update if column was found
      UPDATE columns_list_tmp
      SET is_found = 1
      WHERE column_name = column_name_v;

       RAISE NOTICE 'SQL --> %', sql_v;
    END IF;

  END LOOP;

  counter_v:=0;
  -- check number of parameters
  SELECT count(*)
  INTO counter_v
  FROM columns_list_tmp
  WHERE (is_nullable = 'NO' or column_default is not null) AND is_found = 0;

  IF counter_v > 0
  THEN
    RAISE NOTICE 'Not all parameters found --> %', object_settings_in;
    RETURN 0;
  ELSE

    sql_v:=sql_v || ') ' || sql_select_v;


    EXECUTE sql_v;
    RAISE NOTICE 'SQL --> %', sql_v;
  END IF;

  -- check if something actually been deleted
  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  RETURN rowcount_v;

END
$BODY$
LANGUAGE plpgsql;

-- test case 1 no parameters
select dv_config_object_insert('dv_release','{{"hub_key","2"},{"release_key","3"}}');

-- test case 2 some incorrect parameters


-- test 3 incorrect object (+)
select dv_config_object_insert('dv_release_x','{{"release_description","Bla bla"},{"version_number","3"}}');

-- test 4 all good
select dv_config_object_insert('dv_release','{{"release_description","Bla bla"},{"version_number","3"},{"release_number","20170210"}}');
select dv_config_object_insert('dv_owner','{{"owner_name","Bla bla"},{"owner_description","test tes"},{"release_number","20170210"},{"version_number","1"},{"is_retired","0"}}');
select dv_config_object_insert('dv_hub','{{"hub_name","Bla bla"},{"hub_schema","test"},{"release_number","20170210"},{"owner_key","1"}}');
-- test 5 release or owner not correct

-- test 6 not enough not nullable parameters
select dv_config_object_insert('dv_release','{{"release_description","Bla bla"},{"version_number","3"}}');

select * from dv_release

CREATE TABLE dv_hub
(
    hub_key INTEGER DEFAULT nextval('dv_hub_hub_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_name VARCHAR(128) NOT NULL,
    hub_abbreviation VARCHAR(4),
    hub_schema VARCHAR(128) NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_hub_dv_release_master FOREIGN KEY (release_key) REFERENCES dv_release_master (release_key),
    CONSTRAINT fk_dv_hub_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_abr_unq ON dv_hub (owner_key, hub_abbreviation);
CREATE UNIQUE INDEX dv_hub_unq ON dv_hub (owner_key, hub_schema, hub_name);