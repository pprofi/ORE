/*-------------------------------------------
        OPTIMAL REPORTING ENGINE
        CONFIG SETUP SCRIPTS
-------------------------------------------*/

-- file with procedures for configuring config db
-- use prefix
-- dv_config_<>
-- generic code to setup config

SET search_path TO ore_config;


/*--------------- DELETE OBJECT ----------------------------------*/
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
  object_schema_v varchar(50):='ore_config';
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
    t.table_schema = object_schema_v AND t.table_name = object_type_in;

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




/*-------------- UPDATE CONFIG OBJECT DETAILS --------------------- */

CREATE OR REPLACE FUNCTION dv_config_object_update
  (
    object_type_in     VARCHAR(100), -- config table name
    object_key_in      INTEGER, -- object key
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
  object_schema_v    VARCHAR(50) :='ore_config';
BEGIN

  rowcount_v:=0;

  -- list of all columns
  CREATE TEMP TABLE columns_list_tmp ON COMMIT DROP AS
    SELECT
      column_name,
      data_type,
      is_key,
      is_no_update
    FROM
      (
        SELECT
          c.column_name,
          c.is_nullable,
          replace(c.data_type, 'character varying', 'varchar') AS data_type,
          CASE WHEN c.column_name = kcu.column_name
            THEN 1
          ELSE 0 END                                        AS is_key,
          CASE WHEN c.column_name IN ('updated_by', 'updated_datetime', 'release_key')
            THEN 1
          ELSE 0 END                                              is_no_update
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
        WHERE t.table_schema = object_schema_v AND t.table_name = object_type_in
      ) r;

  -- check if object exists
  SELECT column_name
  INTO key_column_v
  FROM columns_list_tmp
  WHERE is_key = 1;

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

    -- if there are any columns to update
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

        -- excluding key and audit parameters for update
        SELECT
          column_name,
          data_type
        INTO column_name_v, column_data_type_v
        FROM columns_list_tmp
        WHERE is_key=0  AND is_no_update =0 AND column_name = object_settings_in [i] [1];

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
               || quote_literal(cast(object_key_in AS INT));

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


/*------------------ ADD OBJECT INTO CONFIG ----------------*/

CREATE OR REPLACE FUNCTION dv_config_object_insert
  (
    object_type_in     VARCHAR(100), -- table name in a list of
    object_settings_in VARCHAR [] [2]-- array parameters to insert column_name -> value
  )
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v      INTEGER :=0;
  column_name_v   VARCHAR(50);
  column_value_v  VARCHAR;
  column_type_v   VARCHAR(30);
  release_key_v   INT;
  owner_key_v     INT;
  sql_v           VARCHAR(2000);
  sql_select_v    VARCHAR(2000);
  array_length_v  INT;
  counter_v       INT;
  delimiter_v     CHAR(10) = ' , ';
  object_schema_v VARCHAR(50) :='ore_config';
BEGIN

  rowcount_v:=0;
  -- check if object exists
  SELECT count(*)
  INTO rowcount_v
  FROM information_schema.tables t
  WHERE t.table_schema = object_schema_v AND t.table_name = object_type_in;

  IF rowcount_v = 0
  THEN
    RAISE NOTICE 'Not valid object type --> %', object_type_in;
    RETURN rowcount_v;
  END IF;

  -- if there any columns to update
  array_length_v:=array_length(object_settings_in, 1);

  IF array_length_v = 0
  THEN
    RAISE NOTICE 'Nothing to insert, check parameters --> %', object_settings_in;
    RETURN array_length_v;
  END IF;

  -- return list of columns except from key column and audit columns
  -- also fine if column omitted and nullable
  -- need checking release and owner integrity
  DROP TABLE IF EXISTS columns_list_tmp;

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
        WHERE t.table_schema = object_schema_v AND t.table_name = object_type_in
              AND c.column_name NOT IN ('updated_by', 'updated_datetime')) r
    WHERE is_key IS NULL;

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

    IF object_settings_in [i] [1] = 'release_number' AND object_type_in NOT IN ('dv_release')
    THEN
      SELECT release_key
      INTO release_key_v
      FROM dv_release
      WHERE release_number = cast(object_settings_in [i] [2] AS INTEGER);

      GET DIAGNOSTICS rowcount_v = ROW_COUNT;

      IF rowcount_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number --> %', object_settings_in [i] [2];

        RETURN rowcount_v;
      END IF;
    END IF;


    IF object_settings_in [i] [1] = 'owner_key' AND
       object_type_in NOT IN ('dv_release', 'dv_owner', 'dv_default_column')
    THEN
      SELECT owner_key
      INTO owner_key_v
      FROM dv_owner
      WHERE owner_key = cast(object_settings_in [i] [2] AS INTEGER);

      GET DIAGNOSTICS rowcount_v = ROW_COUNT;

      IF rowcount_v = 0
      THEN
        RAISE NOTICE 'Nonexistent owner_key --> %', object_settings_in [i] [2];
        RETURN rowcount_v;
      END IF;

    END IF;

    -- lookup columns in temp table
    -- release number processed differently for 2 cases - dv_release and the rest
    SELECT
      column_name,
      data_type,
      CASE WHEN column_name = 'release_key'
        THEN cast(release_key_v AS VARCHAR)
      ELSE object_settings_in [i] [2] END
    INTO column_name_v, column_type_v, column_value_v
    FROM columns_list_tmp
    WHERE column_name = CASE WHEN object_type_in <> 'dv_release'
      THEN replace(object_settings_in [i] [1], 'release_number', 'release_key')
                        ELSE object_settings_in [i] [1] END;

    GET DIAGNOSTICS rowcount_v = ROW_COUNT;

    IF rowcount_v > 0
    THEN
      counter_v:=counter_v + 1;
      IF counter_v > 1
      THEN
        sql_v:=sql_v || delimiter_v;
        sql_select_v:=sql_select_v || delimiter_v;
      END IF;

      -- sql list of columns
      sql_v:=sql_v || quote_ident(column_name_v);
      -- sql list of values
      sql_select_v:=sql_select_v
                    || ' cast('
                    || quote_literal(column_value_v)
                    || ' as '
                    || column_type_v
                    || ')';

      -- update if column was found
      UPDATE columns_list_tmp
      SET is_found = 1
      WHERE column_name = column_name_v;
    END IF;

  END LOOP;


  counter_v:=0;
  -- check number of 'not found must' parameters for insert
  SELECT count(*)
  INTO counter_v
  FROM columns_list_tmp
  WHERE is_nullable = 'NO' AND column_default IS NULL AND is_found = 0;


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

