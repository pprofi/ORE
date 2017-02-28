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
          CASE WHEN c.column_name IN ('updated_by', 'updated_datetime')
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
