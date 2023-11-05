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
