/*-------------------------------------------
        OPTIMAL REPORTING ENGINE
        DATA VAULT SETUP SCRIPTS
        object creation, etc
-------------------------------------------*/

-- create stage table
-- create hub
-- create satellite

SET search_path TO ore_config;

-- default columns to fill in
CREATE TABLE ore_config.defaults_columns
(LIKE dv_default_column EXCLUDING CONSTRAINTS
);

SELECT *
FROM ore_config.defaults_columns;

SELECT *
FROM ore_config.dv_default_column;

DROP TABLE ore_config.defaults_columns;

CREATE TABLE ore_config.defaults_columns
(
  object_type        VARCHAR(100),
  object_column_type VARCHAR(100),
  ordinal_position   VARCHAR(100),
  column_prefix      VARCHAR(100),
  column_name        VARCHAR(256),
  column_suffix      VARCHAR(100),
  column_type        VARCHAR(100),
  column_length      VARCHAR(100),
  column_precision   VARCHAR(100),
  column_scale       VARCHAR(100),
  collation_name     VARCHAR(128),
  is_nullable        VARCHAR(100),
  is_pk              VARCHAR(100),
  discard_flag       VARCHAR(100),
  release_key        VARCHAR(100),
  version_number     VARCHAR(100)
);

UPDATE ore_config.defaults_columns
SET release_key = 4;

SELECT *
FROM dv_release;
SELECT *
FROM dv_owner;

-- default release number
SELECT dv_config_object_insert('dv_release',
                               '{{"release_description","Initial release"},{"release_number","0"}}');
-- default owner_key - OptimalBI
SELECT dv_config_object_insert('dv_owner',
                               '{{"owner_name","OptimalBI"},{"owner_description","OptimalBI"},{"release_number","0"},{"is_retired","0"}}');


DO $$DECLARE r          RECORD;
             param      VARCHAR(50)  ;
             rowcount_v INT;

BEGIN
  param:=array_fill(NULL :: VARCHAR, ARRAY 16, 2);

  FOR r IN SELECT *
           FROM ore_config.defaults_columns
  LOOP

    param 1 1:='object_type';
    param 1 2:=r.object_type;
    param 2 1:='object_column_type';
    param 2 2:=r.object_column_type;
    param 3 1:='ordinal_position';
    param 3 2:=r.ordinal_position;
    param 4 1:='column_prefix';
    param 4 2:=r.column_prefix;
    param 5 1:='column_name';
    param 5 2:= r.column_name;
    param 6 1:='column_suffix';
    param 6 2:= r.column_suffix;
    param 7 1:='column_type';
    param 7 2:= r.column_type;
    param 8 1:= 'column_length';
    param 8 2:= CASE WHEN r.column_length = ''
      THEN '0'
                    ELSE r.column_length END;
    param 9 1:= 'column_precision';
    param 9 2:=  CASE WHEN r.column_precision = ''
      THEN '0'
                     ELSE r.column_precision END;
    param 10 1:= 'column_scale';
    param 10 2:=  CASE WHEN r.column_scale = ''
      THEN '0'
                      ELSE r.column_scale END;
    param 11 1:= 'collation_name';
    param 11 2:= r.collation_name;
    param 12 1:= 'is_nullable';
    param 12 2:= r.is_nullable;
    param 13 1:= 'is_pk';
    param 13 2:=r.is_pk;
    param 14 1:= 'discard_flag';
    param 14 2:=  r.discard_flag;
    param 15 1:= 'owner_key';
    param 15 2:=  2;
    param 16 1:= 'release_number';
    param 16 2:=  0;

    RAISE NOTICE '	param	%', param;
    SELECT ore_config.dv_config_object_insert('dv_default_column', param)
    INTO rowcount_v;
  END LOOP;
END$$;

UPDATE ore_config.dv_default_column
SET column_type = 'timestamp'
WHERE column_type = 'datetimeoffset';

UPDATE ore_config.dv_default_column
SET column_name = '%'
WHERE column_name = '_';
commit;

select * from ore_config.dv_default_column

UPDATE ore_config.dv_default_column
SET object_type = case when object_type='Hub' then 'hub' when object_type='Sat' then 'satellite'
  else 'link' end
;
-- create table

-- table schema
-- table name
-- create index
-- column definitions = coming from list of columns
-- primary key -- mytable_key    serial primary key,

-- add defaults

-- type


CREATE TYPE dv_column_type AS
(
  column_name      VARCHAR(128),
  column_type      VARCHAR(50) ,
  column_length    INT ,
  column_precision INT ,
  column_scale     INT ,
  ordinal_position INT
);





CREATE OR REPLACE FUNCTION dv_config_dv_table_create(
  object_name_in    VARCHAR(128),
  object_schema_in  VARCHAR(128),
  object_type_in    VARCHAR(30),
 -- object_columns_in REFCURSOR,
  recreate_flag_in  CHAR(1) = 'N'
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v    INT :=0;
  sql_create_v  TEXT;
  sql_key_v     TEXT;
  sql_col_def_v TEXT;
  sql_index_v   TEXT;
  sql_drop_v    TEXT;
  crlf_v        CHAR(10) :=' ';
  delimiter_v   CHAR(2) :=',';
  cnt_rec_v     INT;
  r             dv_column_type;
  rec           RECORD;

BEGIN

  -- check if parameters correct
  -- schema exists
  -- object_type correct satellite, hub, stage_table
  RAISE NOTICE 'SQL --> %', sql_create_v;

  IF COALESCE(object_name_in, '') = ''
  THEN
    RAISE NOTICE 'Not valid object name --> %', object_name_in;
    RETURN rowcount_v;
  END IF;
  IF COALESCE(object_schema_in, '') = ''
  THEN
    RAISE NOTICE 'Not valid object schema--> %', object_schema_in;
    RETURN rowcount_v;
  END IF;
  IF COALESCE(object_type_in, '') NOT IN ('hub', 'link', 'satellite', 'stage_table')
  THEN
    RAISE NOTICE 'Not valid object type: can be only hub, satellite, stage_table --> %', object_type_in;
    RETURN rowcount_v;
  END IF;

  -- check parameters

  IF COALESCE(recreate_flag_in, '') NOT IN ('Y', 'N')
  THEN
    RAISE NOTICE 'Not valid recreate_flag value : Y or N --> %', recreate_flag_in;
    RETURN rowcount_v;
  END IF;

  -- check if object already exists

  SELECT count(*)
  INTO rowcount_v
  FROM information_schema.tables t
  WHERE t.table_schema = object_schema_in AND t.table_name = object_name_in;

  -- if recreate flag set to Yes then drop / the same if set to No and object exists

  IF recreate_flag_in = 'Y' OR (recreate_flag_in = 'N' AND rowcount_v = 1)
  THEN
    IF rowcount_v = 1
    THEN
      sql_drop_v:='DROP TABLE ' || object_schema_in || '.' || object_name_in;
    ELSE
      RAISE NOTICE 'Can not drop table, object does not exist  --> %', object_schema_in || '.' || object_name_in;
      RETURN rowcount_v;
    END IF;

  END IF;

  -- build create statement
  sql_create_v:='create table ' || object_schema_in || '.' || object_name_in || ' (';

  -- column definitions

  FOR rec IN (SELECT
              case WHEN d.object_column_type = 'Object_Key'
                then rtrim(coalesce(column_prefix, '') || replace(d.column_name, '%', object_name_in) ||
                   coalesce(column_suffix, '')) else d.column_name end AS column_name,

              column_type,
              column_length,
              column_precision,
              column_scale,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 0
              ELSE 1 END                         AS is_nullable,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 1
              ELSE 0 END                         AS is_key
            FROM dv_default_column d
            WHERE object_type = object_type_in) LOOP
    SELECT fn_build_column_definition(rec.column_name, rec.column_type,
                                      rec.column_length,
                                      rec.column_precision,
                                      rec.column_scale,
                                      rec.is_nullable,
                                      rec.is_key)
    INTO sql_col_def_v;

    sql_create_v:=sql_create_v || crlf_v || sql_col_def_v || crlf_v || delimiter_v;
  END LOOP;

  -- open cursor
/*
  WHILE TRUE
  LOOP
    FETCH object_columns_in INTO r;

    -- add key column


    SELECT fn_build_column_definition(r.column_name, r.column_type,
                                      r.column_length,
                                      r.column_precision,
                                      r.column_scale,
                                      1,
                                      0)
    INTO sql_col_def_v;

    sql_create_v:=sql_create_v || crlf_v || sql_col_def_v || crlf_v || delimiter_v;
    EXIT WHEN NOT found;
    RAISE NOTICE '%', r;
  END LOOP;
  */

  sql_create_v:=sql_create_v||' )';

  RAISE NOTICE 'SQL --> %', sql_create_v;
  RETURN rowcount_v;
END
$BODY$
LANGUAGE plpgsql;


-- column definition
CREATE OR REPLACE FUNCTION fn_build_column_definition
  (
    column_name_in varchar(50),
    data_type_in   VARCHAR(50),
    data_length_in INT,
    precision_in   INT,
    scale_in       INT,
    is_nullable_in int,
    is_key_in      int
  )
  RETURNS VARCHAR AS
$BODY$
DECLARE
  result_v VARCHAR(500);
BEGIN

  result_v:= column_name_in;

  -- if key
  IF is_key_in = 1
  THEN
    result_v:=result_v||' serial primary key';
  ELSE
    result_v:=result_v||' '||upper(data_type_in);
    CASE
    -- numeric
      WHEN upper(data_type_in) IN ('decimal', 'numeric')
      THEN
        result_v:=result_v || '(' || cast(precision_in AS VARCHAR) || ',' || cast(scale_in AS VARCHAR) || ') ';
    -- varchar
      WHEN upper(data_type_in) IN ('char', 'varchar')
      THEN
        result_v:=result_v || '(' || cast(data_length_in AS VARCHAR) || ')';
    ELSE
      result_v:=result_v;
    END CASE;

    -- if not null
    IF is_nullable_in = 0
    THEN
      result_v:=result_v || ' NOT NULL ';
    END IF;

  END IF;


  RETURN result_v;

END
$BODY$
LANGUAGE plpgsql;