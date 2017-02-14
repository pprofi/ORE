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
  object_columns_in dv_column_type ,
  recreate_flag_in  CHAR(1) = 'N'
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v  INT :=0;
  sql_create_v       TEXT;
  sql_key_v text;
  sql_col_def_v text;
  sql_index_v       TEXT;
  sql_drop_v text;
  crlf_v      CHAR(10) :=' ';
  delimiter_v CHAR(2) :=',';
  array_length_v     INT;
BEGIN

-- check if parameters correct
-- schema exists
-- object_type correct satellite, hub, stage_table

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
  IF COALESCE(object_type_in, '') NOT IN ('Hub', 'Lnk', 'Sat')
  THEN
    RAISE NOTICE 'Not valid object type: can be only hub, satellite --> %', object_type_in;
    RETURN rowcount_v;
  END IF;

   -- if there are any columns
    array_length_v:=array_length(object_columns_in, 1);

  IF array_length_v = 0
  THEN
    RAISE NOTICE 'Not valid object columns --> %', object_columns_in;
    RETURN rowcount_v;
  END IF;

  IF COALESCE(recreate_flag_in, '') NOT IN ('Y', 'N')
  THEN
    RAISE NOTICE 'Not valid recreate_flag value : Y or N --> %', recreate_flag_in;
    RETURN rowcount_v;
  END IF;

-- if recreate flag set to Yes then drop

  IF recreate_flag_in = 'Y'
  THEN

    SELECT count(*)
    INTO rowcount_v
    FROM information_schema.tables t
    WHERE t.table_schema = object_schema_in AND t.table_name = object_name_in;

    IF rowcount_v = 1
    THEN
      sql_drop_v:='DROP TABLE ' || object_schema_in || '.' || object_name_in;
    ELSE
      RAISE NOTICE 'Can not drop table, object does not exist  --> %', object_schema_in || '.' || object_name_in;
      RETURN rowcount_v;
    END IF;

  END IF;


  -- build create statement
  sql_create_v:='create table '||object_schema_in || '.' || object_name_in||' (';
  sql_key_v:='serial primary key,';

  -- column definitions

  FOR i IN 1..array_length_v LOOP

   select fn_build_column_definition(object_columns_ini) into sql_col_def_v;
   sql_create_v:=sql_create_v||crlf_v||sql_col_def_v|| crlf_v||delimiter_v;


  END LOOP;

SET _Step = 'Create Table Statement'
SELECT SQL += 'CREATE TABLE ' + table_name + '(' + crlf + ' '

  /*--------------------------------------------------------------------------------------------------------------*/
  SET _Step = 'Add the Columns'
--1. Primary Key
SELECT SQL = SQL + column_name +
             dbo.fn_build_column_definition(column_type, column_length, column_precision, column_scale, Collation_Name,
                                            0, 1) + crlf + ','
FROM fn_get_key_definition(object_name, object_type)

--Payload
SELECT SQL = SQL + column_name + ' ' +
             dbo.fn_build_column_definition(column_type, column_length, column_precision, column_scale, Collation_Name,
                                            1, 0) + crlf + ','
FROM
  (SELECT *
   FROM default_columns) a
ORDER BY source_ordinal_position

SELECT SQL = SQL + column_name + ' ' +
             dbo.fn_build_column_definition(column_type, column_length, column_precision, column_scale, Collation_Name,
                                            1, 0) + crlf + ','
FROM
  (SELECT *
   FROM payload_columns) a
ORDER BY satellite_ordinal_position, column_name


END
$BODY$
LANGUAGE plpgsql;


-- column definition
create or replace FUNCTION fn_build_column_definition
(
	 data_type_in varchar(50),
   data_length_in int,
   precision_in int,
   scale_in int,
   is_nullable_in bit,
   is_key_in bit
)
RETURNS varchar AS
$BODY$
DECLARE
  result_v varchar(500);
BEGIN

  result_v:=upper(data_type_in);

 CASE
--NUMERIC
  WHEN upper(data_type_in) IN ('decimal','numeric')
               THEN

  result_v:=result_v||'('||cast(precision_in as varchar)||','||cast(scale_in as varchar)||') ';


--FLOAT
               WHEN  upper(data_type_in) IN ('float', 'int','integer','double precision',
                                             'smallint','bigint','text','real','money','bootean','bit')
               THEN
                   result_v:=result_v;
--char varchar
               WHEN  upper(data_type_in) IN ('char','varchar')
               THEN

result_v:=result_v||'('||cast(data_length_in as varchar)||')';
--datetime
               WHEN UPPER(DataType) IN ('datetime','image','real')
               THEN SPACE(18 - LEN(UPPER(DataType)))
                    + '              '





set ResultVar = rtrim(ResultVar) + case when isnull (is_identity, 1) = 1 then ' IDENTITY(1,1) ' else ' ' END

set ResultVar = rtrim(ResultVar) + case when isnull (is_nullable, 1) = 1 then '' else ' NOT' END + ' NULL'
RETURN result_v;

END
$BODY$
LANGUAGE plpgsql;