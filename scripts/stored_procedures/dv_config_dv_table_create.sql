-- generic function for table creation ddl using set of columns passed as ref cursor

CREATE OR REPLACE FUNCTION dv_config_dv_table_create(
  object_name_in    VARCHAR(128),
  object_schema_in  VARCHAR(128),
  object_columns_in REFCURSOR,
  recreate_flag_in  CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_create_v       TEXT;
  sql_key_v          TEXT;
  sql_col_def_v      TEXT;
  sql_index_v        TEXT :='';
  sql_drop_v         TEXT :='';
  crlf_v             CHAR(10) :=' ';
  delimiter_v        CHAR(2) :=',';
  newline_v          CHAR(3) :=E'\n';
  cnt_rec_v          INT;
  rec                dv_column_type;
  i                  INT :=0;
  sql_create_index_v TEXT;

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

  -- check parameters

  IF COALESCE(recreate_flag_in, '') NOT IN ('Y', 'N')
  THEN
    RAISE NOTICE 'Not valid recreate_flag value : Y or N --> %', recreate_flag_in;
    RETURN rowcount_v;
  END IF;

  -- if recreate flag set to Yes then drop / the same if set to No and object exists

  IF recreate_flag_in = 'Y'  -- OR (recreate_flag_in = 'N' AND rowcount_v = 1)
  THEN
    IF rowcount_v = 1
    THEN
      sql_drop_v:='DROP TABLE ' || object_schema_in || '.' || object_name_in || ';' || newline_v;
      /*  ELSE
          RAISE NOTICE 'Can not drop table, object does not exist  --> %', object_schema_in || '.' || object_name_in;
          RETURN rowcount_v;*/
    END IF;

  END IF;

  -- build create statement
  sql_create_v:='create table ' || object_schema_in || '.' || object_name_in || newline_v || '(' || newline_v;
  sql_create_index_v:=
  'create unique index ux_' || object_name_in || '_' || public.uuid_generate_v4() || newline_v || ' on ' ||
   object_schema_in || '.' ||object_name_in || newline_v || '(';
  sql_create_index_v:=replace(sql_create_index_v,'-','_');

  -- column definitions
  -- open cursor

  WHILE TRUE
  LOOP
    FETCH $3 INTO rec;
    EXIT WHEN NOT found;
    -- add key column

    SELECT fn_build_column_definition(rec)
    INTO sql_col_def_v;

    sql_create_v:=sql_create_v || crlf_v || sql_col_def_v || crlf_v || delimiter_v || newline_v;

    -- add index
    IF rec.is_indexed = 1
    THEN
      sql_create_index_v:=sql_create_index_v || rec.column_name || delimiter_v;
      i:=i + 1;
    END IF;

    RAISE NOTICE '%', rec;
  END LOOP;

  -- remove last comma
  sql_create_v:=substring(sql_create_v FROM 1 FOR length(sql_create_v) - 2) || newline_v || ');' || newline_v;

  IF i <> 0
  THEN
    sql_create_index_v:=substring(sql_create_index_v FROM 1 FOR length(sql_create_index_v) - 1) || newline_v || ');' ||
                        newline_v;
    sql_create_v:=sql_create_v || sql_create_index_v;
  END IF;

  RAISE NOTICE 'SQL --> %', sql_create_v;

  RETURN sql_drop_v || sql_create_v;
END
$BODY$
LANGUAGE plpgsql;
