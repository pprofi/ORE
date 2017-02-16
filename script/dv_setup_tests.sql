CREATE TABLE dv_hub
(
    hub_key INTEGER DEFAULT nextval('dv_hub_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_name VARCHAR(128) NOT NULL,
    hub_schema VARCHAR(128) NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT "current_user"() NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_hub_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_hub_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_unq ON dv_hub (owner_key, hub_schema, hub_name);

select * from dv_default_column;

SET search_path TO ore_config;


DO $$DECLARE r     dv_column_type[2];

        rowcount_v INT;

BEGIN


 r[1].column_name='a';

  /*insert into r (column_name ,
  column_type  ,
  column_length  ,
  column_precision ,
  column_scale     ,
  ordinal_position) values
('a','int',1,2,3,1);
*/
END$$;






create table test_create of dv_column_type;


select * from test_create;

INSERT INTO test_create (column_name,
                         column_type,
                         column_length,
                         column_precision,
                         column_scale,
                         ordinal_position)

  SELECT
    'col1',
    'int',
    0,
    0,
    0,
    1
  UNION ALL
  SELECT
    'col2',
    'varchar',
    10,
    0,
    0,
    2;



DO $$DECLARE
  cnt_v int;
  rec  cursor for select * from test_create;
BEGIN

open rec;


select ore_config.dv_config_dv_table_create(
  'customer',
  'ore_config',
  'hub',
  'rec',
   'N'
) into cnt_v;
  raise NOTICE 'SQL -->%',cnt_v;
END$$;



DO $$
declare xxx cursor for select * from ore_config.test_create;
sqlv text;
BEGIN
  open xxx;
  select public.fx('xxx') into sqlv;
end$$;





CREATE OR REPLACE FUNCTION public.fx(refcursor)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare r ore_config.dv_column_type;
begin
  while true
  loop
    fetch $1 into r;
    exit when not found;
    raise notice '%', r;
  end loop;
end;
$function$





CREATE OR REPLACE FUNCTION ore_config.testcursor(refcursor)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare r dv_column_type;
  sql_create_v text;
  sql_col_def_v text;
begin
  while true
  loop
    FETCH $1 INTO r;

    -- add key column


    SELECT fn_build_column_definition(r)
    INTO sql_col_def_v;

    sql_create_v:=sql_create_v || '  ' || sql_col_def_v || '  '|| ',';
    EXIT WHEN NOT found;
    RAISE NOTICE '%', r;
  end loop;
end;
$function$



DO $$
declare xxx cursor for select * from test_create;
sqlv text;
BEGIN
  open xxx;
  select ore_config.testcursor('xxx') into sqlv;
end$$;



CREATE OR REPLACE FUNCTION fn_test
  (
    r col_type
  )
  RETURNS VARCHAR AS
$BODY$
DECLARE
  result_v VARCHAR(500);
BEGIN

  result_v:= r.column_name;

  -- if key
  IF r.is_key = 1
  THEN
    result_v:=result_v||' serial primary key';
  ELSE
    result_v:=result_v||' '||upper(r.column_type);
    CASE
    -- numeric
      WHEN upper(r.column_type) IN ('decimal', 'numeric')
      THEN
        result_v:=result_v || '(' || cast(r.column_precision AS VARCHAR) || ',' || cast(r.column_scale AS VARCHAR) || ') ';
    -- varchar
      WHEN upper(r.column_type) IN ('char', 'varchar')
      THEN
        result_v:=result_v || '(' || cast(r.column_length AS VARCHAR) || ')';
    ELSE
      result_v:=result_v;
    END CASE;

    -- if not null
    IF r.is_nullable = 0
    THEN
      result_v:=result_v || ' NOT NULL ';
    END IF;

  END IF;


  RETURN result_v;

END
$BODY$
LANGUAGE plpgsql;


-- test
create table test_col_type of dv_column_type;

CREATE TYPE col_type AS
(
  column_name      VARCHAR(128),
  column_type      VARCHAR(50) ,
  column_length    INT ,
  column_precision INT ,
  column_scale     INT ,
  is_nullable int,
  is_key int
);

select * from test_col_type;

INSERT INTO test_col_type (column_name,
                         column_type,
                         column_length,
                         column_precision,
                         column_scale,
                          is_nullable,is_key)

  SELECT
    'col1',
    'int',
    0,
    0,
    0,
     0,
    1
  UNION ALL
  SELECT
    'col2',
    'varchar',
    10,
    0,
    0,

1,0;


CREATE OR REPLACE FUNCTION ore_config.testcursor(refcursor)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare r col_type;
  sql_create_v text;
  sql_col_def_v text;
begin
  while true
  loop
    FETCH $1 INTO r;

    -- add key column


    SELECT fn_test(r)
    INTO sql_col_def_v;

    sql_create_v:=sql_create_v || '  ' || sql_col_def_v || '  '|| ',';
    EXIT WHEN NOT found;
    RAISE NOTICE '%', r;
  end loop;
end;
$function$;


DO $$
declare xxx cursor for select * from test_col_type;
sqlv text;
BEGIN
  open xxx;
  select ore_config.testcursor(xxx) into sqlv;
end$$;

DO $$
declare
    r col_type;
  xxx cursor for select * from test_col_type;
  l record;
sqlv text;
BEGIN
  open xxx;
  LOOP --начинаем цикл по курсору
 --извлекаем данные из строки и записываем их в переменные
 FETCH xxx INTO r;
 --если такого периода и не возникнет, то мы выходим
 IF NOT FOUND THEN EXIT;END IF;
  select ore_config.fn_test(r) into sqlv;
  RAISE NOTICE '%', sqlv;
    END LOOP;
  close xxx;
end$$;




--- 2

CREATE OR REPLACE FUNCTION ore_config.fx_1(refcursor)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare r col_type;
  sqlv text;
begin
  while true
  loop
    fetch $1 into r;
    exit when not found;
 --  select ore_config.fn_test(r) into sqlv;
    raise notice '%', sqlv;
  end loop;
end;
$function$


  DO $$
declare xxx cursor for select * from test_col_type;
sqlv text;
BEGIN
  open xxx;
  select public.fx_1('xxx') into sqlv;
end$$;

-----------------------


DO $$
declare xxx cursor for select * from ore_config.test_col_type;
sqlv text;
BEGIN
  open xxx;
  select public.fx('xxx') into sqlv;
end$$;





CREATE OR REPLACE FUNCTION public.fx(refcursor)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare r ore_config.col_type;
   sqlv text;
begin
  while true
  loop
    fetch $1 into r;
    exit when not found;
    select ore_config.fn_test(r) into sqlv;
    raise notice '%', sqlv;
    raise notice '%', r;
  end loop;
end;
$function$


  -- test example
DO $$
declare xxx cursor for select * from ore_config.test_col_type;
sqlv text;
BEGIN
  open xxx;
  select ore_config.dv_config_dv_table_create('customer',
  'ore_config',
  --'hub',
  'xxx',
   'N') into sqlv;
end$$;


DO $$
declare
tx text:='test of adding columns,';
  tx_sub text;
len_v int;
begin

select length(tx)-1 into len_v;
   raise notice '%', len_v;

select substring(tx from 1 for length(tx)-1) into tx_sub;
  raise notice '%', tx_sub;
end$$;

-- get object default columns