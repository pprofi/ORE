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
                          is_nullable,is_key,is_indexed)

  SELECT
    'col1',
    'int',
    0,
    0,
    0,
     0,
    1,1
  UNION ALL
  SELECT
    'col2',
    'varchar',
    10,
    0,
    0,

1,0,0;


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
  LOOP

 FETCH xxx INTO r;

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
do $$
declare

begin
end;$$

select * from fn_get_dv_object_default_columns('customer','hub');


-- create hub test
-- add to config

-- dv_hub
-- dv_hub_key_column
select * from dv_owner;
select * from dv_release;

select * from dv_hub;
select * from public.dv_hub;

select * from public.dv_hub_key_column;

select * from dv_hub_key_column;

create schema DV;

-- add hub records
SELECT dv_config_object_insert('dv_hub',
                               '{{"hub_name","customer"},{"hub_schema","DV"},{"release_number","0"},{"owner_key","2"}}');

-- add hub key column

SELECT dv_config_object_insert('dv_hub_key_column',
                               '{{"hub_key","2"},{"hub_key_column_name","CustomerID"},
                               {"hub_key_column_type","varchar"},
                               {"hub_key_column_length","30"},
                               {"hub_key_column_precision","0"},
                               {"hub_key_column_scale","0"},
                               {"release_number","0"},{"owner_key","2"}}');

SELECT *
FROM dv_config_dv_create_hub(
    'customer',
    'DV',
    2,
    'N'
);

create table DV.h_customer
(
h_customer_key serial primary key,
dv_record_source varchar(50),
dv_load_date_time timestamp,
CustomerID varchar(30)
);
create unique index ux_h_customer_30f9c8ee_c8ba_4e21_ac25_f04de527783c
 on DV.h_customer
(CustomerID
);








-- add index test

-- add drop if recreate



-- generate uuid_generate_v4()

select gen_random_uuid()::text -- uuid_generate_v4();

-- To see what extensions are already installed in your Postgres, run this SQL:

  select * from pg_extension;

-- To see if the "uuid-ossp" extension is available, run this SQL:
  select * from pg_available_extensions;

-- To install/load the extension, run this SQL:
  CREATE EXTENSION "uuid-ossp";

select * from information_schema.table_constraints;


select E'bla bla bla \n enter';

SET search_path TO ore_config;

select public.uuid_generate_v4();


select * from public.dv_defaults;


SELECT CASE
         WHEN default_subtype = 'prefix'
           THEN default_varchar || 'customer'
         WHEN default_subtype = 'suffix'
           THEN 'customer' || default_varchar
         END
  -- INTO result_v
  FROM dv_defaults
  WHERE 1 = 1
        AND default_type = 'hub'
        AND default_subtype IN ('prefix', 'suffix');

SET search_path = ore_config;
CREATE EXTENSION "uuid-ossp";


-- insert satellite data
select * from dv_hub;

SELECT dv_config_object_insert('dv_satellite',
                               '{{"satellite_name","customer_detail"},{"satellite_schema","DV"},
                               {"hub_key","2"},
                               {"link_hub_satellite_flag","H"},
                               {"release_number","0"},{"owner_key","2"}}');

select * from dv_satellite;

-- add source system
SELECT dv_config_object_insert('dv_source_system',
                               '{{"source_system_name","test_system"},{"source_system_schema","DV"},
                                {"release_number","0"},{"owner_key","2"}}');
select * from dv_source_system;

-- add stage table
SELECT dv_config_object_insert('dv_stage_table',
                               '{{"system_key","1"},{"stage_table_schema","DV"},{"stage_table_name","customer_info"},
                                {"release_number","0"},{"owner_key","2"}}');
-- add stage table columns
select * from dv_stage_table;

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","CustomerID"},
                               {"column_type","varchar"},
                               {"column_length","30"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","1"},
                               {"release_number","0"},{"owner_key","2"}}');

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","last_name"},
                               {"column_type","varchar"},
                               {"column_length","50"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","2"},
                               {"release_number","0"},{"owner_key","2"}}');

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","first_name"},
                               {"column_type","varchar"},
                               {"column_length","50"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","3"},
                               {"release_number","0"},{"owner_key","2"}}');

SELECT dv_config_object_insert('dv_stage_table_column',
                               '{{"stage_table_key","1"},{"column_name","phone_number"},
                               {"column_type","varchar"},
                               {"column_length","50"},
                               {"column_precision","0"},
                               {"column_scale","0"},{"source_ordinal_position","4"},
                               {"release_number","0"},{"owner_key","2"}}');

select * from dv_stage_table_column;

-- add linking with stage_table
SELECT dv_config_object_insert('dv_satellite_column',
                               '{{"satellite_key","1"},{"column_key","2"},
                                {"release_number","0"},{"owner_key","2"}}');

SELECT dv_config_object_insert('dv_satellite_column',
                               '{{"satellite_key","1"},{"column_key","3"},
                                {"release_number","0"},{"owner_key","2"}}');

SELECT dv_config_object_insert('dv_satellite_column',
                               '{{"satellite_key","1"},{"column_key","4"},
                                {"release_number","0"},{"owner_key","2"}}');


SELECT * FROM fn_get_dv_object_default_columns('customer_detail', 'satellite');



