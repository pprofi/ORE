CREATE OR REPLACE FUNCTION fn_dv_source_cleanup(table_schema_in VARCHAR, table_name_in VARCHAR)
  RETURNS TEXT AS
-- removes processed data from stage and source tables
$BODY$
DECLARE
  sql_v            TEXT;
  process_status_v VARCHAR :='DONE';
BEGIN

  sql_v:='delete from ' || table_schema_in || '.' || table_name_in || ' where  dv_process_status=' || process_status_v
         || ';';

  RETURN sql_v;

END
$BODY$
LANGUAGE plpgsql;