CREATE FUNCTION fn_source_cleanup (table_schema_in character varying, table_name_in character varying) RETURNS text
	LANGUAGE plpgsql
AS $$
DECLARE
  sql_v            TEXT;
  process_status_v VARCHAR :='DONE';
BEGIN

  sql_v:='delete from ' || table_schema_in || '.' || table_name_in || ' where  dv_process_status=''' || process_status_v
         || ''';';

  RETURN sql_v;

END
$$