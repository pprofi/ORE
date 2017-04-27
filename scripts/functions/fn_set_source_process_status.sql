CREATE OR REPLACE FUNCTION fn_set_source_process_status(table_schema_in CHARACTER VARYING,
                                                        table_name_in   CHARACTER VARYING,
                                                        operation_in    CHARACTER VARYING)
  RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  process_status_to_v   VARCHAR(30) :=operation_in;
  process_status_from_v VARCHAR(100);
  sql_v                 TEXT;
  is_null_v             VARCHAR(100) :='';
BEGIN

  CASE operation_in
    WHEN 'PROCESSING'
    THEN
      process_status_from_v:='RAW';
      is_null_v:=' or dv_process_status is null';
    WHEN 'DONE'
    THEN
      process_status_from_v:='PROCESSING';
  ELSE
    NULL;
  END CASE;

  sql_v:='update ' || table_schema_in || '.' || table_name_in || ' set dv_process_status=''' || process_status_to_v ||
         ''' where dv_process_status=''' || process_status_from_v || '''' || is_null_v || ';';

  RETURN sql_v;

END
$$
