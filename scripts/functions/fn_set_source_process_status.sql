CREATE OR REPLACE FUNCTION fn_set_source_process_status(table_schema_in VARCHAR, table_name_in VARCHAR,
                                                        operation_in    VARCHAR)
  RETURNS TEXT AS
$BODY$
DECLARE
  process_status_to_v   VARCHAR(30) :=operation_in;
  process_status_from_v VARCHAR(30);
  sql_v                 TEXT;
BEGIN

  -- update processing status
  CASE operation_in
    WHEN 'PROCESSING'
    THEN
      process_status_from_v:='RAW';
    WHEN 'DONE'
    THEN
      process_status_from_v:='PROCESSING';
  ELSE
    NULL;
  END CASE;

  sql_v:='update ' || table_schema_in || '.' || table_name_in || ' set dv_process_status=' || process_status_to_v ||
         ' where dv_process_status=' || process_status_from_v || ';';

  RETURN sql_v;

END
$BODY$
LANGUAGE plpgsql;