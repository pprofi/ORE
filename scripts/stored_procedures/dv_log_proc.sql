CREATE OR REPLACE FUNCTION dv_log_proc(proc_name_in TEXT, message_in TEXT)
  RETURNS VOID AS
$BODY$
BEGIN

  INSERT INTO dv_log (log_datetime, log_proc, message)
  VALUES (now(), proc_name_in, message_in);

  RETURN;
END
$BODY$
LANGUAGE plpgsql;