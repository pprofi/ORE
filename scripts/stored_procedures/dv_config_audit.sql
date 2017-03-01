CREATE OR REPLACE FUNCTION dv_config_audit()
  RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  new.updated_by:=current_user;
  new.updated_datetime:= now();
  RETURN NULL;
END;
$$