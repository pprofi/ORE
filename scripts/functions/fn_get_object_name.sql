-- get object name

CREATE OR REPLACE FUNCTION fn_get_object_name
  (
      object_name_in VARCHAR(256)
    , object_type_in VARCHAR(50)
  )
  RETURNS VARCHAR(256)
AS
$BODY$
DECLARE result_v VARCHAR(256);
BEGIN
  SELECT CASE
         WHEN default_subtype = 'prefix'
           THEN default_varchar || object_name_in
         WHEN default_subtype = 'suffix'
           THEN object_name_in || default_varchar
         END
  INTO result_v
  FROM dv_defaults
  WHERE 1 = 1
        AND default_type = object_type_in
        AND default_subtype IN ('prefix', 'suffix');

  RETURN result_v;
END
$BODY$
LANGUAGE 'plpgsql';
