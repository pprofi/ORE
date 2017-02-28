-- function for getting set of default columns for data vault object
CREATE OR REPLACE FUNCTION fn_get_dv_object_default_columns(object_name_in VARCHAR(128), object_type_in VARCHAR(128),
  object_column_type_in varchar(30) default NULL -- all or particular type
)
  RETURNS SETOF dv_column_type AS
$BODY$
DECLARE
  r dv_column_type%ROWTYPE;
BEGIN

  -- check parameter
  IF COALESCE(object_type_in, '') NOT IN ('hub', 'link', 'satellite','stage_table')
  THEN
    RAISE NOTICE 'Not valid object type: can be only hub, satellite --> %', object_type_in;
    RETURN;
  END IF;

  FOR r IN (SELECT
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN rtrim(coalesce(column_prefix, '') || replace(d.column_name, '%', object_name_in) ||
                           coalesce(column_suffix, ''))
              ELSE d.column_name END AS column_name,

              column_type,
              column_length,
              column_precision,
              column_scale,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 0
              ELSE 1 END             AS is_nullable,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 1
              ELSE 0 END             AS is_key,
             d.is_indexed
            FROM dv_default_column d
            WHERE object_type = object_type_in
            and (d.object_column_type=object_column_type_in or object_column_type_in is null)
            ORDER BY is_key DESC) LOOP
    RETURN NEXT r;
  END LOOP;
  RETURN;
END
$BODY$
LANGUAGE 'plpgsql';