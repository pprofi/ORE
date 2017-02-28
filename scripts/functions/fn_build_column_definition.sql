CREATE OR REPLACE FUNCTION fn_build_column_definition
  (
    r dv_column_type
  )
  RETURNS VARCHAR AS
$BODY$
DECLARE
  result_v VARCHAR(500);
BEGIN
-- build column definition
  result_v:= r.column_name;

  -- if key
  IF r.is_key = 1
  THEN
    result_v:=result_v || ' serial primary key';
  ELSE
    result_v:=result_v || ' ' || lower(r.column_type);
    CASE
    -- numeric
      WHEN lower(r.column_type) IN ('decimal', 'numeric')
      THEN
        result_v:=result_v || '(' || cast(r.column_precision AS VARCHAR) || ',' || cast(r.column_scale AS VARCHAR) ||
                  ') ';
        -- varchar
      WHEN lower(r.column_type) IN ('char', 'varchar')
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

raise NOTICE 'Column defenition % -->',result_v;

  RETURN result_v;

END
$BODY$
LANGUAGE plpgsql;