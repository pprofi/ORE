/*-------------------------------------------
        OPTIMAL REPORTING ENGINE
        CONFIG SETUP SCRIPTS
-------------------------------------------*/

-- file with procedures for configuring config db
-- use prefix
-- dv_config_<>

SET search_path TO ore_config;

-- dv_config_release
-- insert, update, delete all parameters


CREATE OR REPLACE FUNCTION dv_config_release(
  operation_key_in       CHAR(1),
  release_key_in         INT DEFAULT 0,
  release_number_in      INT DEFAULT 0,
  release_description_in VARCHAR(256) DEFAULT NULL
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount INTEGER :=0;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_release
    WHERE release_key = release_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_release
      SET release_description = release_description_in, reference_number = release_number_in
      WHERE release_key = release_key_in;
  ELSIF operation_key_in = 'i'
    THEN
      INSERT INTO dv_release (release_number, release_description)
        SELECT
          release_number_in,
          release_description_in;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_owner_conf

CREATE OR REPLACE FUNCTION dv_config_owner(
  operation_key_in     CHAR(1),
  owner_key_in         INT DEFAULT 0,
  owner_name_in        VARCHAR(256) DEFAULT NULL,
  owner_description_in VARCHAR(256) DEFAULT NULL,
  release_number_in    INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_owner
    WHERE owner_key = owner_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_owner
      SET owner_description = owner_description_in, owner_name = owner_name_in
      WHERE owner_key = owner_key_in;
  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT m.release_key
      INTO release_key_v
      FROM dv_release m
      WHERE release_number = release_number_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number--> %', release_number_in;
      ELSE
        INSERT INTO dv_owner (owner_name, owner_description, release_key)
          SELECT
            owner_name_in,
            owner_description_in,
            release_key_v;
      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_source_system_conf


CREATE OR REPLACE FUNCTION dv_config_source_system(
  operation_key_in        CHAR(1),
  system_key_in           INT DEFAULT 0,
  source_system_name_in   VARCHAR(256) DEFAULT NULL,
  source_system_schema_in VARCHAR(256) DEFAULT NULL,
  is_retired_in           BOOLEAN DEFAULT FALSE,
  release_number_in       INT DEFAULT 0,
  owner_key_in            INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;

BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_source_system
    WHERE source_system_key = system_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_source_system
      SET source_system_name = source_system_name_in, source_system_schema = source_system_schema_in,
        is_retired           = is_retired_in
      WHERE source_system_key = system_key_in;
  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE
        INSERT INTO dv_source_system (source_system_name, source_system_schema, is_retired, release_key, owner_key)
          SELECT
            source_system_name_in,
            source_system_schema_in,
            is_retired_in,
            release_number_in,
            owner_key_in;
      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_source_table_conf

CREATE OR REPLACE FUNCTION dv_config_source_table(
  operation_key_in       CHAR(1),
  table_key_in           INT DEFAULT 0,
  system_key_in          INT DEFAULT 0,
  source_table_schema_in VARCHAR(128) DEFAULT NULL,
  source_table_name_in   VARCHAR(128) DEFAULT NULL,
  is_retired_in          BOOLEAN DEFAULT FALSE,
  release_number_in      INT DEFAULT 0,
  owner_key_in           INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_source_table
    WHERE source_table_key = table_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_source_table
      SET system_key      = system_key_in, source_table_schema = source_table_schema_in,
        source_table_name = source_table_name_in,
        is_retired        = is_retired_in
      WHERE source_table_key = table_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE

        INSERT INTO dv_source_table (system_key, source_table_schema, source_table_name,
                                     is_retired, release_key, owner_key)
          SELECT
            system_key_in,
            source_table_schema_in,
            source_table_name_in,
            is_retired_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_stage_table_conf

CREATE OR REPLACE FUNCTION dv_config_stage_table(
  operation_key_in      CHAR(1),
  stage_table_key_in    INT DEFAULT 0,
  system_key_in         INT DEFAULT 0,
  stage_table_schema_in VARCHAR(128) DEFAULT NULL,
  stage_table_name_in   VARCHAR(128) DEFAULT NULL,
  is_retired_in         BOOLEAN DEFAULT FALSE,
  release_number_in     INT DEFAULT 0,
  owner_key_in          INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_source_table
    WHERE source_table_key = stage_table_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_stage_table
      SET system_key     = system_key_in, stage_table_schema = stage_table_schema_in,
        stage_table_name = stage_table_name_in,
        is_retired       = is_retired_in
      WHERE stage_table_key = stage_table_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE

        INSERT INTO dv_source_table (system_key, stage_table_schema, stage_table_name,
                                     is_retired, release_key, owner_key)
          SELECT
            system_key_in,
            stage_table_schema_in,
            stage_table_name_in,
            is_retired_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_stage_table_column_conf

CREATE OR REPLACE FUNCTION dv_config_stage_table_column(
  operation_key_in           CHAR(1),
  column_key_in              INT DEFAULT 0,
  stage_table_key_in         INT DEFAULT 0,
  column_name_in             VARCHAR(128) DEFAULT NULL,
  column_type_in             VARCHAR(30) DEFAULT NULL,
  column_length_in           INT DEFAULT NULL,
  column_precision_in        INT DEFAULT NULL,
  column_scale_in            INT DEFAULT NULL,
  collation_name_in          VARCHAR(128) DEFAULT NULL,
  source_ordinal_position_in INT DEFAULT NULL,
  is_source_date_in          INT DEFAULT 0,
  discard_flag_in            INT DEFAULT 0,
  is_retired_in              BOOLEAN DEFAULT FALSE,
  release_number_in          INT DEFAULT 0,
  owner_key_in               INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN

    DELETE
    FROM dv_stage_table_column
    WHERE column_key = column_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_stage_table_column
      SET stage_table_key       = stage_table_key_in, column_name = column_name_in, column_type = column_type,
        column_length           = column_length_in, column_precision = column_precision_in,
        column_scale            = column_scale_in, collation_name = collation_name_in,
        source_ordinal_position = source_ordinal_position_in,
        is_source_date          = is_source_date_in,
        discard_flag            = discard_flag_in, is_retired = is_retired_in
      WHERE column_key = column_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE

        INSERT INTO dv_stage_table_column (stage_table_key, column_name, column_type, column_length, column_precision, column_scale, collation_name,
                                           source_ordinal_position,
                                           is_source_date, discard_flag, is_retired, release_key, owner_key)
          SELECT
            stage_table_key_in,
            column_name_in,
            column_type_in,
            column_length_in,
            column_precision_in,
            column_scale_in,
            collation_name_in,
            source_ordinal_position_in,
            is_source_date_in,
            discard_flag_in,
            is_retired_in,
            release_key_v,
            owner_key_v;


      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_hub_conf

CREATE OR REPLACE FUNCTION dv_config_hub(
  operation_key_in  CHAR(1),
  hub_key_in        INT DEFAULT 0,
  hub_name_in       VARCHAR(128) DEFAULT NULL,
  hub_schema_in     VARCHAR(128) DEFAULT NULL,
  is_retired_in     BOOLEAN DEFAULT FALSE,
  release_number_in INT DEFAULT 0,
  owner_key_in      INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_hub
    WHERE hub_key = hub_key_in;

  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_hub
      SET hub_name = hub_name_in,
        hub_schema = hub_schema_in, is_retired = is_retired_in
      WHERE hub_key = hub_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE

        INSERT INTO dv_hub (hub_name, hub_schema, is_retired, release_key, owner_key)
          SELECT
            hub_name_in,
            hub_schema_in,
            is_retired_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_hub_key_conf

CREATE OR REPLACE FUNCTION dv_config_hub_key(
  operation_key_in            CHAR(1),
  hub_key_column_key_in       INT DEFAULT 0,
  hub_key_in                  INT DEFAULT 0,
  hub_key_column_name_in      VARCHAR(128) DEFAULT NULL,
  hub_key_column_type_in      VARCHAR(30) DEFAULT NULL,
  hub_key_column_length_in    INT DEFAULT NULL,
  hub_key_column_precision_in INT DEFAULT NULL,
  hub_key_column_scale_in     INT DEFAULT NULL,
  hub_key_collation_name_in   VARCHAR(128) DEFAULT NULL,
  hub_key_ordinal_position_in INT DEFAULT NULL,
  release_number_in           INT DEFAULT 0,
  owner_key_in                INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_hub_key_column
    WHERE hub_key_column_key = hub_key_column_key_in;

  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_hub_key_column
      SET hub_key                = hub_key_in, hub_key_column_name = hub_key_column_name_in,
        hub_key_column_type      = hub_key_column_type_in,
        hub_key_column_length    = hub_key_column_length_in, hub_key_column_precision = hub_key_column_precision_in,
        hub_key_column_scale     = hub_key_column_scale_in, hub_key_collation_name = hub_key_collation_name_in,
        hub_key_ordinal_position = hub_key_ordinal_position_in
      WHERE hub_key_column_key = hub_key_column_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE


        INSERT INTO dv_hub_key_column (hub_key, hub_key_column_name, hub_key_column_type, hub_key_column_length, hub_key_column_precision, hub_key_column_scale, hub_key_collation_name, hub_key_ordinal_position, release_key, owner_key)
          SELECT
            hub_key_in,
            hub_key_column_name_in,
            hub_key_column_type_in,
            hub_key_column_length_in,
            hub_key_column_precision_in,
            hub_key_column_scale_in,
            hub_key_collation_name_in,
            hub_key_ordinal_position_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;


-- dv_hub_column_conf
CREATE OR REPLACE FUNCTION dv_config_hub_column(
  operation_key_in      CHAR(1),
  hub_column_key_in     INT DEFAULT 0,
  hub_key_column_key_in INT DEFAULT 0,
  column_key_in         INT DEFAULT 0,
  release_number_in     INT DEFAULT 0,
  owner_key_in          INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_hub_column
    WHERE hub_column_key = hub_column_key_in;

  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_hub_column
      SET hub_key_column_key = hub_key_column_key_in,
        column_key           = column_key_in
      WHERE hub_column_key = hub_column_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE


        INSERT INTO dv_hub_column (hub_key_column_key, column_key_in, release_key, owner_key)
          SELECT
            hub_key_column_key_in,
            column_key_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_satellite_conf


CREATE OR REPLACE FUNCTION dv_config_satellite(
  operation_key_in               CHAR(1),
  satellite_key_in               INT DEFAULT 0,
  hub_key_in                     INT DEFAULT 0,
  link_key_in                    INT DEFAULT 0,
  link_hub_satellite_flag_in     CHAR(1) DEFAULT 'H',
  satellite_name_in              VARCHAR(128) DEFAULT NULL,
  satellite_schema_in            VARCHAR(128) DEFAULT NULL,
  duplicate_removal_threshold_in INT DEFAULT 0,
  is_retired_in                  BOOLEAN DEFAULT FALSE,
  release_number_in              INT DEFAULT 0,
  owner_key_in                   INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_satellite
    WHERE satellite_key = satellite_key_in;

  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_satellite
      SET hub_key               = hub_key_in, link_key = link_key_in,
        link_hub_satellite_flag = link_hub_satellite_flag_in, satellite_name = satellite_name_in,
        satellite_schema        = satellite_schema_in, duplicate_removal_threshold = duplicate_removal_threshold_in,
        is_retired              = is_retired_in
      WHERE satellite_key = satellite_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE

        INSERT INTO dv_satellite (hub_key, link_key, link_hub_satellite_flag, satellite_name,
                                  satellite_schema, duplicate_removal_threshold, is_retired, release_key, owner_key)
          SELECT
            hub_key_in,
            link_key_in,
            link_hub_satellite_flag_in,
            satellite_name_in,
            satellite_schema_in,
            duplicate_removal_threshold_in,
            is_retired_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_satellite_column_conf

CREATE OR REPLACE FUNCTION dv_config_satellite_column(
  operation_key_in        CHAR(1),
  satellite_column_key_in INT DEFAULT 0,
  satellite_key_in        INT DEFAULT 0,
  column_key_in           INT DEFAULT 0,
  release_number_in       INT DEFAULT 0,
  owner_key_in            INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_satellite_column
    WHERE satellite_column_key = satellite_column_key_in;

  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_satellite_column
      SET satellite_key = satellite_key_in,
        column_key      = column_key_in
      WHERE satellite_column_key = satellite_column_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE


        INSERT INTO dv_hub_column (hub_key_column_key, column_key_in, release_key, owner_key)
          SELECT
            hub_key_column_key_in,
            column_key_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_default_column_conf

CREATE OR REPLACE FUNCTION dv_config_default_column(
  operation_key_in      CHAR(1),
  default_column_key_in INT DEFAULT 0,
  object_type_in        VARCHAR(30) DEFAULT NULL,
  object_column_type_in VARCHAR(30) DEFAULT NULL,
  ordinal_position_in   INTEGER DEFAULT 0,
  column_prefix_in      VARCHAR(30) DEFAULT NULL,
  column_suffix_in      VARCHAR(30) DEFAULT NULL,
  column_name_in        VARCHAR(128) DEFAULT NULL,
  column_type_in        VARCHAR(30) DEFAULT NULL,
  column_length_in      INT DEFAULT NULL,
  column_precision_in   INT DEFAULT NULL,
  column_scale_in       INT DEFAULT NULL,
  collation_name_in     VARCHAR(128) DEFAULT NULL,
  is_nullable_in        BOOLEAN DEFAULT TRUE,
  is_pk_in              BOOLEAN DEFAULT FALSE,
  discard_flag_in       BOOLEAN DEFAULT FALSE,
  release_number_in     INT DEFAULT 0,
  owner_key_in          INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_default_column
    WHERE default_column_key = default_column_key_in;

  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_hub_key_column
      SET object_type      = object_type_in,
        object_column_type = object_column_type_in,
        ordinal_position   = ordinal_position_in,
        column_prefix      = column_prefix_in,
        column_suffix      = column_suffix_in,
        column_name        = column_name_in,
        column_type        = column_type_in,
        column_length      = column_length_in,
        column_precision   = column_precision_in,
        column_scale       = column_scale_in,
        collation_name     = collation_name_in,
        is_nullable        = is_nullable_in,
        is_pk              = is_pk_in,
        discard_flag       = discard_flag_in
      WHERE default_column_key = default_column_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE


        INSERT INTO dv_default_column (object_type,
                                       object_column_type,
                                       ordinal_position,
                                       column_prefix,
                                       column_suffix,
                                       column_name,
                                       column_type,
                                       column_length,
                                       column_precision,
                                       column_scale,
                                       collation_name,
                                       is_nullable,
                                       is_pk,
                                       discard_flag, release_key, owner_key)
          SELECT
            object_type_in,
            object_column_type_in,
            ordinal_position_in,
            column_prefix_in,
            column_suffix_in,
            column_name_in,
            column_type_in,
            column_length_in,
            column_precision_in,
            column_scale_in,
            collation_name_in,
            is_nullable_in,
            is_pk_in,
            discard_flag_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;

-- dv_business_rule

CREATE OR REPLACE FUNCTION dv_config_business_rule(
  operation_key_in       CHAR(1),
  business_rule_key_in   INTEGER DEFAULT 0,
  stage_table_key_in     INTEGER DEFAULT 0,
  business_rule_name_in  VARCHAR(128) DEFAULT NULL,
  business_rule_type_in  VARCHAR(20) DEFAULT 'internal_sql',
  business_rule_logic_in TEXT DEFAULT NULL,
  load_type_in           VARCHAR(50) DEFAULT 'delta',
  is_external_in         BOOLEAN DEFAULT FALSE,
  is_retired_in          BOOLEAN DEFAULT FALSE,
  release_number_in      INT DEFAULT 0,
  owner_key_in           INT DEFAULT 0
)
  RETURNS INT AS
$BODY$
DECLARE
  rowcount      INTEGER :=0;
  cnt_v         INT :=0;
  release_key_v INT;
  owner_key_v   INT;
BEGIN
  -- define operation we perform
  IF operation_key_in = 'd'
  THEN
    DELETE
    FROM dv_business_rule
    WHERE business_rule_key = business_rule_key_in;
  ELSIF operation_key_in = 'u'
    THEN
      UPDATE dv_business_rule
      SET stage_table_key   = stage_table_key_in,
        business_rule_name  = business_rule_name_in,
        business_rule_type  = business_rule_type_in,
        business_rule_logic = business_rule_logic_in,
        load_type           = load_type_in,
        is_external         = is_external_in,
        is_retired          = is_retired_in
      WHERE business_rule_key = business_rule_key_in;

  ELSIF operation_key_in = 'i'
    THEN
      cnt_v:=0;

      SELECT
        m.release_key,
        ow.owner_key
      INTO release_key_v, owner_key_v
      FROM dv_release m, dv_owner ow
      WHERE release_number = release_number_in AND ow.owner_key = owner_key_in;

      GET DIAGNOSTICS cnt_v = ROW_COUNT;
      IF cnt_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number or owner_key --> %', release_number_in || ';' || owner_key_in;
      ELSE

        INSERT INTO dv_business_rule (stage_table_key,
                                      business_rule_name,
                                      business_rule_type,
                                      business_rule_logic,
                                      load_type,
                                      is_external,
                                      is_retired, release_key, owner_key)
          SELECT
            stage_table_key_in,
            business_rule_name_in,
            business_rule_type_in,
            business_rule_logic_in,
            load_type_in,
            is_external_in,
            is_retired_in,
            release_key_v,
            owner_key_v;

      END IF;
  ELSE
    RAISE NOTICE 'Nonexistent operation_key_in --> %', operation_key_in;
  END IF;

  GET DIAGNOSTICS rowcount = ROW_COUNT;

  RETURN rowcount;

END
$BODY$
LANGUAGE plpgsql;


