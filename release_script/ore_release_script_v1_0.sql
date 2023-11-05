/**********************************************************************************************************************/
/*************************** OPTIMAL REPORTING ENGINE version 1.0 ****************************************************/

-- config schema settings


DO $$
BEGIN

  RAISE NOTICE 'Creating config schema...';

  IF NOT EXISTS(
      SELECT schema_name
      FROM information_schema.schemata
      WHERE schema_name = 'ore_config'
  )
  THEN
    EXECUTE 'CREATE SCHEMA ore_config';
  END IF;

END
$$;

SET search_path TO ore_config;

/*************************** config objects ***************************************************************************/

-- audit function

DO $$
BEGIN
  RAISE NOTICE 'Creating audit function...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_audit()
  RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  new.updated_by:=current_user;
  new.updated_datetime:= now();
  RETURN NULL;
END;
$$;


DO $$
BEGIN
  RAISE NOTICE 'Creating dv_owner...';
END
$$;
/*------ dv owner capture ---------------*/
CREATE SEQUENCE dv_owner_key_seq START 1;

CREATE TABLE dv_owner
(
  owner_key         INTEGER                  DEFAULT nextval(
      'dv_owner_key_seq' :: REGCLASS) PRIMARY KEY                                                        NOT NULL,
  owner_name        VARCHAR(256),
  owner_description VARCHAR(256),
  is_retired        BOOLEAN DEFAULT FALSE                                                                NOT NULL,
  version_number    INTEGER DEFAULT 1                                                                    NOT NULL,
  updated_by        VARCHAR(50) DEFAULT current_user                                                     NOT NULL,
  updated_datetime  TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE UNIQUE INDEX dv_owner_name_unq
  ON dv_owner (owner_name);

-- audit
CREATE TRIGGER dv_owner_audit
AFTER UPDATE ON dv_owner
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();


/*----- release management capture --------------*/

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_release...';
END
$$;

CREATE SEQUENCE dv_release_key_seq START 1;

CREATE TABLE dv_release
(
  release_key         INTEGER DEFAULT nextval('dv_release_key_seq' :: REGCLASS) PRIMARY KEY       NOT NULL,
  release_number      INTEGER                                                                     NOT NULL,
  release_description VARCHAR(256),
  version_number      INTEGER DEFAULT 1                                                           NOT NULL,
  owner_key           INTEGER DEFAULT 1                                                           NOT NULL,
  updated_by          VARCHAR(50) DEFAULT current_user                                            NOT NULL,
  updated_datetime    TIMESTAMP WITH TIME ZONE DEFAULT now()                                      NOT NULL,
  CONSTRAINT fk_dv_release_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_release_number_unq
  ON dv_release (owner_key, release_number);

-- audit
CREATE TRIGGER dv_release_audit
AFTER UPDATE ON dv_release
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- defaults
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_defaults...';
END
$$;

CREATE SEQUENCE dv_defaults_key_seq START 1;
CREATE TABLE ore_config.dv_defaults
(
  default_key      INTEGER                  DEFAULT nextval(
      'dv_defaults_key_seq' :: REGCLASS) PRIMARY KEY                                                       NOT NULL,
  default_type     VARCHAR(50)                                                                             NOT NULL,
  default_subtype  VARCHAR(50)                                                                             NOT NULL,
  default_sequence INTEGER                                                                                 NOT NULL,
  data_type        VARCHAR(50)                                                                             NOT NULL,
  default_integer  INTEGER,
  default_varchar  VARCHAR(128),
  default_datetime TIMESTAMP,
  owner_key        INTEGER DEFAULT 1                                                                       NOT NULL,
  release_key      INTEGER DEFAULT 1                                                                       NOT NULL,
  version_number   INTEGER DEFAULT 1                                                                       NOT NULL,
  updated_by       VARCHAR(50) DEFAULT "current_user"()                                                    NOT NULL,
  updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_defaults_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_defaults_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_defaults_unq
  ON dv_defaults (owner_key, default_type, default_subtype);

-- audit
CREATE TRIGGER dv_defaults_audit
AFTER UPDATE ON dv_defaults
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- default columns

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_default_columns...';
END
$$;

CREATE SEQUENCE dv_default_column_key_seq START 1;

CREATE TABLE dv_default_column
(
  default_column_key INTEGER                  DEFAULT nextval(
      'dv_default_column_key_seq' :: REGCLASS) PRIMARY KEY                                        NOT NULL,
  object_type        VARCHAR(30)                                                                  NOT NULL,
  object_column_type VARCHAR(30)                                                                  NOT NULL,
  ordinal_position   INTEGER DEFAULT 0                                                            NOT NULL,
  column_prefix      VARCHAR(30),
  column_name        VARCHAR(256)                                                                 NOT NULL,
  column_suffix      VARCHAR(30),
  column_type        VARCHAR(30)                                                                  NOT NULL,
  column_length      INTEGER,
  column_precision   INTEGER,
  column_scale       INTEGER,
  collation_name     VARCHAR(128),
  is_nullable        BOOLEAN DEFAULT TRUE                                                         NOT NULL,
  is_pk              BOOLEAN DEFAULT FALSE                                                        NOT NULL,
  is_indexed         BOOLEAN DEFAULT FALSE                                                        NOT NULL,
  discard_flag       BOOLEAN DEFAULT FALSE                                                        NOT NULL,
  release_key        INTEGER DEFAULT 1                                                            NOT NULL,
  owner_key          INTEGER DEFAULT 1                                                            NOT NULL,
  version_number     INTEGER DEFAULT 1                                                            NOT NULL,
  updated_by         VARCHAR(50) DEFAULT current_user                                             NOT NULL,
  updated_datetime   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_default_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_default_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_default_column_unq
  ON dv_default_column (owner_key, object_type, column_name);

-- audit
CREATE TRIGGER dv_default_column_audit
AFTER UPDATE ON dv_default_column
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

/*---- source system capture -------------*/

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_source_system...';
END
$$;

CREATE SEQUENCE dv_source_system_key_seq START 1;

CREATE TABLE dv_source_system
(
  source_system_key    INTEGER                  DEFAULT nextval(
      'dv_source_system_key_seq' :: REGCLASS) PRIMARY KEY                                          NOT NULL,
  source_system_name   VARCHAR(50)                                                                 NOT NULL,
  source_system_schema VARCHAR(50),
  is_retired           BOOLEAN DEFAULT FALSE                                                       NOT NULL,
  release_key          INTEGER DEFAULT 1                                                           NOT NULL,
  owner_key            INTEGER DEFAULT 1                                                           NOT NULL,
  version_number       INTEGER                  DEFAULT 1,
  updated_by           VARCHAR(50) DEFAULT current_user                                            NOT NULL,
  updated_datetime     TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_source_system_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_source_system_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX source_system_name_unq
  ON dv_source_system (owner_key, source_system_name);

-- audit
CREATE TRIGGER dv_source_system_audit
AFTER UPDATE ON dv_source_system
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

/* -- source tables capture -----*/
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_source_table...';
END
$$;

CREATE SEQUENCE dv_source_table_key_seq START 1;

CREATE TABLE dv_source_table
(
  source_table_key    INTEGER                  DEFAULT nextval(
      'dv_source_table_key_seq' :: REGCLASS) PRIMARY KEY                                         NOT NULL,
  system_key          INTEGER                                                                    NOT NULL,
  source_table_schema VARCHAR(128)                                                               NOT NULL,
  source_table_name   VARCHAR(128)                                                               NOT NULL,
  is_retired          BOOLEAN DEFAULT FALSE                                                      NOT NULL,
  release_key         INTEGER DEFAULT 1                                                          NOT NULL,
  owner_key           INTEGER DEFAULT 1                                                          NOT NULL,
  version_number      INTEGER                  DEFAULT 1,
  updated_by          VARCHAR(50) DEFAULT current_user                                           NOT NULL,
  updated_datetime    TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_source_table_dv_source_system FOREIGN KEY (system_key) REFERENCES dv_source_system (source_system_key),
  CONSTRAINT fk_dv_source_table_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_source_table_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_source_table_unq
  ON dv_source_table (owner_key, system_key, source_table_schema, source_table_name);

-- audit
CREATE TRIGGER dv_source_table_audit
AFTER UPDATE ON dv_source_table
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();


/*--- stage table capture -----*/
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_stage_table...';
END
$$;

CREATE SEQUENCE dv_stage_table_key_seq START 1;

CREATE TABLE dv_stage_table
(
  stage_table_key    INTEGER                  DEFAULT nextval(
      'dv_stage_table_key_seq' :: REGCLASS) PRIMARY KEY                                        NOT NULL,
  system_key         INTEGER                                                                   NOT NULL,
  stage_table_schema VARCHAR(128)                                                              NOT NULL,
  stage_table_name   VARCHAR(128)                                                              NOT NULL,
  is_retired         BOOLEAN DEFAULT FALSE                                                     NOT NULL,
  release_key        INTEGER DEFAULT 1                                                         NOT NULL,
  owner_key          INTEGER DEFAULT 1                                                         NOT NULL,
  version_number     INTEGER                  DEFAULT 1,
  updated_by         VARCHAR(50) DEFAULT current_user                                          NOT NULL,
  updated_datetime   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_stage_table_dv_source_system FOREIGN KEY (system_key) REFERENCES dv_source_system (source_system_key),
  CONSTRAINT fk_dv_stage_table_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_stage_table_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_stage_table_unq
  ON dv_stage_table (owner_key, system_key, stage_table_schema, stage_table_name);

-- audit
CREATE TRIGGER dv_stage_table_audit
AFTER UPDATE ON dv_stage_table
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- stage table columns
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_stage_table_column...';
END
$$;

CREATE SEQUENCE dv_stage_table_column_key_seq START 1;

CREATE TABLE dv_stage_table_column
(
  column_key       INTEGER                  DEFAULT nextval(
      'dv_stage_table_column_key_seq' :: REGCLASS) PRIMARY KEY                                             NOT NULL,
  stage_table_key  INTEGER                                                                                 NOT NULL,
  column_name      VARCHAR(128)                                                                            NOT NULL,
  column_type      VARCHAR(30)                                                                             NOT NULL,
  column_length    INTEGER,
  column_precision INTEGER,
  column_scale     INTEGER,
  collation_name   VARCHAR(128),
  is_source_date   BOOLEAN DEFAULT FALSE                                                                   NOT NULL,
  discard_flag     BOOLEAN DEFAULT FALSE                                                                   NOT NULL,
  is_retired       BOOLEAN DEFAULT FALSE                                                                   NOT NULL,
  release_key      INTEGER DEFAULT 1                                                                       NOT NULL,
  owner_key        INTEGER DEFAULT 1                                                                       NOT NULL,
  version_number   INTEGER DEFAULT 1                                                                       NOT NULL,
  updated_by       VARCHAR(50) DEFAULT current_user                                                        NOT NULL,
  updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_stage_table_column_dv_stage_table FOREIGN KEY (stage_table_key) REFERENCES dv_stage_table (stage_table_key),
  CONSTRAINT fk_dv_stage_table_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_stage_table_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_stage_table_column_unq
  ON dv_stage_table_column (owner_key, stage_table_key, column_name);

-- audit
CREATE TRIGGER dv_stage_table_column_audit
AFTER UPDATE ON dv_stage_table_column
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();


/*----------- business rule capture ------------ */

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_business_rule...';
END
$$;

CREATE SEQUENCE dv_business_rule_key_seq START 1;

CREATE TABLE dv_business_rule
(
  business_rule_key   INTEGER                  DEFAULT nextval(
      'dv_business_rule_key_seq' :: REGCLASS) PRIMARY KEY                                           NOT NULL,
  stage_table_key     INTEGER                                                                       NOT NULL,
  business_rule_name  VARCHAR(128)                                                                  NOT NULL,
  business_rule_type  VARCHAR(20) DEFAULT 'internal_sql' :: CHARACTER VARYING                       NOT NULL,
  business_rule_logic TEXT                                                                          NOT NULL,
  load_type           VARCHAR(50)                                                                   NOT NULL,
  is_external         BOOLEAN DEFAULT FALSE                                                         NOT NULL,
  is_retired          BOOLEAN DEFAULT FALSE                                                         NOT NULL,
  release_key         INTEGER DEFAULT 1                                                             NOT NULL,
  owner_key           INTEGER DEFAULT 1                                                             NOT NULL,
  version_number      INTEGER DEFAULT 1                                                             NOT NULL,
  updated_by          VARCHAR(50) DEFAULT "current_user"()                                          NOT NULL,
  updated_datetime    TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_business_rule_dv_stage_table FOREIGN KEY (stage_table_key) REFERENCES dv_stage_table (stage_table_key),
  CONSTRAINT fk_dv_business_rule_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_business_rule_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_business_rule_key_unq
  ON dv_business_rule (owner_key, stage_table_key, business_rule_name, load_type);

-- audit
CREATE TRIGGER dv_business_rule_audit
AFTER UPDATE ON dv_business_rule
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- hub config
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_hub...';
END
$$;

CREATE SEQUENCE dv_hub_key_seq START 1;

CREATE TABLE dv_hub
(
  hub_key          INTEGER                  DEFAULT nextval(
      'dv_hub_key_seq' :: REGCLASS) PRIMARY KEY                                                             NOT NULL,
  hub_name         VARCHAR(128)                                                                             NOT NULL,
  hub_schema       VARCHAR(128)                                                                             NOT NULL,
  is_retired       BOOLEAN DEFAULT FALSE                                                                    NOT NULL,
  release_key      INTEGER DEFAULT 1                                                                        NOT NULL,
  owner_key        INTEGER DEFAULT 1                                                                        NOT NULL,
  version_number   INTEGER DEFAULT 1                                                                        NOT NULL,
  updated_by       VARCHAR(50) DEFAULT current_user                                                         NOT NULL,
  updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_hub_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_hub_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_hub_unq
  ON dv_hub (owner_key, hub_schema, hub_name);

-- audit
CREATE TRIGGER dv_hub_audit
AFTER UPDATE ON dv_hub
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- hub key config
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_hub_key_column...';
END
$$;

CREATE SEQUENCE dv_hub_key_column_key_seq START 1;

CREATE TABLE dv_hub_key_column
(
  hub_key_column_key       INTEGER                  DEFAULT nextval(
      'dv_hub_key_column_key_seq' :: REGCLASS) PRIMARY KEY                                              NOT NULL,
  hub_key                  INTEGER                                                                      NOT NULL,
  hub_key_column_name      VARCHAR(128)                                                                 NOT NULL,
  hub_key_column_type      VARCHAR(30)                                                                  NOT NULL,
  hub_key_column_length    INTEGER,
  hub_key_column_precision INTEGER,
  hub_key_column_scale     INTEGER,
  hub_key_collation_name   VARCHAR(128),
  hub_key_ordinal_position INTEGER DEFAULT 0                                                            NOT NULL,
  release_key              INTEGER DEFAULT 1                                                            NOT NULL,
  owner_key                INTEGER DEFAULT 1                                                            NOT NULL,
  version_number           INTEGER DEFAULT 1                                                            NOT NULL,
  updated_by               VARCHAR(50) DEFAULT current_user                                             NOT NULL,
  updated_datetime         TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_hub_key_column_dv_hub FOREIGN KEY (hub_key) REFERENCES dv_hub (hub_key),
  CONSTRAINT fk_dv_hub_key_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_hub_key_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_key_column_unq
  ON dv_hub_key_column (owner_key, hub_key, hub_key_column_name);

-- audit
CREATE TRIGGER dv_hub_key_column_audit
AFTER UPDATE ON dv_hub_key_column
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- hub column config

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_hub_column...';
END
$$;
CREATE SEQUENCE dv_hub_column_key_seq START 1;

CREATE TABLE dv_hub_column
(
  hub_column_key     INTEGER                  DEFAULT nextval(
      'dv_hub_column_key_seq' :: REGCLASS) PRIMARY KEY                                                         NOT NULL,
  hub_key_column_key INTEGER                                                                                   NOT NULL,
  column_key         INTEGER                                                                                   NOT NULL,
  release_key        INTEGER DEFAULT 1                                                                         NOT NULL,
  owner_key          INTEGER DEFAULT 1                                                                         NOT NULL,
  version_number     INTEGER DEFAULT 1                                                                         NOT NULL,
  updated_by         VARCHAR(50) DEFAULT current_user                                                          NOT NULL,
  updated_datetime   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_hub_column_dv_hub_key_column FOREIGN KEY (hub_key_column_key) REFERENCES dv_hub_key_column (hub_key_column_key),
  CONSTRAINT fk_dv_hub_column_dv_column FOREIGN KEY (column_key) REFERENCES dv_stage_table_column (column_key),
  CONSTRAINT fk_dv_hub_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_hub_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_column_unq
  ON dv_hub_column (owner_key, hub_key_column_key, column_key);


-- audit
CREATE TRIGGER dv_hub_column_audit
AFTER UPDATE ON dv_hub_column
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- config satellite

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_satellite...';
END
$$;

CREATE SEQUENCE dv_satellite_key_seq START 1;

CREATE TABLE dv_satellite
(
  satellite_key           INTEGER                  DEFAULT nextval(
      'dv_satellite_key_seq' ::
      REGCLASS) PRIMARY KEY                                                                                               NOT NULL,
  hub_key                 INTEGER DEFAULT 0                                                                               NOT NULL,
  link_key                INTEGER DEFAULT 0                                                                               NOT NULL,
  link_hub_satellite_flag CHAR DEFAULT 'H' :: bpchar                                                                      NOT NULL,
  satellite_name          VARCHAR(128)                                                                                    NOT NULL,
  satellite_schema        VARCHAR(128)                                                                                    NOT NULL,
  is_retired              BOOLEAN DEFAULT FALSE                                                                           NOT NULL,
  release_key             INTEGER DEFAULT 1                                                                               NOT NULL,
  owner_key               INTEGER DEFAULT 1                                                                               NOT NULL,
  version_number          INTEGER DEFAULT 1                                                                               NOT NULL,
  updated_by              VARCHAR(50) DEFAULT current_user                                                                NOT NULL,
  updated_datetime        TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_satellite_dv_hub FOREIGN KEY (hub_key) REFERENCES dv_hub (hub_key),
  CONSTRAINT fk_dv_satellite_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_satellite_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_satellite_unq
  ON dv_satellite (owner_key, satellite_schema, satellite_name);

-- audit
CREATE TRIGGER dv_satellite_audit
AFTER UPDATE ON dv_satellite
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- configure sat columns and link them with source columns

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_satellite_column...';
END
$$;

CREATE SEQUENCE dv_satellite_column_key_seq START 1;

CREATE TABLE dv_satellite_column
(
  satellite_column_key INTEGER                  DEFAULT nextval(
      'dv_satellite_column_key_seq' :: REGCLASS) PRIMARY KEY                                          NOT NULL,
  satellite_key        INTEGER                                                                        NOT NULL,
  column_key           INTEGER                                                                        NOT NULL,
  release_key          INTEGER DEFAULT 1                                                              NOT NULL,
  owner_key            INTEGER DEFAULT 1                                                              NOT NULL,
  version_number       INTEGER DEFAULT 1                                                              NOT NULL,
  updated_by           VARCHAR(50) DEFAULT current_user                                               NOT NULL,
  updated_datetime     TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_satellite_column_dv_satellite FOREIGN KEY (satellite_key) REFERENCES dv_satellite (satellite_key),
  CONSTRAINT fk_dv_satellite_column_dv_stage_table_column FOREIGN KEY (column_key) REFERENCES dv_stage_table_column (column_key),
  CONSTRAINT fk_dv_satellite_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_satellite_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)

);
CREATE UNIQUE INDEX dv_satellite_column_unq
  ON dv_satellite_column (owner_key, satellite_key, column_key);


-- audit
CREATE TRIGGER dv_satellite_column_audit
AFTER UPDATE ON dv_satellite_column
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

-- column type
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_column_type...';
END
$$;

CREATE TYPE dv_column_type AS
(
  column_name      VARCHAR(128),
  column_type      VARCHAR(50),
  column_length    INT,
  column_precision INT,
  column_scale     INT,
  is_nullable      INT,
  is_key           INT,
  is_indexed       INT
);

/************************************** config  functions ************************************************************/
DO $$
BEGIN
  RAISE NOTICE 'Creating fn_build_column_definition...';
END
$$;

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

  RAISE NOTICE 'Column defenition % -->', result_v;

  RETURN result_v;

END
$BODY$
LANGUAGE plpgsql;


DO $$
BEGIN
  RAISE NOTICE 'Creating fn_get_dv_object_default_columns...';
END
$$;
-- function for getting set of default columns for data vault object
CREATE OR REPLACE FUNCTION fn_get_dv_object_default_columns(object_name_in        VARCHAR(128),
                                                            object_type_in        VARCHAR(128),
                                                            object_column_type_in VARCHAR(30) DEFAULT NULL -- all or particular type
)
  RETURNS SETOF dv_column_type AS
$BODY$
DECLARE
  r dv_column_type%ROWTYPE;
BEGIN

  -- check parameter
  IF COALESCE(object_type_in, '') NOT IN ('hub', 'link', 'satellite', 'stage_table')
  THEN
    RAISE NOTICE 'Not valid object type: can be only hub, satellite --> %', object_type_in;
    RETURN;
  END IF;

  FOR r IN (SELECT
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN rtrim(coalesce(column_prefix, '') || replace(d.column_name, '%', object_name_in) ||
                           coalesce(column_suffix, ''))
              ELSE d.column_name END        AS column_name,

              column_type,
              column_length,
              column_precision,
              column_scale,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 0
              ELSE 1 END                    AS is_nullable,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 1
              ELSE 0 END                    AS is_key,
              cast(d.is_indexed AS INTEGER) AS is_indexed
            FROM dv_default_column d
            WHERE object_type = object_type_in
                  AND (d.object_column_type = object_column_type_in OR object_column_type_in IS NULL)
            ORDER BY is_key DESC) LOOP
    RETURN NEXT r;
  END LOOP;
  RETURN;
END
$BODY$
LANGUAGE 'plpgsql';

-- get object name

DO $$
BEGIN
  RAISE NOTICE 'Creating fn_get_object_name...';
END
$$;
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

/********************************************config setup *************************************************************/
/*------------------ ADD OBJECT INTO CONFIG ----------------*/

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_object_insert...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_object_insert
  (
    object_type_in     VARCHAR(100), -- table name in a list of
    object_settings_in VARCHAR [] [2]-- array parameters to insert column_name -> value
  )
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v      INTEGER :=0;
  column_name_v   VARCHAR(50);
  column_value_v  VARCHAR;
  column_type_v   VARCHAR(30);
  release_key_v   INT;
  owner_key_v     INT;
  sql_v           text;
  sql_select_v    text;
  array_length_v  INT;
  counter_v       INT;
  delimiter_v     CHAR(10) = ' , ';
  object_schema_v VARCHAR(50) :='ore_config';
BEGIN

  rowcount_v:=0;
  -- check if object exists
  SELECT count(*)
  INTO rowcount_v
  FROM information_schema.tables t
  WHERE t.table_schema = object_schema_v AND t.table_name = object_type_in;

  IF rowcount_v = 0
  THEN
    RAISE NOTICE 'Not valid object type --> %', object_type_in;
    RETURN rowcount_v;
  END IF;

  -- if there any columns to update
  array_length_v:=array_length(object_settings_in, 1);

  IF array_length_v = 0
  THEN
    RAISE NOTICE 'Nothing to insert, check parameters --> %', object_settings_in;
    RETURN array_length_v;
  END IF;

  -- return list of columns except from key column and audit columns
  -- also fine if column omitted and nullable
  -- need checking release and owner integrity
  DROP TABLE IF EXISTS columns_list_tmp;

  CREATE TEMP TABLE columns_list_tmp ON COMMIT DROP AS
    SELECT
      column_name,
      data_type,
      is_nullable,
      is_found,
      column_default
    FROM
      (
        SELECT
          c.column_name,
          c.is_nullable,
          replace(c.data_type, 'character varying', 'varchar') AS data_type,
          CASE WHEN c.column_name = kcu.column_name
            THEN 1
          ELSE NULL END                                        AS is_key,
          cast(0 AS INTEGER)                                   AS is_found,
          column_default
        FROM information_schema.tables t
          JOIN information_schema.columns c
            ON t.table_schema = c.table_schema AND t.table_name = c.table_name
          LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON tc.table_catalog = t.table_catalog
               AND tc.table_schema = t.table_schema
               AND tc.table_name = t.table_name
               AND tc.constraint_type = 'PRIMARY KEY'
          LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON kcu.table_catalog = tc.table_catalog
               AND kcu.table_schema = tc.table_schema
               AND kcu.table_name = tc.table_name
               AND kcu.constraint_name = tc.constraint_name
        WHERE t.table_schema = object_schema_v AND t.table_name = object_type_in
              AND c.column_name NOT IN ('updated_by', 'updated_datetime')) r
    WHERE is_key IS NULL;

  -- insert statement
  sql_v:='insert into '
         || ' '
         || quote_ident(object_type_in)
         || ' ( ';

  sql_select_v:=' select ';

  -- check parameters
  -- in case some incorrect parameters
  counter_v:=0;
  -- columns to lookup in information schema
  FOR i IN 1..array_length_v LOOP

    -- checking release_number and owner_key existance
    -- excluded for dv_release

    IF object_settings_in [i] [1] = 'release_number' AND object_type_in NOT IN ('dv_release')
    THEN
      SELECT release_key
      INTO release_key_v
      FROM dv_release
      WHERE release_number = cast(object_settings_in [i] [2] AS INTEGER);

      GET DIAGNOSTICS rowcount_v = ROW_COUNT;

      IF rowcount_v = 0
      THEN
        RAISE NOTICE 'Nonexistent release_number --> %', object_settings_in [i] [2];

        RETURN rowcount_v;
      END IF;
    END IF;


    IF object_settings_in [i] [1] = 'owner_key' AND
       object_type_in NOT IN ('dv_owner', 'dv_default_column')
    THEN
      SELECT owner_key
      INTO owner_key_v
      FROM dv_owner
      WHERE owner_key = cast(object_settings_in [i] [2] AS INTEGER);

      GET DIAGNOSTICS rowcount_v = ROW_COUNT;

      IF rowcount_v = 0
      THEN
        RAISE NOTICE 'Nonexistent owner_key --> %', object_settings_in [i] [2];
        RETURN rowcount_v;
      END IF;

    END IF;

    -- lookup columns in temp table
    -- release number processed differently for 2 cases - dv_release and the rest
    SELECT
      column_name,
      data_type,
      CASE WHEN column_name = 'release_key'
        THEN cast(release_key_v AS VARCHAR)
      ELSE object_settings_in [i] [2] END
    INTO column_name_v, column_type_v, column_value_v
    FROM columns_list_tmp
    WHERE column_name = CASE WHEN object_type_in <> 'dv_release'
      THEN replace(object_settings_in [i] [1], 'release_number', 'release_key')
                        ELSE object_settings_in [i] [1] END;

    GET DIAGNOSTICS rowcount_v = ROW_COUNT;

    IF rowcount_v > 0
    THEN
      counter_v:=counter_v + 1;
      IF counter_v > 1
      THEN
        sql_v:=sql_v || delimiter_v;
        sql_select_v:=sql_select_v || delimiter_v;
      END IF;

      -- sql list of columns
      sql_v:=sql_v || quote_ident(column_name_v);
      -- sql list of values

      IF column_value_v IS NULL
      THEN

        sql_select_v:=sql_select_v || 'NULL';

      ELSE


        sql_select_v:=sql_select_v
                      || ' cast('
                      || quote_literal(column_value_v)
                      || ' as '
                      || column_type_v
                      || ')';

      END IF;

      /*
      sql_select_v:=sql_select_v
                    || ' cast('
                    || quote_literal(column_value_v)
                    || ' as '
                    || column_type_v
                    || ')';

      */
      -- update if column was found
      UPDATE columns_list_tmp
      SET is_found = 1
      WHERE column_name = column_name_v;
    END IF;

  END LOOP;


  counter_v:=0;
  -- check number of 'not found must' parameters for insert
  SELECT count(*)
  INTO counter_v
  FROM columns_list_tmp
  WHERE is_nullable = 'NO' AND column_default IS NULL AND is_found = 0;


  IF counter_v > 0
  THEN
    RAISE NOTICE 'Not all parameters found --> %', object_settings_in;
    RETURN 0;
  ELSE

    sql_v:=sql_v || ') ' || sql_select_v;

    EXECUTE sql_v;
    RAISE NOTICE 'SQL --> %', sql_v;
  END IF;

  -- check if something actually been deleted
  GET DIAGNOSTICS rowcount_v = ROW_COUNT;

  RETURN rowcount_v;

END
$BODY$
LANGUAGE plpgsql;


/*-------------- UPDATE CONFIG OBJECT DETAILS --------------------- */

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_object_update...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_object_update
  (
    object_type_in     VARCHAR(100), -- config table name
    object_key_in      INTEGER, -- object key
    object_settings_in VARCHAR [] [2]-- array parameters to update column_name -> value
  )
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v         INTEGER :=0;
  key_column_v       VARCHAR(50);
  column_name_v      VARCHAR(50);
  column_data_type_v VARCHAR(50);
  sql_v              VARCHAR(2000);
  array_length_v     INT;
  counter_v          INT;
  delimiter_v        CHAR(10) = ' , ';
  object_schema_v    VARCHAR(50) :='ore_config';
BEGIN

  rowcount_v:=0;

  -- list of all columns
  CREATE TEMP TABLE columns_list_tmp ON COMMIT DROP AS
    SELECT
      column_name,
      data_type,
      is_key,
      is_no_update
    FROM
      (
        SELECT
          c.column_name,
          c.is_nullable,
          replace(c.data_type, 'character varying', 'varchar') AS data_type,
          CASE WHEN c.column_name = kcu.column_name
            THEN 1
          ELSE 0 END                                           AS is_key,
          CASE WHEN c.column_name IN ('updated_by', 'updated_datetime')
            THEN 1
          ELSE 0 END                                              is_no_update
        FROM information_schema.tables t
          JOIN information_schema.columns c
            ON t.table_schema = c.table_schema AND t.table_name = c.table_name
          LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON tc.table_catalog = t.table_catalog
               AND tc.table_schema = t.table_schema
               AND tc.table_name = t.table_name
               AND tc.constraint_type = 'PRIMARY KEY'
          LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON kcu.table_catalog = tc.table_catalog
               AND kcu.table_schema = tc.table_schema
               AND kcu.table_name = tc.table_name
               AND kcu.constraint_name = tc.constraint_name
        WHERE t.table_schema = object_schema_v AND t.table_name = object_type_in
      ) r;

  -- check if object exists
  SELECT column_name
  INTO key_column_v
  FROM columns_list_tmp
  WHERE is_key = 1;

  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  IF rowcount_v = 0
  THEN
    RAISE NOTICE 'Not valid object type --> %', object_type_in;
  ELSE
    -- update statement
    sql_v:='update '
           || ' '
           || quote_ident(object_type_in)
           || ' set ';

    -- if there are any columns to update
    array_length_v:=array_length(object_settings_in, 1);

    -- here should be cycle for all columns to be updated
    IF array_length_v = 0
    THEN
      RAISE NOTICE 'Nothing to update, check parameters --> %', object_settings_in;
    ELSE
      -- in case some incorrect parameters
      counter_v:=0;
      -- all columns to lookup in information schema
      FOR i IN 1..array_length_v LOOP

        -- excluding key and audit parameters for update
        SELECT
          column_name,
          data_type
        INTO column_name_v, column_data_type_v
        FROM columns_list_tmp
        WHERE is_key = 0 AND is_no_update = 0 AND column_name = object_settings_in [i] [1];

        GET DIAGNOSTICS rowcount_v = ROW_COUNT;


        IF rowcount_v > 0
        THEN
          counter_v:=counter_v + 1;
          IF counter_v > 1
          THEN
            sql_v:=sql_v || delimiter_v;
          END IF;

          sql_v:=sql_v || quote_ident(column_name_v)
                 || '='
                 || ' cast('
                 || quote_literal(object_settings_in [i] [2])
                 || ' as '
                 || column_data_type_v
                 || ')';
        END IF;
      END LOOP;

      IF counter_v > 0
      THEN
        sql_v:=sql_v || ' where '
               || quote_ident(key_column_v)
               || '='
               || quote_literal(cast(object_key_in AS INT));

        RAISE NOTICE 'SQL --> %', sql_v;

        EXECUTE sql_v;
      ELSE
        RAISE NOTICE 'Nothing to update or parameters prohibited to update, check parameters --> %', object_settings_in;
      END IF;
    END IF;

  END IF;
  -- check if something actually been deleted
  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  RETURN rowcount_v;

END
$BODY$
LANGUAGE plpgsql;

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_object_delete...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_object_delete
  (
    object_type_in VARCHAR(100), -- table name in a list of
    object_key_in  INTEGER
  )
  RETURNS INT AS
$BODY$
DECLARE
  rowcount_v      INTEGER :=0;
  key_column_v    VARCHAR(50);
  sql_v           VARCHAR(2000);
  object_schema_v VARCHAR(50) :='ore_config';
BEGIN

  -- check if table exists
  rowcount_v:=0;

  -- find primary key column and check if object exists
  SELECT kcu.column_name
  INTO key_column_v
  FROM INFORMATION_SCHEMA.TABLES t
    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      ON tc.table_catalog = t.table_catalog
         AND tc.table_schema = t.table_schema
         AND tc.table_name = t.table_name
         AND tc.constraint_type = 'PRIMARY KEY'
    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      ON kcu.table_catalog = tc.table_catalog
         AND kcu.table_schema = tc.table_schema
         AND kcu.table_name = tc.table_name
         AND kcu.constraint_name = tc.constraint_name
  WHERE
    t.table_schema = object_schema_v AND t.table_name = object_type_in;

  GET DIAGNOSTICS rowcount_v = ROW_COUNT;
  IF rowcount_v = 0
  THEN
    RAISE NOTICE 'Not valid object type --> %', object_type_in;
  ELSE
    -- delete record
    EXECUTE 'delete from '
            || ' '
            || quote_ident(object_type_in)
            || ' where '
            || quote_ident(key_column_v)
            || '='
            || quote_literal(object_key_in);
  END IF;

  -- check if something actually been deleted
  GET DIAGNOSTICS rowcount_v = ROW_COUNT;

  RETURN rowcount_v;

END
$BODY$
LANGUAGE plpgsql;

/*************************************** data vault setup *************************************************************/

-- generic function for table creation ddl using set of columns passed as ref cursor

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_dv_table_create...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_dv_table_create(
  object_name_in    VARCHAR(128),
  object_schema_in  VARCHAR(128),
  object_columns_in REFCURSOR,
  recreate_flag_in  CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_create_v       TEXT;
  sql_key_v          TEXT;
  sql_col_def_v      TEXT;
  sql_index_v        TEXT :='';
  sql_drop_v         TEXT :='';
  crlf_v             CHAR(10) :=' ';
  delimiter_v        CHAR(2) :=',';
  newline_v          CHAR(3) :=E'\n';
  cnt_rec_v          INT;
  rec                dv_column_type;
  i                  INT :=0;
  sql_create_index_v TEXT;

BEGIN

  -- check if parameters correct
  -- schema exists
  -- object_type correct satellite, hub, stage_table
  RAISE NOTICE 'SQL --> %', sql_create_v;

  IF COALESCE(object_name_in, '') = ''
  THEN
    RAISE NOTICE 'Not valid object name --> %', object_name_in;
    RETURN rowcount_v;
  END IF;
  IF COALESCE(object_schema_in, '') = ''
  THEN
    RAISE NOTICE 'Not valid object schema--> %', object_schema_in;
    RETURN rowcount_v;
  END IF;

  -- check parameters

  IF COALESCE(recreate_flag_in, '') NOT IN ('Y', 'N')
  THEN
    RAISE NOTICE 'Not valid recreate_flag value : Y or N --> %', recreate_flag_in;
    RETURN rowcount_v;
  END IF;

  -- if recreate flag set to Yes then drop / the same if set to No and object exists

  IF recreate_flag_in = 'Y'  -- OR (recreate_flag_in = 'N' AND rowcount_v = 1)
  THEN
    IF rowcount_v = 1
    THEN
      sql_drop_v:='DROP TABLE ' || object_schema_in || '.' || object_name_in || ';' || newline_v;
      /*  ELSE
          RAISE NOTICE 'Can not drop table, object does not exist  --> %', object_schema_in || '.' || object_name_in;
          RETURN rowcount_v;*/
    END IF;

  END IF;

  -- build create statement
  sql_create_v:='create table ' || object_schema_in || '.' || object_name_in || newline_v || '(' || newline_v;
  sql_create_index_v:=
  'create unique index ux_' || object_name_in || '_' || public.uuid_generate_v4() || newline_v || ' on ' ||
  object_schema_in || '.' || object_name_in || newline_v || '(';
  sql_create_index_v:=replace(sql_create_index_v, '-', '_');

  -- column definitions
  -- open cursor

  WHILE TRUE
  LOOP
    FETCH $3 INTO rec;
    EXIT WHEN NOT found;
    -- add key column

    SELECT fn_build_column_definition(rec)
    INTO sql_col_def_v;

    sql_create_v:=sql_create_v || crlf_v || sql_col_def_v || crlf_v || delimiter_v || newline_v;

    -- add index
    IF rec.is_indexed = 1
    THEN
      sql_create_index_v:=sql_create_index_v || rec.column_name || delimiter_v;
      i:=i + 1;
    END IF;

    RAISE NOTICE '%', rec;
  END LOOP;

  -- remove last comma
  sql_create_v:=substring(sql_create_v FROM 1 FOR length(sql_create_v) - 2) || newline_v || ');' || newline_v;

  IF i <> 0
  THEN
    sql_create_index_v:=substring(sql_create_index_v FROM 1 FOR length(sql_create_index_v) - 1) || newline_v || ');' ||
                        newline_v;
    sql_create_v:=sql_create_v || sql_create_index_v;
  END IF;

  RAISE NOTICE 'SQL --> %', sql_create_v;

  RETURN sql_drop_v || sql_create_v;
END
$BODY$
LANGUAGE plpgsql;

-- create hub table
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_dv_create_hub...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_dv_create_hub(
  object_name_in   VARCHAR(128),
  object_schema_in VARCHAR(128),
  recreate_flag_in CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_v              TEXT;
  sql_create_table_v TEXT;
  sql_create_index_v TEXT;
  hub_name_v         VARCHAR(200);

    rec CURSOR FOR SELECT *
                   FROM fn_get_dv_object_default_columns(object_name_in, 'hub')
                   UNION ALL
                   SELECT
                     hkc.hub_key_column_name      AS column_name,
                     hkc.hub_key_column_type      AS column_type,
                     hkc.hub_key_column_length    AS column_length,
                     hkc.hub_key_column_precision AS column_precision,
                     hkc.hub_key_column_scale     AS column_scale,
                     1                            AS is_nullable,
                     0                            AS is_key,
                     1                            AS is_indexed
                   FROM ore_config.dv_hub h
                     INNER JOIN ore_config.dv_hub_key_column hkc
                       ON h.hub_key = hkc.hub_key
                   WHERE h.hub_schema = object_schema_in
                         AND h.hub_name = object_name_in
  ;
BEGIN

  OPEN rec;

  -- create statement
  -- generate hub name
  hub_name_v:=fn_get_object_name(object_name_in, 'hub');

  SELECT ore_config.dv_config_dv_table_create(hub_name_v,
                                              object_schema_in,
                                              'rec',
                                              recreate_flag_in
  )
  INTO sql_create_table_v;

  CLOSE rec;


  RETURN sql_create_table_v;

END
$BODY$
LANGUAGE 'plpgsql';

-- create satellite
DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_dv_create_satellite...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_dv_create_satellite(
  object_name_in   VARCHAR(128),
  object_schema_in VARCHAR(128),
  object_type_in   CHAR(1) DEFAULT 'H',
  recreate_flag_in CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_v              TEXT;
  sql_create_table_v TEXT;
  sql_create_index_v TEXT;
  sat_name_v         VARCHAR(200);
  hub_name_v         VARCHAR(30);

  -- get columns
    rec CURSOR (hub_name VARCHAR ) FOR SELECT *
                                       FROM fn_get_dv_object_default_columns(object_name_in, 'satellite')
                                       UNION ALL
                                       -- get hub surrogate key columns
                                       SELECT
                                         column_name,
                                         column_type,
                                         column_length,
                                         column_precision,
                                         column_scale,
                                         1 AS is_nullable,
                                         0 AS is_key,
                                         1 AS is_indexed
                                       FROM fn_get_dv_object_default_columns(hub_name, 'hub', 'Object_Key')
                                       UNION ALL
                                       SELECT
                                         stc.column_name      AS column_name,
                                         stc.column_type      AS column_type,
                                         stc.column_length    AS column_length,
                                         stc.column_precision AS column_precision,
                                         stc.column_scale     AS column_scale,
                                         1                    AS is_nullable,
                                         0                    AS is_key,
                                         0                    AS is_indexed

                                       FROM ore_config.dv_satellite s
                                         INNER JOIN ore_config.dv_satellite_column sc
                                           ON s.satellite_key = sc.satellite_key
                                         JOIN dv_stage_table_column stc ON sc.column_key = stc.column_key
                                       WHERE s.satellite_schema = object_schema_in
                                             AND s.satellite_name = object_name_in;
BEGIN

  -- it will be easy to find link name as well using the same query for column definition selection
  -- just add 2d parameter to cursor, type of object : link or hub
  -- find related hub name
  SELECT h.hub_name
  INTO hub_name_v
  FROM dv_satellite s
    JOIN dv_hub h ON s.hub_key = h.hub_key
  WHERE s.satellite_name = object_name_in AND s.satellite_schema = object_schema_in
        AND s.link_hub_satellite_flag = 'H';

  -- generate satellite name
  sat_name_v:=fn_get_object_name(object_name_in, 'satellite');

  RAISE NOTICE ' Sat name %-->', sat_name_v;
  RAISE NOTICE ' Hub name %-->', hub_name_v;

  OPEN rec (hub_name:=hub_name_v);
  -- create statement


  SELECT ore_config.dv_config_dv_table_create(sat_name_v,
                                              object_schema_in,
                                              'rec',
                                              recreate_flag_in
  )
  INTO sql_create_table_v;

  CLOSE rec;

  RETURN sql_create_table_v;

END
$BODY$
LANGUAGE 'plpgsql';

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_dv_create_stage_table...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_dv_create_stage_table(
  object_name_in   VARCHAR(128),
  object_schema_in VARCHAR(128),
  recreate_flag_in CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_v              TEXT;
  sql_create_table_v TEXT;
  sql_create_index_v TEXT;
    rec CURSOR FOR
    SELECT *
    FROM fn_get_dv_object_default_columns(object_name_in, 'stage_table')
    UNION ALL
    SELECT
      sc.column_name,
      sc.column_type,
      sc.column_length,
      sc.column_precision,
      sc.column_scale,
      0 AS is_nullable,
      0 AS is_key,
      0 AS is_indexed
    FROM dv_stage_table t
      INNER JOIN dv_stage_table_column sc
        ON t.stage_table_key = sc.stage_table_key
    WHERE t.stage_table_schema = object_schema_in
          AND t.stage_table_name = object_name_in

  ;
BEGIN

  OPEN rec;

  -- create statement

  SELECT ore_config.dv_config_dv_table_create(object_name_in,
                                              object_schema_in,
                                              'rec',
                                              recreate_flag_in
  )
  INTO sql_create_table_v;

  CLOSE rec;

  RETURN sql_create_table_v;

END
$BODY$
LANGUAGE 'plpgsql';


/*****************************************data vault orchestration engine**********************************************/

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_dv_load_hub...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_dv_load_hub(stage_table_schema_in CHARACTER VARYING,
                                                 stage_table_name_in   CHARACTER VARYING,
                                                 hub_schema_in         CHARACTER VARYING, hub_name_in CHARACTER VARYING)
  RETURNS TEXT
LANGUAGE plpgsql
AS $fun$
DECLARE
  sql_block_start_v    TEXT :='';
  sql_block_end_v      TEXT :='';
  sql_block_body_v     TEXT;
  sql_process_start_v  TEXT :='';
  sql_process_finish_v TEXT :='';
  delimiter_v          CHAR(2) :=',';
  newline_v            CHAR(3) :=E'\n';
  load_date_time_v     VARCHAR(10) :='now()';
  hub_name_v           VARCHAR(50);
BEGIN
  /*-----TO DO add error handling generation if load failed checks on counts
    */

  -- hub name check
  hub_name_v:= fn_get_object_name(hub_name_in, 'hub');

  IF COALESCE(hub_name_v, '') = ''
  THEN
    RAISE NOTICE 'Not valid hub name --> %', hub_name_in;
    RETURN NULL;
  END IF;

  -- code snippets
  /* sql_block_start_v:='DO $$' || newline_v || 'begin' || newline_v;
   sql_block_end_v:=newline_v || 'end$$;';

   -- update processing status stage
   sql_process_start_v:=
   'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' || quote_literal('PROCESSING') ||
   ' where status=' ||
   quote_literal('RAW')||';'||newline_v;
   sql_process_finish_v:=
   newline_v || 'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' ||
   quote_literal('PROCESSED') || ' where status=' ||
   quote_literal('PROCESSING')||';'||newline_v;

 */

  -- dynamic upsert statement
  -- add process status select-update in transaction
  WITH sql AS (
    SELECT
      stc.column_name            stage_col_name,
      hkc.hub_key_column_name    hub_col_name,
      hkc.hub_key_column_type AS column_type,
      1                       AS is_bk
    FROM dv_stage_table st
      JOIN dv_stage_table_column stc ON st.stage_table_key = stc.stage_table_key
      JOIN dv_hub_column hc ON hc.column_key = stc.column_key
      JOIN dv_hub_key_column hkc ON hc.hub_key_column_key = hkc.hub_key_column_key
      JOIN dv_hub H ON hkc.hub_key = H.hub_key
    WHERE COALESCE(stc.is_retired, CAST(0 AS BOOLEAN)) <> CAST(1 AS BOOLEAN)
          AND stage_table_schema = stage_table_schema_in
          AND stage_table_name = stage_table_name_in
          AND h.hub_name = hub_name_in
          AND H.hub_schema = hub_schema_in
          AND st.owner_key = h.owner_key
    UNION ALL
    -- get defaults
    SELECT
      CASE WHEN column_name = 'dv_load_date_time'
        THEN load_date_time_v
      ELSE quote_literal(stage_table_schema_in || '.' || stage_table_name_in)
      END         AS stage_col_name,
      column_name AS hub_col_name,
      column_type,
      0           AS is_bk
    FROM fn_get_dv_object_default_columns(hub_name_in, 'hub')
    WHERE is_key = 0
  )
  SELECT array_to_string(array_agg(t.ssql), E'\n')
  FROM (
         SELECT 'with src as ' || '( select ' ||
                array_to_string(array_agg('cast(' || sql.stage_col_name || ' as ' || sql.column_type || ')'),
                                ', ') AS ssql
         FROM sql
         UNION ALL
         SELECT DISTINCT
           ' from ' || stage_table_schema_in || '.' || stage_table_name_in || ' where dv_process_status=' ||
           quote_literal('PROCESSING') || ')'
         FROM sql
         UNION ALL
         SELECT 'insert into ' || hub_schema_in || '.' || hub_name_v || '(' ||
                array_to_string(array_agg(sql.hub_col_name), ', ') || ')'
         FROM sql
         -- GROUP BY sql.hub_schema, fn_get_object_name(sql.hub_name, 'hub')
         UNION ALL
         SELECT DISTINCT
           'select * from src' || E'\n' || 'on conflict(' || array_to_string(array_agg(sql.hub_col_name), ', ') ||
           ') ' || 'do nothing;' || E'\n'
         FROM sql
         WHERE is_bk = 1) t
  INTO sql_block_body_v;

  RETURN sql_block_start_v || sql_process_start_v || sql_block_body_v || sql_process_finish_v || sql_block_end_v;

END
$fun$;


DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_dv_load_satellite...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_dv_load_satellite(stage_table_schema_in CHARACTER VARYING,
                                                       stage_table_name_in   CHARACTER VARYING,
                                                       satellite_schema_in   CHARACTER VARYING,
                                                       satellite_name_in     CHARACTER VARYING,
                                                       load_type_in          CHARACTER VARYING DEFAULT 'delta' :: CHARACTER VARYING)
  RETURNS TEXT
LANGUAGE plpgsql
AS $fun$
DECLARE
  sql_block_start_v    TEXT :='';
  sql_block_end_v      TEXT :='';
  sql_block_body_v     TEXT;
  sql_process_start_v  TEXT :='';
  sql_process_finish_v TEXT :='';
  delimiter_v          CHAR(2) :=',';
  newline_v            CHAR(3) :=E'\n';
  load_time_v          TIMESTAMPTZ;
  hub_name_v           VARCHAR(50);
  hub_schema_v         VARCHAR(50);
  load_date_time_v     VARCHAR(10) :='now()';
  default_enddate_v    VARCHAR(50) :=quote_literal(to_date('01-01-2100 00:00:00', 'dd-mm-yyyy hh24:mi:ss'));
  satellite_name_v     VARCHAR(50);
BEGIN

  -- parameters check
  -- get related hub name
  SELECT
    fn_get_object_name(h.hub_name, 'hub'),
    h.hub_schema
  INTO
    hub_name_v, hub_schema_v
  FROM dv_satellite s
    JOIN dv_hub h ON h.hub_key = s.hub_key AND h.owner_key = s.owner_key
  WHERE s.satellite_schema = satellite_schema_in AND s.satellite_name = satellite_name_in;

  -- get satellite name
  satellite_name_v:=fn_get_object_name(satellite_name_in, 'satellite');

  IF COALESCE(satellite_name_v, '') = '' OR COALESCE(hub_name_v, '') = ''
  THEN
    RAISE NOTICE 'Not valid satellite name --> %', satellite_name_in;
    RETURN NULL;
  END IF;

  -- code snippets
  -- block
  /* sql_block_start_v:='DO $$' || newline_v || 'begin' || newline_v;
   sql_block_end_v:=newline_v || 'end$$;';

   -- update status of records in stage table
   sql_process_start_v:=
   'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' || quote_literal('PROCESSING') ||
   ' where status=' ||
   quote_literal('RAW') || ';' || newline_v;
   sql_process_finish_v:=
   newline_v || 'update ' || stage_table_schema_in || '.' || stage_table_name_in || ' set status=' ||
   quote_literal('PROCESSED') || ' where status=' ||
   quote_literal('PROCESSING') || ';' || newline_v;

 */

  -- dynamic upsert statement
  -- full load means that records for whose keys in staging not found will be marked as deleted
  -- lookup keys in hub
  -- insert records for new keys
  -- update changed records for existing keys, insert new record with changed values

  WITH sql AS (
    -- list of stage table- satellite match and hub key lookup column
    SELECT
      stc.column_name AS stage_col_name,
      stc.column_name AS sat_col_name,
      stc.column_type,
      0               AS is_surrogate_key,
      hkc.hub_key_column_name,
      hkc.hub_key_column_type,
      0                  is_default,
      CASE WHEN hkc.hub_key_column_name IS NOT NULL
        THEN 1
      ELSE 0 END         is_business_key
    FROM dv_stage_table st
      JOIN dv_stage_table_column stc ON st.stage_table_key = stc.stage_table_key
      JOIN dv_satellite_column sc ON sc.column_key = stc.column_key
      JOIN dv_satellite s ON s.satellite_key = sc.satellite_key
      LEFT JOIN (SELECT
                   hkc.hub_key,
                   hkc.owner_key,
                   hkc.hub_key_column_name,
                   hc.column_key,
                   hkc.hub_key_column_type
                 FROM
                   dv_hub_key_column hkc
                   JOIN dv_hub_column hc ON hc.hub_key_column_key = hkc.hub_key_column_key) hkc
        ON hkc.column_key = stc.column_key
           AND s.hub_key = hkc.hub_key AND s.owner_key = hkc.owner_key
    WHERE COALESCE(s.is_retired, CAST(0 AS BOOLEAN)) <> CAST(1 AS BOOLEAN)
          AND stage_table_schema = stage_table_schema_in
          AND stage_table_name = stage_table_name_in
          AND s.satellite_name = satellite_name_in
          AND s.satellite_schema = satellite_schema_in
          AND st.owner_key = s.owner_key
    -- list of default columns
    UNION ALL
    SELECT
      CASE WHEN column_name IN ('dv_source_date_time', 'dv_rowstartdate', 'dv_rowstartdate')
        THEN load_date_time_v
      WHEN column_name = 'dv_record_source'
        THEN quote_literal(stage_table_schema_in || '.' || stage_table_name_in)
      WHEN column_name = 'dv_row_is_current'
        THEN '1'
      WHEN column_name = 'dv_rowenddate'
        THEN default_enddate_v
      ELSE column_name
      END         AS stage_col_name,
      column_name AS hub_col_name,
      column_type,
      0,
      NULL,
      NULL,
      1,
      0
    FROM fn_get_dv_object_default_columns(satellite_name_in, 'satellite')
    WHERE is_key = 0
    -- related hub surrogate key
    UNION ALL
    SELECT
      c.column_name,
      c.column_name AS sat_col_name,
      c.column_type,
      1             AS is_surrogate_key,
      NULL,
      NULL,
      0,
      0
    FROM dv_satellite s
      JOIN dv_hub h ON h.hub_key = s.hub_key
      JOIN fn_get_dv_object_default_columns(h.hub_name, 'hub') c ON 1 = 1
    WHERE s.owner_key = h.owner_key
          AND c.is_key = 1
          AND s.satellite_name = satellite_name_in
          AND s.satellite_schema = satellite_schema_in
  )
  SELECT array_to_string(array_agg(t.ssql), E'\n')
  FROM (
         SELECT 'with src as ' || '( select distinct ' ||
                array_to_string(
                    array_agg(
                        'cast(' || (CASE WHEN sql.is_default = 1
                          THEN ' '
                                    ELSE ' s.' END) || sql.stage_col_name || ' as ' || sql.column_type || ') as ' ||
                        sql.sat_col_name),
                    ', ') AS ssql
         FROM sql
         WHERE sql.is_surrogate_key = 0
         UNION ALL
         SELECT DISTINCT ', h.' || sql.sat_col_name || ' as hub_SK ' || ' from ' || stage_table_schema_in || '.' ||
                         stage_table_name_in
                         || ' as s full join ' || hub_schema_v ||
                         '.' ||
                         hub_name_v ||
                         ' as h '
                         ' on '
         FROM sql
         WHERE sql.is_surrogate_key = 1
         UNION ALL
         SELECT array_to_string(array_agg(' s.' || sql.stage_col_name || '=h.' || sql.hub_key_column_name),
                                ' and ') || ' where s.dv_process_status=' || quote_literal('PROCESSING')
         FROM sql
         WHERE sql.is_business_key = 1
         UNION ALL
         -- except statement : checking source to exclude duplicates in time series
         SELECT ' except  select ' || array_to_string(
             array_agg(
                 'cast(' ||
                 (CASE WHEN sql.is_default = 0
                   THEN sql.sat_col_name || ' as ' || sql.column_type || ')'
                  ELSE sql.stage_col_name || ' as ' || sql.column_type || ')' END)),
             ', ') || ' from ' || satellite_schema_in || '.' || satellite_name_v || ' where ' ||
                ' dv_row_is_current=1::bit ),'
         FROM sql
         UNION ALL
         -- full load - mark all keys that not found in stage as deleted
         -- lookup key values
         SELECT DISTINCT CASE WHEN load_type_in = 'delta'
           THEN ' '
                         ELSE
                           '  deleted as ( update ' || satellite_schema_in || '.' || satellite_name_v
                           ||
                           ' as s  set dv_rowenddate=' || load_date_time_v ||
                           ' from src  where ' ||
                           -- list of lookup columns
                           ' s.' || sql.sat_col_name ||
                           '=src.hub_SK  and src.dv_record_source is null and s.dv_row_is_current=1::bit ), '
                         END
         FROM sql
         WHERE sql.is_surrogate_key = 1
         UNION ALL
         -- update row if key is found
         SELECT ' updates as ( update ' || satellite_schema_in || '.' || satellite_name_v
         UNION ALL
         SELECT 'as u  set dv_row_is_current=0::bit,dv_rowenddate=' || load_date_time_v
         UNION ALL
         SELECT ' from src '
         UNION ALL
         SELECT ' where u.' || sql.sat_col_name ||
                '=src.hub_SK and src.dv_record_source is not null and u.dv_row_is_current=1::bit ' ||
                E'\n returning src.* )'
         FROM sql
         WHERE sql.is_surrogate_key = 1
         UNION ALL
         -- if new record insert
         SELECT ' insert into ' || satellite_schema_in || '.' || satellite_name_v || '(' ||
                array_to_string(array_agg(sql.sat_col_name),
                                ', ') || ')'
         FROM sql
         UNION ALL
         SELECT 'select distinct r.* from (select ' || array_to_string(array_agg(sql.sat_col_name), ', ')
                || ',hub_SK ' ||
                ' from updates u union all select ' || array_to_string(array_agg(sql.sat_col_name), ', ') ||
                ',src.hub_SK ' ||
                ' from src where src.hub_SK is not null ) r '
         FROM sql
         WHERE is_surrogate_key = 0
         UNION ALL
         SELECT ' ;'

       ) t
  INTO sql_block_body_v;


  RETURN sql_block_start_v || sql_process_start_v || sql_block_body_v || sql_process_finish_v || sql_block_end_v;

END
$fun$;


/*************************function dealing with source and stage processing statuses**********************************/

DO $$
BEGIN
  RAISE NOTICE 'Configuring helper functions...';
END
$$;

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
$$;


CREATE FUNCTION fn_source_cleanup(table_schema_in CHARACTER VARYING, table_name_in CHARACTER VARYING)
  RETURNS TEXT
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
$$;


/************************* logger **********************************************************************/

DO $$
BEGIN
  RAISE NOTICE 'Configuring logging module...';
END
$$;


CREATE SEQUENCE dv_log_id_seq START 1;

CREATE TABLE dv_log
(
  id           INTEGER DEFAULT nextval('dv_log_id_seq' :: REGCLASS) PRIMARY KEY NOT NULL,
  log_datetime TIMESTAMP,
  log_proc     TEXT,
  message      TEXT
);


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

/************************* scheduler **********************************************************************/
DO $$
BEGIN
  RAISE NOTICE 'Configuring schedule module...';
END
$$;


CREATE SEQUENCE dv_schedule_seq START 1;

CREATE TABLE dv_schedule
(
  schedule_key         INTEGER                  DEFAULT nextval(
      'dv_schedule_seq' :: REGCLASS) PRIMARY KEY                                                           NOT NULL,
  schedule_name        VARCHAR(128)                                                                        NOT NULL,
  schedule_description VARCHAR(500),
  schedule_frequency   INTERVAL,
  start_date           TIMESTAMP                DEFAULT now(),
  last_start_date      TIMESTAMP,
  is_cancelled         BOOLEAN DEFAULT FALSE                                                               NOT NULL,
  release_key          INTEGER DEFAULT 1                                                                   NOT NULL,
  owner_key            INTEGER DEFAULT 1                                                                   NOT NULL,
  version_number       INTEGER DEFAULT 1                                                                   NOT NULL,
  updated_by           VARCHAR(50) DEFAULT "current_user"()                                                NOT NULL,
  updated_datetime     TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_schedule_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_schedule_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_schedule_unq
  ON dv_schedule (owner_key, schedule_name);

-- audit
CREATE TRIGGER dv_schedule_audit
AFTER UPDATE ON dv_schedule
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();


CREATE SEQUENCE dv_schedule_task_seq START 1;
CREATE TABLE dv_schedule_task
(
  schedule_task_key INTEGER                  DEFAULT nextval(
      'dv_schedule_task_seq' :: REGCLASS) PRIMARY KEY                                                        NOT NULL,
  schedule_key      INTEGER                                                                                  NOT NULL,
  object_key        INTEGER                                                                                  NOT NULL,
  object_type       VARCHAR(50)                                                                              NOT NULL,
  load_type         VARCHAR(30)                                                                              NOT NULL,
  is_cancelled      BOOLEAN DEFAULT FALSE                                                                    NOT NULL,
  release_key       INTEGER DEFAULT 1                                                                        NOT NULL,
  owner_key         INTEGER DEFAULT 1                                                                        NOT NULL,
  version_number    INTEGER DEFAULT 1                                                                        NOT NULL,
  updated_by        VARCHAR(50) DEFAULT "current_user"()                                                     NOT NULL,
  updated_datetime  TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_schedule_task_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_schedule_task_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_schedule_task_dv_schedule FOREIGN KEY (schedule_key) REFERENCES dv_schedule (schedule_key)

);
CREATE UNIQUE INDEX dv_schedule_task_unq
  ON dv_schedule_task (owner_key, schedule_key, object_key, object_type, load_type);

-- audit
CREATE TRIGGER dv_schedule_task_audit
AFTER UPDATE ON dv_schedule_task
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

CREATE SEQUENCE dv_schedule_task_hierarchy_seq START 1;
CREATE TABLE dv_schedule_task_hierarchy
(
  schedule_task_hierarchy_key INTEGER                  DEFAULT nextval(
      'dv_schedule_task_hierarchy_seq' :: REGCLASS) PRIMARY KEY    NOT NULL,
  schedule_task_key           INTEGER                              NOT NULL,
  schedule_parent_task_key    INTEGER,
  is_cancelled                BOOLEAN DEFAULT FALSE                NOT NULL,
  release_key                 INTEGER DEFAULT 1                    NOT NULL,
  owner_key                   INTEGER DEFAULT 1                    NOT NULL,
  version_number              INTEGER DEFAULT 1                    NOT NULL,
  updated_by                  VARCHAR(50) DEFAULT "current_user"() NOT NULL,
  updated_datetime            TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_schedule_task_hierarchy_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_schedule_task_hierarchy_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_schedule_task_hierarchy_dv_schedule_task FOREIGN KEY (schedule_task_key) REFERENCES dv_schedule_task (schedule_task_key)
);

CREATE UNIQUE INDEX dv_schedule_task_hierarchy_unq
  ON dv_schedule_task_hierarchy (owner_key, schedule_task_key, schedule_parent_task_key);

CREATE TRIGGER dv_schedule_task_hierarchy_audit
AFTER UPDATE ON dv_schedule_task_hierarchy
FOR EACH ROW
EXECUTE PROCEDURE dv_config_audit();

CREATE OR REPLACE FUNCTION ore_config.dv_run_next_schedule_task(job_id_in          INTEGER, schedule_key_in INTEGER,
                                                                parent_task_key_in INTEGER)
  RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  exec_script_v    TEXT;
  sql_v            TEXT;
  task_key_v       INT;
  job_id_v         INT :=job_id_in;
  schedule_key_v   INT :=schedule_key_in;
  status_v         VARCHAR :='done';
  exec_type_v      VARCHAR(30);
  exec_script_l2_v TEXT;
  state_v          VARCHAR;
  proc_v           VARCHAR(50) :='dv_run_next_schedule_task';
BEGIN

  -- identify first task to run

  SELECT
    coalesce(min(schedule_task_key), -1),
    min(script),
    min(exec_type)
  INTO task_key_v, exec_script_v, exec_type_v
  FROM dv_schedule_task_queue
  WHERE job_id = job_id_in AND parent_task_key = parent_task_key_in AND schedule_key = schedule_key_in;

  SELECT dv_log_proc(proc_v, 'Task to run-->' || task_key_v)
  INTO state_v;

  IF task_key_v <> -1
  THEN

    BEGIN

      -- set status to processing
      status_v:='processing';

      -- update task status
      UPDATE dv_schedule_task_queue
      SET process_status = status_v, update_datetime = now()
      WHERE job_id = job_id_v AND schedule_key = schedule_key_in AND schedule_task_key = task_key_v;

      SELECT dv_log_proc(proc_v,
                         'Executing task-->' || task_key_v || '; type-->' || exec_type_v || '; task script-->' ||
                         exec_script_v)
      INTO state_v;

      -- run statement
      EXECUTE exec_script_v
      INTO exec_script_l2_v;

      -- for any other type than business_rule_proc run second time as first run only generates code to run in that case
      IF exec_type_v <> 'business_rule_proc'
      THEN

        SELECT dv_log_proc(proc_v, 'Executing script-->' || exec_script_l2_v)
        INTO state_v;

        EXECUTE exec_script_l2_v;

      END IF;

      -- set status to DONE if executed successfully
      status_v:='done';

      EXCEPTION WHEN OTHERS
      THEN
        -- in case of failure
        status_v:='failed';
    END;
  ELSE
    -- if no child tasks check if there are another jobs for this schedule queued minimum of jobs_id
    SELECT
      coalesce(min(schedule_task_key), -1),
      coalesce(min(job_id), -1)
    INTO task_key_v, job_id_v
    FROM (
           SELECT
             schedule_task_key,
             job_id,
             ROW_NUMBER()
             OVER (
               ORDER BY job_id ASC) AS rn
           FROM dv_schedule_task_queue
           WHERE parent_task_key IS NULL AND process_status = 'queued'
                 AND schedule_key = schedule_key_in AND job_id <> job_id_in AND parent_task_key IS NULL) t
    WHERE rn = 1;

  END IF;

  -- update status of the task to done or failed depending on execution
  UPDATE dv_schedule_task_queue
  SET process_status = status_v, update_datetime = now()
  WHERE job_id = job_id_v AND schedule_key = schedule_key_in AND schedule_task_key = task_key_v;

  -- no tasks to run for job_id
  -- clean up and dump all executed tasks into history
  IF task_key_v = -1 OR job_id_v = -1
  THEN

    WITH del AS
    (
      DELETE FROM dv_schedule_task_queue
      WHERE process_status = 'done' AND schedule_key = schedule_key_in AND job_id = job_id_in
      RETURNING *
    )
    INSERT INTO dv_schedule_task_queue_history (job_id,
                                                schedule_key,
                                                schedule_task_key,
                                                parent_task_key,
                                                task_level,
                                                process_status,
                                                script,
                                                exec_type,
                                                start_datetime,
                                                owner_key, insert_datetime)
      SELECT
        job_id,
        schedule_key,
        schedule_task_key,
        parent_task_key,
        task_level,
        process_status,
        script,
        exec_type,
        start_datetime,
        owner_key,
        now()
      FROM del;
  END IF;

  SELECT dv_log_proc(proc_v, 'Finished execution ....')
  INTO state_v;

  RETURN 1;
END
$$;


CREATE OR REPLACE FUNCTION dv_init_schedule_task_run()
  RETURNS TRIGGER
AS $body$
DECLARE
  result_v INT;
  proc_v   VARCHAR(50) :='dv_init_schedule_task_run';
  state_v  VARCHAR;
BEGIN


  IF new.process_status = 'done'
  THEN

    SELECT dv_log_proc(proc_v, 'Schedule-->' || new.schedule_key || '; job_id-->' || new.job_id || ';task_key-->' ||
                               new.schedule_task_key)
    INTO state_v;

    -- run next task
    SELECT dv_run_next_schedule_task(new.job_id, new.schedule_key, new.schedule_task_key)
    INTO result_v;

  END IF;
  RETURN NULL;
END
$body$
LANGUAGE plpgsql;


CREATE SEQUENCE dv_job_id_seq START 1;

CREATE TABLE dv_schedule_task_queue
(
  job_id            INT,
  schedule_key      INT,
  schedule_task_key INT,
  parent_task_key   INT,
  task_level        INT,
  process_status    VARCHAR(50),
  script            TEXT,
  exec_type         VARCHAR(30),
  start_datetime    TIMESTAMP,
  update_datetime   TIMESTAMP,
  owner_key         INT
);

CREATE TRIGGER dv_schedule_task_queue_tgu
AFTER UPDATE ON ore_config.dv_schedule_task_queue
FOR EACH ROW EXECUTE PROCEDURE ore_config.dv_init_schedule_task_run();


CREATE TABLE dv_schedule_task_queue_history
(
  job_id            INT,
  schedule_key      INT,
  schedule_task_key INT,
  parent_task_key   INT,
  task_level        INT,
  process_status    VARCHAR(50),
  script            TEXT,
  exec_type         VARCHAR(30),
  start_datetime    TIMESTAMP,
  update_datetime   TIMESTAMP,
  owner_key         INT,
  insert_datetime   TIMESTAMP
);


CREATE FUNCTION fn_get_dv_object_load_script(object_key_in INTEGER, object_type_in CHARACTER VARYING,
                                             load_type_in  CHARACTER VARYING, owner_key_in INTEGER)
  RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  sql_v TEXT;
BEGIN

  CASE
    WHEN object_type_in IN ('business_rule', 'business_rule_proc')
    THEN
      -- 1. business_rule/ stage table
      -- if it is stored procedure then different
      SELECT business_rule_logic
      INTO sql_v
      FROM dv_business_rule
      WHERE business_rule_key = object_key_in
            AND is_retired = FALSE
            AND owner_key = owner_key_in;

    WHEN object_type_in = 'hub'
    THEN
      -- 2. hub
      SELECT DISTINCT
        'select ore_config.dv_config_dv_load_hub(''' || st.stage_table_schema || ''',''' || st.stage_table_name ||
        ''',''' ||
        h.hub_schema || ''',''' ||
        h.hub_name || ''');'
      INTO sql_v
      FROM dv_hub h
        JOIN dv_hub_key_column hk ON h.hub_key = hk.hub_key
        JOIN dv_hub_column hc ON hc.hub_key_column_key = hk.hub_key_column_key
        JOIN dv_stage_table_column sc ON sc.column_key = hc.column_key
        JOIN dv_stage_table st ON st.stage_table_key = sc.stage_table_key
      WHERE h.owner_key = owner_key_in AND h.is_retired = FALSE AND st.is_retired = FALSE AND sc.is_retired = FALSE
            AND st.stage_table_key = object_key_in;
    WHEN object_type_in = 'satellite'
    THEN
      -- 3. satellite
      SELECT DISTINCT
        'select ore_config.dv_config_dv_load_satellite(''' || st.stage_table_schema || ''',''' || st.stage_table_name ||
        ''',''' ||
        s.satellite_schema || ''',''' || s.satellite_name || ''',''' || load_type_in || ''');'
      INTO sql_v
      FROM dv_satellite s
        JOIN dv_satellite_column sc ON sc.satellite_key = s.satellite_key
        JOIN dv_stage_table_column stc ON stc.column_key = sc.column_key
        JOIN dv_stage_table st ON stc.stage_table_key = st.stage_table_key
      WHERE s.is_retired = FALSE AND st.is_retired = FALSE AND stc.is_retired = FALSE
            AND s.owner_key = owner_key_in
            AND st.stage_table_key = object_key_in;
  ELSE
    -- 4. source or anything else -  nothing
    sql_v:='';
  END CASE;


  RETURN sql_v;

END
$$;

-- list of valid schedule tasks & execution sctipts
CREATE OR REPLACE VIEW dv_schedule_valid_tasks AS
  SELECT
    t.schedule_key,
    t.schedule_name,
    t.owner_key,
    t.schedule_frequency,
    t.schedule_task_key,
    t.parent_task_key,
    t.depth                                                                                        AS task_level,
    t.object_key,
    t.object_type,
    t.load_type,
    ore_config.fn_get_dv_object_load_script(t.object_key, t.object_type, t.load_type, t.owner_key) AS load_script
  FROM
    (
      SELECT
        s.schedule_key,
        s.owner_key,
        s.schedule_name,
        s.schedule_frequency,
        s.start_date,
        st.schedule_task_key,
        sth.parent_task_key,
        sth.depth,
        st.object_key,
        st.object_type,
        st.load_type
      FROM dv_schedule s
        JOIN dv_schedule_task st ON s.schedule_key = st.schedule_key
        JOIN
        (
          WITH RECURSIVE node_rec AS
          (
            SELECT
              1                        AS depth,
              schedule_task_key        AS task_key,
              schedule_parent_task_key AS parent_task_key
            FROM dv_schedule_task_hierarchy
            WHERE schedule_parent_task_key IS NULL AND is_cancelled = FALSE
            UNION ALL
            SELECT
              depth + 1,
              n.schedule_task_key        AS task_key,
              n.schedule_parent_task_key AS parent_task_key
            FROM dv_schedule_task_hierarchy AS n
              JOIN node_rec r ON n.schedule_parent_task_key = r.task_key
            WHERE n.is_cancelled = FALSE
          )
          SELECT
            depth,
            task_key,
            parent_task_key
          FROM node_rec
        )
        sth ON sth.task_key = st.schedule_task_key
      WHERE s.is_cancelled = FALSE AND st.is_cancelled = FALSE
    ) t;


CREATE FUNCTION dv_load_source_status_update(owner_name_in   CHARACTER VARYING, system_name_in CHARACTER VARYING,
                                             table_schema_in CHARACTER VARYING, table_name_in CHARACTER VARYING)
  RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  owner_key_v      INTEGER;
  start_time_v     TIMESTAMP;
  process_status_v VARCHAR(20) :='queued';
  schedule_key_v   INT;
  state_v          VARCHAR;
  proc_v           VARCHAR(50) :='dv_load_source_status_update';
  job_id_v         INT;
BEGIN

  SET SEARCH_PATH TO ore_config;

  -- generate job_id
  SELECT nextval('dv_job_id_seq' :: REGCLASS)
  INTO job_id_v;

  SELECT dv_log_proc(proc_v, 'Starting execution of job_id-->' || job_id_v)
  INTO state_v;

  start_time_v:=now();

  -- find owner key
  SELECT owner_key
  INTO owner_key_v
  FROM dv_owner
  WHERE owner_name = owner_name_in;

  -- find related schedule tasks for source
  -- select tasks for execution
  -- queued process state
  WITH src AS (
      SELECT
        S.schedule_task_key AS task_key,
        s.schedule_key
      FROM dv_schedule_task S
        JOIN dv_source_table st ON S.object_key = st.source_table_key
        JOIN dv_source_system ss ON st.system_key = ss.source_system_key
      WHERE S.object_type = 'source' AND S.owner_key = owner_key_v
            AND ss.source_system_name = system_name_in
            AND st.source_table_schema = table_schema_in
            AND st.source_table_name = table_name_in)
  INSERT INTO dv_schedule_task_queue (job_id,
                                      schedule_key,
                                      schedule_task_key,
                                      parent_task_key,
                                      task_level,
                                      process_status,
                                      script,
                                      exec_type,
                                      start_datetime,
                                      owner_key)
    SELECT
      job_id_v,
      v.schedule_key,
      v.schedule_task_key,
      v.parent_task_key,
      v.task_level,
      process_status_v,
      v.load_script AS script,
      v.object_type,
      start_time_v,
      v.owner_key
    FROM dv_schedule_valid_tasks v
      JOIN src ON v.schedule_key = src.schedule_key;

  -- updates first task to trigger next job_id for the same schedule
  -- need to check if there is another job for this schedule is running and update status appropriately
  UPDATE dv_schedule_task_queue q
  SET process_status = 'done', update_datetime = now()
  WHERE q.job_id = job_id_v
        AND q.parent_task_key IS NULL
        AND NOT exists(SELECT 1
                       FROM dv_schedule_task_queue d
                       WHERE d.schedule_key = q.schedule_key AND d.job_id <> job_id_v AND
                             d.process_status IN ('queued', 'processing')
                             AND d.start_datetime < q.start_datetime
  );

  SELECT dv_log_proc(proc_v, 'Finished execution of job_id-->' || job_id_v)
  INTO state_v;

END
$$;


/************************* modeller package**********************************************************************/
DO $$
BEGIN
  RAISE NOTICE 'Configuring modeller...';
END
$$;

CREATE TABLE dv_model_L1_design
(
  object_type         VARCHAR,
  object_schema       VARCHAR,
  object_name         VARCHAR,
  object_relationship VARCHAR,
  is_parent           INT
);

CREATE TABLE dv_model_L2_contents
(
  object_type      VARCHAR,
  object_name      VARCHAR,
  object_schema    VARCHAR,
  column_name      VARCHAR,
  column_type      VARCHAR,
  column_length    INT,
  column_precision INT,
  column_scale     INT
);

CREATE TABLE dv_model_L3_mapping
(
  mapping_type      VARCHAR,
  object_name_in    VARCHAR,
  object_schema_in  VARCHAR,
  column_name_in    VARCHAR,
  object_name_out   VARCHAR,
  object_schema_out VARCHAR,
  column_name_out   VARCHAR
);

CREATE TABLE dv_model_L4_logic
(
  schedule_name           VARCHAR,
  object_type             VARCHAR,
  source_name             VARCHAR,
  source_schema           VARCHAR,
  source_load_type        VARCHAR,
  business_rule_name      VARCHAR,
  business_rule_logic     TEXT,
  business_rule_load_type VARCHAR,
  business_rule_type      VARCHAR,
  rn_order                INT
);


-- model - design
CREATE OR REPLACE FUNCTION dv_model_l1_load_design(owner_name_in     VARCHAR, owner_desc_in VARCHAR,
                                                   release_number_in INT,
                                                   release_desc_in   VARCHAR)
  RETURNS VOID AS
$BODY$
DECLARE
  release_key_v INT;
  owner_key_v   INT;
  object_v      VARCHAR [] [];
  r             RECORD;
  rd            RECORD;
  object_key_v  INT;
  state_v       VARCHAR;
BEGIN
  -- check if owner exists
  SELECT owner_key
  INTO owner_key_v
  FROM dv_owner ow
  WHERE owner_name = owner_name_in;

  RAISE NOTICE 'Owner_key-->%', owner_key_v;

  -- new owner
  IF owner_key_v IS NULL
  THEN
    -- add owner
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [2, 2]);
    object_v [1] [1]:='owner_name';
    object_v [1] [2]:=owner_name_in;
    object_v [2] [1]:='owner_description';
    object_v [2] [2]:=owner_desc_in;


    SELECT dv_config_object_insert('dv_owner',
                                   object_v)
    INTO state_v;


    SELECT owner_key
    INTO owner_key_v
    FROM dv_owner ow
    WHERE owner_name = owner_name_in;

    RAISE NOTICE 'Added owner -->%', owner_key_v;
  END IF;

  -- release
  SELECT release_key
  INTO release_key_v
  FROM dv_release
  WHERE release_number = release_number_in;

  -- new release
  IF release_key_v IS NULL
  THEN
    -- add release
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [3, 2]);
    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='release_description';
    object_v [2] [2]:=release_desc_in;
    object_v [3] [1]:='owner_key';
    object_v [3] [2]:= owner_key_v;

    RAISE NOTICE 'Adding release -->%', release_key_v;
    SELECT dv_config_object_insert('dv_release',
                                   object_v)
    INTO state_v;
  END IF;

  -- go through the rest and add DV objects into config
  -- source_systems


  object_v:=array_fill(NULL :: VARCHAR, ARRAY [5, 2]);
  object_v [1] [1]:='release_number';
  object_v [1] [2]:=release_number_in;
  object_v [2] [1]:='owner_key';
  object_v [2] [2]:= owner_key_v;

  FOR r IN (SELECT *
            FROM dv_model_L1_design
            WHERE object_type IN ('source_system', 'hub')) LOOP

    object_key_v:=null;

    object_v [3] [1]:=r.object_type || '_schema';
    object_v [3] [2]:=r.object_schema;
    object_v [4] [1]:=r.object_type || '_name';
    object_v [4] [2]:=r.object_name;

    SELECT dv_config_object_insert('dv_' || r.object_type,
                                   object_v)
    INTO state_v;

    EXECUTE 'select ' || r.object_type || '_key from dv_' || r.object_type || ' where ' || r.object_type || '_schema='''
            || r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO object_key_v;

    RAISE NOTICE 'Object_key inserted -->%', object_key_v;

    -- looping through dependamt objects
    FOR rd IN (SELECT *
               FROM dv_model_L1_design
               WHERE is_parent <> 1 AND r.object_relationship = object_relationship)
    LOOP
      object_v [3] [1]:=rd.object_type || '_schema';
      object_v [3] [2]:=rd.object_schema;
      object_v [4] [1]:=rd.object_type || '_name';
      object_v [4] [2]:=rd.object_name;

      object_v [5] [1]:= (CASE WHEN r.object_type = 'source_system'
        THEN 'system'
                          ELSE r.object_type END) || '_key';
      object_v [5] [2]:=object_key_v;

      -- add object to config
      SELECT dv_config_object_insert('dv_' || rd.object_type,
                                     object_v)
      INTO state_v;

    END LOOP;

  END LOOP;


END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dv_model_l2_load_contents(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r            RECORD;
  owner_key_v  INT;
  object_key_v INT;
  object_v     VARCHAR [] [];
  suffix_v     VARCHAR;
  state_v      VARCHAR;
BEGIN
  -- loop through all columns
  FOR r IN (SELECT *
            FROM dv_model_L2_contents) LOOP
    object_key_v:=NULL;

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [8, 2]);

    RAISE NOTICE 'Starting -->%', object_key_v;
    -- get keys of parent objects
    EXECUTE 'select owner_key, ' || r.object_type || '_key from dv_' || r.object_type || ' where ' ||
            r.object_type || '_schema=''' ||
            r.object_schema || ''' and ' || r.object_type || '_name=''' || r.object_name || ''''
    INTO owner_key_v, object_key_v;


    RAISE NOTICE 'Object to link to -->%', object_key_v;

    suffix_v :=CASE WHEN r.object_type = 'hub'
      THEN 'hub_key_'
               ELSE '' END;

    RAISE NOTICE 'Object to link to -->%', suffix_v;

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:=r.object_type || '_key';
    object_v [3] [2]:=object_key_v;

    object_v [4] [1]:= suffix_v || 'column_name';
    object_v [4] [2]:= r.column_name;
    object_v [5] [1]:=suffix_v || 'column_type';
    object_v [5] [2]:= r.column_type;
    object_v [6] [1]:=suffix_v || 'column_length';
    object_v [6] [2]:= r.column_length;
    object_v [7] [1]:=suffix_v || 'column_precision';
    object_v [7] [2]:= r.column_precision;
    object_v [8] [1]:=suffix_v || 'column_scale';
    object_v [8] [2]:= r.column_scale;

    -- add object into config
    SELECT dv_config_object_insert('dv_' || CASE WHEN r.object_type = 'hub'
      THEN suffix_v
                                            ELSE r.object_type || '_' END || 'column',
                                   object_v)
    INTO state_v;


  END LOOP;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dv_model_l3_load_mappings(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r            RECORD;
  owner_key_v  INT;
  object_key_v INT;
  object_v     VARCHAR [] [];
  column_key_v INT;
  state_v      VARCHAR;
BEGIN

  -- loop through columns to map
  FOR r IN (SELECT *
            FROM dv_model_L3_mapping) LOOP

    object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

    object_key_v:=NULL;

    -- stage table column key
    SELECT
      column_key,
      c.owner_key
    INTO column_key_v, owner_key_v
    FROM dv_stage_table st
      JOIN dv_stage_table_column c ON st.stage_table_key = c.stage_table_key
    WHERE stage_table_schema = r.object_schema_out AND st.stage_table_name = r.object_name_out AND
          c.column_name = r.column_name_out;

    RAISE NOTICE 'Column key -->%', column_key_v;

    -- find mapping object key
    IF r.mapping_type = 'hub'
    THEN

      SELECT hub_key_column_key
      INTO object_key_v
      FROM dv_hub_key_column hkc
        JOIN dv_hub h ON h.hub_key = hkc.hub_key
      WHERE h.hub_name = r.object_name_in AND h.hub_schema = r.object_schema_in
            AND hkc.hub_key_column_name = r.column_name_in;

    ELSE
      -- mapping for satellites
      SELECT satellite_key
      INTO object_key_v
      FROM dv_satellite
      WHERE satellite_name = r.object_name_in AND satellite_schema = r.object_schema_in;
    END IF;

    RAISE NOTICE 'Object key -->%', object_key_v;

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:= CASE WHEN r.mapping_type = 'hub'
      THEN 'hub_key_column_key'
                       ELSE 'satellite_key' END;
    object_v [3] [2]:=object_key_v;
    object_v [4] [1]:='column_key';
    object_v [4] [2]:= column_key_v;

    -- add data to config
    SELECT dv_config_object_insert('dv_' || r.mapping_type || '_column',
                                   object_v)
    INTO state_v;

  END LOOP;

END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dv_model_l4_load_logic(release_number_in INT)
  RETURNS VOID AS
$BODY$
DECLARE
  r                 RECORD;
  rh                RECORD;
  owner_key_v       INT;
  object_key_v      INT;
  object_v          VARCHAR [] [];
  object2_key_v     INT;
  object3_key_v     INT;
  task_key_v        INT;
  parent_task_key_v INT;
  state_v           VARCHAR;
BEGIN

  -- check if release exists
  -- find owner_key

  SELECT owner_key
  INTO owner_key_v
  FROM dv_release
  WHERE release_number = release_number_in;

  RAISE NOTICE 'Owner -->%', owner_key_v;

  IF owner_key_v IS NULL
  THEN
    RETURN;
  END IF;

  FOR r IN (SELECT DISTINCT schedule_name AS schedule_name
            FROM dv_model_L4_logic) LOOP

    parent_task_key_v:=NULL;

    -- add schedule - 1 schedule per one - source
    -- add schedule tasks related to source load
    -- add business rules related to tasks
    -- add hierarchy of tasks
    object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

    object_v [1] [1]:='release_number';
    object_v [1] [2]:=release_number_in;
    object_v [2] [1]:='owner_key';
    object_v [2] [2]:= owner_key_v;
    object_v [3] [1]:= 'schedule_name';
    object_v [3] [2]:= r.schedule_name;
    object_v [4] [1]:= 'schedule_description';
    object_v [4] [2]:= r.schedule_name;

    SELECT dv_config_object_insert('dv_schedule',
                                   object_v)
    INTO state_v;

    -- find just inserted object_key
    SELECT schedule_key
    INTO object_key_v
    FROM dv_schedule
    WHERE schedule_name = r.schedule_name AND owner_key = owner_key_v;

    RAISE NOTICE 'Schedule key -->%', object_key_v;

    -- loop through related tasks and business rules
    FOR rh IN (SELECT *
               FROM dv_model_L4_logic
               WHERE schedule_name = r.schedule_name
               ORDER BY rn_order ASC
    ) LOOP

      object2_key_v:=NULL;
      object3_key_v:=NULL;

      -- load source task - find table key

      IF rh.object_type = 'source' or rh.is_stage=0
      THEN

        SELECT source_table_key
        INTO object2_key_v
        FROM dv_source_table
        WHERE source_table_name = rh.source_name
              AND source_table_schema = rh.source_schema
              AND owner_key = owner_key_v;

      ELSE
        -- find stage table key for other tasks
        SELECT stage_table_key
        INTO object2_key_v
        FROM dv_stage_table
        WHERE stage_table_name = rh.source_name
              AND stage_table_schema = rh.source_schema
              AND owner_key = owner_key_v;
      END IF;

      RAISE NOTICE 'Object key 2 source or stage tables -->%', object2_key_v;
      -- configure and add business rules
      IF rh.object_type like '%business_rule%'
      --rh.business_rule_name in ('')
      THEN

        object_v:=array_fill(NULL :: VARCHAR, ARRAY [7, 2]);

        object_v [1] [1]:='release_number';
        object_v [1] [2]:=release_number_in;
        object_v [2] [1]:='owner_key';
        object_v [2] [2]:= owner_key_v;

        object_v [3] [1]:= 'business_rule_name';
        object_v [3] [2]:= rh.business_rule_name;

        object_v [4] [1]:= 'business_rule_logic';
        object_v [4] [2]:= rh.business_rule_logic;

        object_v [5] [1]:= 'stage_table_key';
        object_v [5] [2]:= object2_key_v;
        object_v [6] [1]:= 'business_rule_type';
        object_v [6] [2]:= rh.business_rule_type;

        object_v [7] [1]:= 'load_type';
        object_v [7] [2]:= rh.business_rule_load_type;

        -- add business rule
        SELECT dv_config_object_insert('dv_business_rule', object_v)
        INTO state_v;

        -- find newly added key
        SELECT business_rule_key
        INTO object3_key_v
        FROM dv_business_rule
        WHERE
          owner_key = owner_key_v AND business_rule_name = rh.business_rule_name AND stage_table_key = object2_key_v;

        RAISE NOTICE 'Business rule key -->%', object3_key_v;

      END IF;

      -- configure schedule task
      object_v:=array_fill(NULL :: VARCHAR, ARRAY [6, 2]);

      object_v [1] [1]:='release_number';
      object_v [1] [2]:=release_number_in;
      object_v [2] [1]:='owner_key';
      object_v [2] [2]:= owner_key_v;

      object_v [3] [1]:= 'schedule_key';
      object_v [3] [2]:= object_key_v;
      object_v [4] [1]:='object_key';
      object_v [4] [2]:= coalesce(object3_key_v, object2_key_v);
      object_v [5] [1]:= 'object_type';
      object_v [5] [2]:= rh.object_type;
      object_v [6] [1]:= 'load_type';
      object_v [6] [2]:= rh.source_load_type;

      -- add schedule task
      SELECT dv_config_object_insert('dv_schedule_task', object_v)
      INTO state_v;

      -- find newly added key to use in hierarchy
      SELECT schedule_task_key
      INTO task_key_v
      FROM dv_schedule_task
      WHERE owner_key = owner_key_v AND object_key = coalesce(object3_key_v, object2_key_v)
            AND object_type = rh.object_type;

      RAISE NOTICE 'Task key -->%', task_key_v;

      -- add schedule_hierarchy
      -- should be sorted by rn_order
      -- source always has no parent

      IF rh.object_type = 'source'
      THEN
        parent_task_key_v:=NULL;
      END IF;

      -- schedule task hierarchy configuration
      object_v:=array_fill(NULL :: VARCHAR, ARRAY [4, 2]);

      object_v [1] [1]:='release_number';
      object_v [1] [2]:=release_number_in;
      object_v [2] [1]:='owner_key';
      object_v [2] [2]:= owner_key_v;

      object_v [3] [1]:= 'schedule_task_key';
      object_v [3] [2]:= task_key_v;
      object_v [4] [1]:='schedule_parent_task_key';
      object_v [4] [2]:= parent_task_key_v;

      RAISE NOTICE 'Hierarchy -->%', object_v;

      SELECT dv_config_object_insert('dv_schedule_task_hierarchy', object_v)
      INTO state_v;

      RAISE NOTICE 'Inserted data into task hierarchy ...';
      -- save for a use for adding next task
      parent_task_key_v:=task_key_v;
    END LOOP;


  END LOOP;

END
$BODY$
LANGUAGE plpgsql;


-- modeller
CREATE OR REPLACE FUNCTION dv_modeller(owner_name_in     VARCHAR, owner_desc_in VARCHAR,
                                       release_number_in INT,
                                       release_desc_in   VARCHAR)
  RETURNS VOID AS
$BODY$
BEGIN

  SELECT dv_model_l1_load_design(owner_name_in, owner_desc_in,
                                 release_number_in,
                                 release_desc_in);
  SELECT dv_model_l2_load_contents(release_number_in);
  SELECT dv_model_l3_load_mappings(release_number_in);
  SELECT dv_model_l4_load_logic(release_number_in);


END
$BODY$
LANGUAGE plpgsql;


/************************* default values setup **********************************************************************/

RAISE NOTICE 'Configuring Postgres extentions...';

SET SEARCH_PATH TO public;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

SET SEARCH_PATH TO ore_config;

DO $$
DECLARE
  owner_key_v   INT;
  release_key_v INT;
  cnt_v         INT;
  release_v     VARCHAR [] [];

BEGIN
  RAISE NOTICE 'Configuring default values...';

  -- default owner
  SELECT dv_config_object_insert('dv_owner',
                                 '{{"owner_name","default"},{"owner_description","default owner"}}')
  INTO cnt_v;

  IF cnt_v > 0
  THEN
    SELECT owner_key
    INTO owner_key_v
    FROM dv_owner
    WHERE owner_name = 'default';
    -- default release

    release_v:=array_fill(NULL :: VARCHAR, ARRAY [3, 2]);
    release_v [1] [1]:='release_number';
    release_v [1] [2]:='0';
    release_v [2] [1]:='release_description';
    release_v [2] [2]:='default release';
    release_v [3] [1]:='owner_key';
    release_v [3] [2]:= cast(owner_key_v AS VARCHAR);
    SELECT dv_config_object_insert('dv_release',
                                   release_v)
    INTO cnt_v;

    SELECT release_key
    INTO release_key_v
    FROM dv_release
    WHERE release_number = 0 AND owner_key = owner_key_v;

    INSERT INTO ore_config.dv_defaults (default_type, default_subtype, default_sequence, data_type, default_integer, default_varchar, default_datetime, owner_key, release_key)
      SELECT
        r.default_type,
        r.default_subtype,
        r.default_sequence,
        r.data_type,
        cast(r.default_integer AS INTEGER),
        r.default_varchar,
        cast(r.default_datetime AS TIMESTAMP),
        owner_key_v,
        release_key_v
      FROM (
             SELECT
               'hub'       AS default_type,
               'filegroup' AS default_subtype,
               1           AS default_sequence,
               'varchar'   AS data_type,
               NULL        AS default_integer,
               'primary'   AS default_varchar,
               NULL        AS default_datetime
             UNION ALL
             SELECT
               'hub',
               'prefix',
               1,
               'varchar',
               NULL,
               'h_',
               NULL
             UNION ALL
             SELECT
               'hub',
               'schema',
               1,
               'varchar',
               NULL,
               'hub',
               NULL
             UNION ALL
             SELECT
               'hubsurrogate',
               'suffix',
               1,
               'varchar',
               NULL,
               '_key',
               NULL
             UNION ALL
             SELECT
               'satellite',
               'filegroup',
               1,
               'varchar',
               NULL,
               'primary',
               NULL
             UNION ALL
             SELECT
               'satellite',
               'prefix',
               1,
               'varchar',
               NULL,
               's_',
               NULL
             UNION ALL
             SELECT
               'satellitesurrogate',
               'suffix',
               1,
               'varchar',
               NULL,
               '_key',
               NULL) r;

    INSERT INTO dv_default_column (object_type, object_column_type, ordinal_position, column_prefix, column_name, column_suffix, column_type, column_length, column_precision, column_scale, collation_name, is_nullable, is_pk, discard_flag, is_indexed, owner_key, release_key)
      SELECT
        r.*,
        owner_key_v,
        release_key_v
      FROM (
             SELECT
               'hub',
               'Data_Source',
               3,
               '',
               'dv_record_source',
               '',
               'varchar',
               50,
               0,
               0,
               '',
               FALSE,
               FALSE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'satellite',
               'Data_Source',
               4,
               '',
               'dv_record_source',
               '',
               'varchar',
               50,
               0,
               0,
               '',
               FALSE,
               FALSE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'satellite',
               'Current_Row',
               5,
               '',
               'dv_row_is_current',
               '',
               'bit',
               0,
               0,
               0,
               '',
               FALSE,
               FALSE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'hub',
               'Load_Date_Time',
               2,
               '',
               'dv_load_date_time',
               '',
               'timestamp',
               0,
               7,
               0,
               '',
               FALSE,
               FALSE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'satellite',
               'Source_Date_Time',
               3,
               '',
               'dv_source_date_time',
               '',
               'timestamp',
               0,
               7,
               0,
               '',
               FALSE,
               FALSE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'satellite',
               'Version_End_Date',
               8,
               '',
               'dv_rowenddate',
               '',
               'timestamp',
               0,
               7,
               0,
               '',
               FALSE,
               FALSE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'hub',
               'Object_Key',
               1,
               'h_',
               '%',
               '_key',
               'int',
               0,
               0,
               0,
               '',
               FALSE,
               TRUE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'satellite',
               'Object_Key',
               1,
               's_',
               '%',
               '_key',
               'int',
               0,
               0,
               0,
               '',
               FALSE,
               TRUE,
               FALSE,
               FALSE
             UNION ALL
             SELECT

               'satellite',
               'Version_Start_Date',
               7,
               '',
               'dv_rowstartdate',
               '',
               'timestamp',
               0,
               7,
               0,
               '',
               FALSE,
               FALSE,
               FALSE,
               TRUE
             UNION ALL
             SELECT
               'stage_table',
               'process_status',
               0,
               NULL,
               'dv_process_status',
               NULL,
               'varchar',
               30,
               0,
               0,
               NULL,
               TRUE,
               FALSE,
               FALSE,
               FALSE
             UNION ALL
             SELECT
               'stage_table',
               'Load_Date_Time',
               0,
               NULL,
               'dv_load_datetime',
               NULL,
               'timestamp',
               NULL,
               NULL,
               NULL,
               NULL,
               TRUE,
               FALSE,
               FALSE,
               FALSE) r;


  ELSE
    RAISE NOTICE 'Something went wrong...';
    RETURN;
  END IF;

  RAISE NOTICE 'Completed...';

END;
$$;





