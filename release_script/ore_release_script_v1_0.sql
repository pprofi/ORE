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
  owner_key         INTEGER                  DEFAULT nextval('dv_owner_key_seq' :: REGCLASS) PRIMARY KEY NOT NULL,
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
  default_key      INTEGER                  DEFAULT nextval('dv_defaults_key_seq' :: REGCLASS) PRIMARY KEY NOT NULL,
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
  column_key              INTEGER                  DEFAULT nextval(
      'dv_stage_table_column_key_seq' :: REGCLASS) PRIMARY KEY                                             NOT NULL,
  stage_table_key         INTEGER                                                                          NOT NULL,
  column_name             VARCHAR(128)                                                                     NOT NULL,
  column_type             VARCHAR(30)                                                                      NOT NULL,
  column_length           INTEGER,
  column_precision        INTEGER,
  column_scale            INTEGER,
  collation_name          VARCHAR(128),
  source_ordinal_position INTEGER                                                                          NOT NULL,
  is_source_date          BOOLEAN DEFAULT FALSE                                                            NOT NULL,
  discard_flag            BOOLEAN DEFAULT FALSE                                                            NOT NULL,
  is_retired              BOOLEAN DEFAULT FALSE                                                            NOT NULL,
  release_key             INTEGER DEFAULT 1                                                                NOT NULL,
  owner_key               INTEGER DEFAULT 1                                                                NOT NULL,
  version_number          INTEGER DEFAULT 1                                                                NOT NULL,
  updated_by              VARCHAR(50) DEFAULT current_user                                                 NOT NULL,
  updated_datetime        TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_stage_table_column_dv_stage_table FOREIGN KEY (stage_table_key) REFERENCES dv_stage_table (stage_table_key),
  CONSTRAINT fk_dv_stage_table_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_stage_table_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_stage_table_column_unq
  ON dv_stage_table_column (owner_key, stage_table_key, column_name);

-- audit
CREATE TRIGGER dv__stage_table_column_audit
AFTER UPDATE ON dv__stage_table_column
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
      'dv_business_rule_key_seq' :: REGCLASS) PRIMARY KEY                                         NOT NULL,
  stage_table_key     INTEGER                                                                     NOT NULL,
  business_rule_name  VARCHAR(128)                                                                NOT NULL,
  business_rule_type  VARCHAR(20) DEFAULT 'internal_sql' :: CHARACTER VARYING                     NOT NULL,
  business_rule_logic TEXT                                                                        NOT NULL,
  load_type           VARCHAR(50)                                                                 NOT NULL,
  is_external         BOOLEAN DEFAULT FALSE                                                       NOT NULL,
  is_retired          BOOLEAN DEFAULT FALSE                                                       NOT NULL,
  release_key         INTEGER DEFAULT 1                                                           NOT NULL,
  owner_key           INTEGER DEFAULT 1                                                           NOT NULL,
  version_number      INTEGER DEFAULT 1                                                           NOT NULL,
  updated_by          VARCHAR(50) DEFAULT "current_user"()                                        NOT NULL,
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
  hub_key          INTEGER                  DEFAULT nextval('dv_hub_key_seq' :: REGCLASS) PRIMARY KEY NOT NULL,
  hub_name         VARCHAR(128)                                                                       NOT NULL,
  hub_schema       VARCHAR(128)                                                                       NOT NULL,
  is_retired       BOOLEAN DEFAULT FALSE                                                              NOT NULL,
  release_key      INTEGER DEFAULT 1                                                                  NOT NULL,
  owner_key        INTEGER DEFAULT 1                                                                  NOT NULL,
  version_number   INTEGER DEFAULT 1                                                                  NOT NULL,
  updated_by       VARCHAR(50) DEFAULT current_user                                                   NOT NULL,
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
AFTER UPDATE ON dv_hub_column_rule
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
  satellite_key               INTEGER                  DEFAULT nextval(
      'dv_satellite_key_seq' :: REGCLASS) PRIMARY KEY                                                 NOT NULL,
  hub_key                     INTEGER DEFAULT 0                                                       NOT NULL,
  link_key                    INTEGER DEFAULT 0                                                       NOT NULL,
  link_hub_satellite_flag     CHAR DEFAULT 'H' :: bpchar                                              NOT NULL,
  satellite_name              VARCHAR(128)                                                            NOT NULL,
  satellite_schema            VARCHAR(128)                                                            NOT NULL,
  is_retired                  BOOLEAN DEFAULT FALSE                                                   NOT NULL,
  release_key                 INTEGER DEFAULT 1                                                       NOT NULL,
  owner_key                   INTEGER DEFAULT 1                                                       NOT NULL,
  version_number              INTEGER DEFAULT 1                                                       NOT NULL,
  updated_by                  VARCHAR(50) DEFAULT current_user                                        NOT NULL,
  updated_datetime            TIMESTAMP WITH TIME ZONE DEFAULT now(),
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
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
  column_type      VARCHAR(50) ,
  column_length    INT ,
  column_precision INT ,
  column_scale     INT ,
  is_nullable int,
  is_key int,
  is_indexed int
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

raise NOTICE 'Column defenition % -->',result_v;

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
  sql_v           VARCHAR(2000);
  sql_select_v    VARCHAR(2000);
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
      sql_select_v:=sql_select_v
                    || ' cast('
                    || quote_literal(column_value_v)
                    || ' as '
                    || column_type_v
                    || ')';

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
          ELSE 0 END                                        AS is_key,
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
        WHERE is_key=0  AND is_no_update =0 AND column_name = object_settings_in [i] [1];

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
  rowcount_v   INTEGER :=0;
  key_column_v VARCHAR(50);
  sql_v        VARCHAR(2000);
  object_schema_v varchar(50):='ore_config';
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
   object_schema_in || '.' ||object_name_in || newline_v || '(';
  sql_create_index_v:=replace(sql_create_index_v,'-','_');

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
  object_name_in      VARCHAR(128),
  object_schema_in    VARCHAR(128),
  recreate_flag_in    CHAR(1) = 'N'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  rowcount_v         INT :=0;
  sql_v              TEXT;
  sql_create_table_v TEXT;
  sql_create_index_v TEXT;
  hub_name_v varchar(200);

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
                     1 as is_indexed
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
  hub_name_v:=fn_get_object_name(object_name_in,'hub');

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

CREATE OR REPLACE FUNCTION dv_config_dv_load_hub(
  stage_table_schema_in VARCHAR(128),
  stage_table_name_in   VARCHAR(128),
  hub_schema_in         VARCHAR(128),
  hub_name_in           VARCHAR(128)
)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_block_start_v    TEXT;
  sql_block_end_v      TEXT;
  sql_block_body_v     TEXT;
  sql_process_start_v  TEXT;
  sql_process_finish_v TEXT;
  delimiter_v          CHAR(2) :=',';
  newline_v            CHAR(3) :=E'\n';
  load_date_time_v          VARCHAR(10):='now()';
  hub_name_v varchar(50);
BEGIN
/*-----TO DO add error handling generation if load failed checks on counts
  */

  -- hub name check
  hub_name_v:= fn_get_object_name(hub_name_in, 'hub');

  IF COALESCE(hub_name_v, '') = ''
  THEN
    RAISE NOTICE 'Not valid hub name --> %', hub_name_in;
    RETURN ;
  END IF;


  -- code snippets
  sql_block_start_v:='DO $$' || newline_v || 'begin' || newline_v;
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



  -- dynamic upsert statement
  -- add process status select-update in transaction
  WITH sql AS (
    SELECT
      stc.column_name            stage_col_name,
      hkc.hub_key_column_name    hub_col_name,
      hkc.hub_key_column_type AS column_type
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
      column_type
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
         SELECT DISTINCT ' from ' || stage_table_schema_in || '.' || stage_table_name_in || ' where status=' ||
                         quote_literal('PROCESSING') || ')'
         FROM sql
         UNION ALL
         SELECT 'insert into ' || hub_schema_in || '.' || hub_name_v || '(' ||
                array_to_string(array_agg(sql.hub_col_name), ', ') || ')'
         FROM sql
         -- GROUP BY sql.hub_schema, fn_get_object_name(sql.hub_name, 'hub')
         UNION ALL
         SELECT DISTINCT 'select * from src' || E'\n' || 'on conflict(' || (SELECT column_name
                                                                            FROM fn_get_dv_object_default_columns(
                                                                                hub_name_in,
                                                                                'hub',
                                                                                'Object_Key')) ||
                         ') ' || 'do nothing;' || E'\n'
         FROM sql) t
  INTO sql_block_body_v;

  RETURN sql_block_start_v || sql_process_start_v || sql_block_body_v || sql_process_finish_v || sql_block_end_v;

END
$BODY$
LANGUAGE plpgsql;

DO $$
BEGIN
  RAISE NOTICE 'Creating dv_config_dv_load_satellite...';
END
$$;

CREATE OR REPLACE FUNCTION dv_config_dv_load_satellite(
  stage_table_schema_in VARCHAR(128),
  stage_table_name_in   VARCHAR(128),
  satellite_schema_in   VARCHAR(128),
  satellite_name_in     VARCHAR(128),
  load_type_in          VARCHAR(10) DEFAULT 'delta'
)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_block_start_v    TEXT;
  sql_block_end_v      TEXT;
  sql_block_body_v     TEXT;
  sql_process_start_v  TEXT;
  sql_process_finish_v TEXT;
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

  IF COALESCE(satellite_name_v, '') = '' or COALESCE(hub_name_v, '')=''
  THEN
    RAISE NOTICE 'Not valid satellite name --> %', satellite_name_in;
    RETURN ;
  END IF;

  -- code snippets
  -- block
  sql_block_start_v:='DO $$' || newline_v || 'begin' || newline_v;
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
      0                  is_default
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
      1
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
      0
    FROM dv_satellite s
      JOIN dv_hub h ON h.hub_key = s.hub_key
      JOIN fn_get_dv_object_default_columns(h.hub_name, 'hub') c ON 1 = 1
    WHERE s.owner_key = h.owner_key
          AND c.is_key = 1)
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
         UNION ALL
         SELECT DISTINCT ', h.' || sql.sat_col_name || ' as hub_SK ' || ' from ' || stage_table_schema_in || '.' ||
                         stage_table_name_in
                         || ' as s left join ' || hub_schema_v ||
                         '.' ||
                         hub_name_v ||
                         ' as h '
                         ' on s.' || sql.sat_col_name || '=h.' || sql.sat_col_name ||
                         ' where s.status=' ||
                         quote_literal('PROCESSING')
         FROM sql
         WHERE sql.is_surrogate_key = 1
         UNION ALL
         -- except statement : checking source to exclude duplicates in time series
         SELECT ' except  select ' || array_to_string(
             array_agg(CASE WHEN sql.is_default = 0
               THEN sql.sat_col_name
                       ELSE sql.stage_col_name END),
             ', ')
         FROM sql
         UNION ALL
         SELECT ', ' || sat_col_name || ' from ' || satellite_schema_in || '.' || satellite_name_v || ' where ' ||
                ' dv_row_is_current=1 ),'
         FROM sql
         WHERE
           sql.is_surrogate_key = 1
         UNION ALL
         -- full load - mark all keys that not found in stage as deleted
         -- lookup key values
         SELECT DISTINCT CASE WHEN load_type_in = 'delta'
           THEN ' '
                         ELSE
                           '  deleted as ( update ' || satellite_schema_in || '.' || satellite_name_v
                           ||
                           ' as s  set s.dv_rowenddate=' || load_date_time_v ||
                           ' from src  where ' ||
                           -- list of lookup columns
                           array_to_string(array_agg(' s.' || sql.sat_col_name || '=src.' || sql.stage_col_name),
                                           ' and ')
                           || ' and src.hub_SK is null and s.dv_row_is_current=1 ), '
                         END
         FROM sql
         WHERE sql.hub_key_column_name IS NOT NULL
         UNION ALL
         -- update row if key is found
         SELECT ' updates as ( update ' || satellite_schema_in || '.' || satellite_name_v
         UNION ALL
         SELECT 'as u  set u.dv_row_is_current=0,u.dv_rowenddate=' || load_date_time_v
         UNION ALL
         SELECT ' from src '
         UNION ALL
         SELECT ' where u.' || sql.sat_col_name || '=src.' || sql.sat_col_name ||
                ' and src.hub_SK is not null and u.dv_row_is_current=1 ' || E'\n returning src.* )'
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
                ||
                ' from updates u union all select ' || array_to_string(array_agg(sql.sat_col_name), ', ') ||
                ' from src where src.hub_SK is not null ) r '
         FROM sql
         UNION ALL
         SELECT ' ;'

       ) t
  INTO sql_block_body_v;


  RETURN sql_block_start_v || sql_process_start_v || sql_block_body_v || sql_process_finish_v || sql_block_end_v;

END
$BODY$
LANGUAGE plpgsql;


/************************* default values setup **********************************************************************/

DO $$
DECLARE
  owner_key_v   INT;
  release_key_v INT;
  cnt_v         INT;
  release_v     VARCHAR(50);

BEGIN
  RAISE NOTICE 'Configuring default values...';

  -- default owner
  SELECT dv_config_object_insert('dv_owner',
                                 '{{"owner_name","default"},{"owner_description","default owner"}')
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
        r.*,
        owner_key_v,
        release_key_v
      FROM (
             SELECT
               'hub',
               'filegroup',
               1,
               'varchar',
               NULL,
               'primary',
               NULL
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
               0
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
               0
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
               0
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
               0
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
               0
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
               0
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
               0
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
               0
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
               1
             UNION ALL
             SELECT
               'stage_table',
               'process_status',
               0,
               NULL,
               'status',
               NULL,
               'varchar',
               0,
               0,
               0,
               NULL,
               TRUE,
               FALSE,
               FALSE,
               0
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
               0) r;


  ELSE
    RAISE NOTICE 'Something went wrong...';
    RETURN;
  END IF;

   RAISE NOTICE 'Completed...';

END;
$$;





