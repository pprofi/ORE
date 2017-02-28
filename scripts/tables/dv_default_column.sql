-- default columns
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