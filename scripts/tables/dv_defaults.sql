-- defaults
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