-- hub key config

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