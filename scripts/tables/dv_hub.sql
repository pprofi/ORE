-- hub config
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