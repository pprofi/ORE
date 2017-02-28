-- hub column config
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