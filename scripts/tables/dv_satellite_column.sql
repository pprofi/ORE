-- configure sat columns and link them with source columns
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