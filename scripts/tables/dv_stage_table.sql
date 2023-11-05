/*--- stage table capture -----*/
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