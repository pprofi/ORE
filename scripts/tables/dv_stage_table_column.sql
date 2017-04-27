-- stage table columns
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
CREATE TRIGGER dv_stage_table_column_audit
AFTER UPDATE ON dv_stage_table_column
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE dv_config_audit();