/*---- source system capture -------------*/

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