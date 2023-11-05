/*----- release management capture --------------*/

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