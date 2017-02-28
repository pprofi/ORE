/*------ dv owner capture ---------------*/
CREATE SEQUENCE dv_owner_key_seq START 1;

CREATE TABLE dv_owner
(
  owner_key         INTEGER                  DEFAULT nextval('dv_owner_key_seq' :: REGCLASS) PRIMARY KEY NOT NULL,
  owner_name        VARCHAR(256),
  owner_description VARCHAR(256),
  is_retired        BOOLEAN DEFAULT FALSE                                                                NOT NULL,
  version_number    INTEGER DEFAULT 1                                                                    NOT NULL,
  updated_by        VARCHAR(50) DEFAULT current_user                                                     NOT NULL,
  updated_datetime  TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE UNIQUE INDEX dv_owner_name_unq
  ON dv_owner (owner_name);

-- audit
CREATE TRIGGER dv_owner_audit
AFTER UPDATE ON dv_owner
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE dv_config_audit();