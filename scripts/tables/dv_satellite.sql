-- config satellite

CREATE SEQUENCE dv_satellite_key_seq START 1;

CREATE TABLE dv_satellite
(
  satellite_key               INTEGER                  DEFAULT nextval(
      'dv_satellite_key_seq' :: REGCLASS) PRIMARY KEY                                                 NOT NULL,
  hub_key                     INTEGER DEFAULT 0                                                       NOT NULL,
  link_key                    INTEGER DEFAULT 0                                                       NOT NULL,
  link_hub_satellite_flag     CHAR DEFAULT 'H' :: bpchar                                              NOT NULL,
  satellite_name              VARCHAR(128)                                                            NOT NULL,
  satellite_schema            VARCHAR(128)                                                            NOT NULL,
  is_retired                  BOOLEAN DEFAULT FALSE                                                   NOT NULL,
  release_key                 INTEGER DEFAULT 1                                                       NOT NULL,
  owner_key                   INTEGER DEFAULT 1                                                       NOT NULL,
  version_number              INTEGER DEFAULT 1                                                       NOT NULL,
  updated_by                  VARCHAR(50) DEFAULT current_user                                        NOT NULL,
  updated_datetime            TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_satellite_dv_hub FOREIGN KEY (hub_key) REFERENCES dv_hub (hub_key),
  CONSTRAINT fk_dv_satellite_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_satellite_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_satellite_unq
  ON dv_satellite (owner_key, satellite_schema, satellite_name);

-- audit
CREATE TRIGGER dv_satellite_audit
AFTER UPDATE ON dv_satellite
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE dv_config_audit();