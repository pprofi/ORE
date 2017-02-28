/*----------- business rule capture ------------ */

CREATE SEQUENCE dv_business_rule_key_seq START 1;

CREATE TABLE dv_business_rule
(
  business_rule_key   INTEGER                  DEFAULT nextval(
      'dv_business_rule_key_seq' :: REGCLASS) PRIMARY KEY                                         NOT NULL,
  stage_table_key     INTEGER                                                                     NOT NULL,
  business_rule_name  VARCHAR(128)                                                                NOT NULL,
  business_rule_type  VARCHAR(20) DEFAULT 'internal_sql' :: CHARACTER VARYING                     NOT NULL,
  business_rule_logic TEXT                                                                        NOT NULL,
  load_type           VARCHAR(50)                                                                 NOT NULL,
  is_external         BOOLEAN DEFAULT FALSE                                                       NOT NULL,
  is_retired          BOOLEAN DEFAULT FALSE                                                       NOT NULL,
  release_key         INTEGER DEFAULT 1                                                           NOT NULL,
  owner_key           INTEGER DEFAULT 1                                                           NOT NULL,
  version_number      INTEGER DEFAULT 1                                                           NOT NULL,
  updated_by          VARCHAR(50) DEFAULT "current_user"()                                        NOT NULL,
  updated_datetime    TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_business_rule_dv_stage_table FOREIGN KEY (stage_table_key) REFERENCES dv_stage_table (stage_table_key),
  CONSTRAINT fk_dv_business_rule_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_business_rule_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_business_rule_key_unq
  ON dv_business_rule (owner_key, stage_table_key, business_rule_name, load_type);

-- audit
CREATE TRIGGER dv_business_rule_audit
AFTER UPDATE ON dv_business_rule
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE dv_config_audit();

