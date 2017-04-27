CREATE SEQUENCE dv_schedule_task_hierarchy_seq START 1;
CREATE TABLE dv_schedule_task_hierarchy
(
  schedule_task_hierarchy_key INTEGER                  DEFAULT nextval(
      'dv_schedule_task_hierarchy_seq' :: REGCLASS) PRIMARY KEY    NOT NULL,
  schedule_task_key           INTEGER                              NOT NULL,
  schedule_parent_task_key    INTEGER,
  is_cancelled                BOOLEAN DEFAULT FALSE                NOT NULL,
  release_key                 INTEGER DEFAULT 1                    NOT NULL,
  owner_key                   INTEGER DEFAULT 1                    NOT NULL,
  version_number              INTEGER DEFAULT 1                    NOT NULL,
  updated_by                  VARCHAR(50) DEFAULT "current_user"() NOT NULL,
  updated_datetime            TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_schedule_task_hierarchy_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_schedule_task_hierarchy_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_schedule_task_hierarchy_dv_schedule_task FOREIGN KEY (schedule_task_key) REFERENCES dv_schedule_task (schedule_task_key)
);

CREATE UNIQUE INDEX dv_schedule_task_hierarchy_unq
  ON dv_schedule_task_hierarchy (owner_key, schedule_task_key, schedule_parent_task_key);