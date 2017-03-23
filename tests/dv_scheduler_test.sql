/*
-- https://gist.github.com/mjgleaso/8031067
-- http://www.leeladharan.com/postgresql-cross-database-queries-using-dblink

SELECT dblink_connect('dbname=postgres');


CREATE EXTENSION dblink;


SELECT pg_namespace.nspname, pg_proc.proname
FROM pg_proc, pg_namespace
WHERE pg_proc.pronamespace=pg_namespace.oid
   AND pg_proc.proname LIKE '%dblink%';


SELECT dblink_connect('dbname=postgres');
SELECT * FROM dblink("SELECT do_stuff_wrapper(0, 5000)") AS t1(c1 TEXT, c2 TEXT, ....);
SELECT * FROM dblink("SELECT do_stuff_wrapper(5001, 5000)") AS t1(c1 TEXT, c2 TEXT, ....);
SELECT * FROM dblink("SELECT do_stuff_wrapper(10001, 5000)") AS t1(c1 TEXT, c2 TEXT, ....);
SELECT dblink_disconnect();
*/

SET SEARCH_PATH TO ore_config;


CREATE SEQUENCE dv_schedule_seq START 1;

CREATE TABLE dv_schedule
(
  schedule_key         INTEGER                  DEFAULT nextval('dv_schedule_seq' :: REGCLASS) PRIMARY KEY NOT NULL,
  schedule_name        VARCHAR(128)                                                                        NOT NULL,
  schedule_description VARCHAR(500),
  schedule_frequency   VARCHAR(500),
  is_cancelled         BOOLEAN DEFAULT FALSE                                                               NOT NULL,
  release_key          INTEGER DEFAULT 1                                                                   NOT NULL,
  owner_key            INTEGER DEFAULT 1                                                                   NOT NULL,
  version_number       INTEGER DEFAULT 1                                                                   NOT NULL,
  updated_by           VARCHAR(50) DEFAULT "current_user"()                                                NOT NULL,
  updated_datetime     TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_schedule_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_schedule_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_schedule_unq
  ON dv_schedule (owner_key, schedule_name);

CREATE SEQUENCE dv_schedule_task_seq START 1;
CREATE TABLE dv_schedule_task
(
  schedule_task_key INTEGER                  DEFAULT nextval('dv_schedule_task_seq' :: REGCLASS) PRIMARY KEY NOT NULL,
  schedule_key      INTEGER                                                                                  NOT NULL,
  object_key        INTEGER                                                                                  NOT NULL,
  object_type       VARCHAR(50)                                                                              NOT NULL,
  load_type         VARCHAR(30)                                                                              NOT NULL,
  is_cancelled      BOOLEAN DEFAULT FALSE                                                                    NOT NULL,
  release_key       INTEGER DEFAULT 1                                                                        NOT NULL,
  owner_key         INTEGER DEFAULT 1                                                                        NOT NULL,
  version_number    INTEGER DEFAULT 1                                                                        NOT NULL,
  updated_by        VARCHAR(50) DEFAULT "current_user"()                                                     NOT NULL,
  updated_datetime  TIMESTAMP WITH TIME ZONE DEFAULT now(),
  CONSTRAINT fk_dv_schedule_task_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_schedule_task_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_schedule_task_dv_schedule FOREIGN KEY (schedule_key) REFERENCES dv_schedule (schedule_key)

);
CREATE UNIQUE INDEX dv_schedule_task_unq
  ON dv_schedule_task (owner_key, schedule_key, object_key, object_type, load_type);

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

CREATE SEQUENCE dv_task_run_seq START 1;
CREATE TABLE dv_task_run
(
  task_run_key INTEGER                  DEFAULT nextval('dv_task_run_seq' :: REGCLASS) PRIMARY KEY    NOT NULL,
  schedule_key  integer not null,
  schedule_task_key           INTEGER                              NOT NULL,
  task_run_status   varchar(30) not null,
  start_datetime timestamp,
  finish_datetime timestamp,
  owner_key                   INTEGER DEFAULT 1                    NOT NULL,
  CONSTRAINT fk_dv_task_run_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_task_run_dv_schedule_task FOREIGN KEY (schedule_task_key) REFERENCES dv_schedule_task (schedule_task_key),
  CONSTRAINT fk_dv_task_run_dv_schedule FOREIGN KEY (schedule_key) REFERENCES dv_schedule (schedule_key)
);

CREATE UNIQUE INDEX dv_task_run_unq
  ON dv_task_run (owner_key, schedule_key, schedule_task_key);

CREATE SEQUENCE dv_task_run_history_seq START 1;
CREATE TABLE dv_task_run_history
(
  task_run_history_key INTEGER                  DEFAULT nextval('dv_task_run_history_seq' :: REGCLASS) PRIMARY KEY    NOT NULL,
  schedule_key  integer not null,
  schedule_task_key           INTEGER                              NOT NULL,
  task_run_status   varchar(30) not null,
  start_datetime timestamp,
  finish_datetime timestamp,
  updated_datetime            TIMESTAMP WITH TIME ZONE DEFAULT now(),
  owner_key                   INTEGER DEFAULT 1                    NOT NULL,
  CONSTRAINT fk_dv_task_run_history_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
  CONSTRAINT fk_dv_task_run_history_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_task_run_history_dv_schedule_task FOREIGN KEY (schedule_task_key) REFERENCES dv_schedule_task (schedule_task_key)

);

CREATE SEQUENCE dv_object_load_state_seq START 1;
CREATE TABLE dv_object_load_state
(

);

CREATE SEQUENCE dv_object_load_state_history_seq START 1;
CREATE TABLE dv_object_load_state_history
(

);


