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
  schedule_key         INTEGER                  DEFAULT nextval(
      'dv_schedule_seq' :: REGCLASS) PRIMARY KEY                                                           NOT NULL,
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
  schedule_task_key INTEGER                  DEFAULT nextval(
      'dv_schedule_task_seq' :: REGCLASS) PRIMARY KEY                                                        NOT NULL,
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
  task_run_key      INTEGER DEFAULT nextval('dv_task_run_seq' :: REGCLASS) PRIMARY KEY    NOT NULL,
  schedule_key      INTEGER                                                               NOT NULL,
  schedule_task_key INTEGER                                                               NOT NULL,
  task_run_status   VARCHAR(30)                                                           NOT NULL,
  start_datetime    TIMESTAMP,
  finish_datetime   TIMESTAMP,
  owner_key         INTEGER DEFAULT 1                                                     NOT NULL,
  CONSTRAINT fk_dv_task_run_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_task_run_dv_schedule_task FOREIGN KEY (schedule_task_key) REFERENCES dv_schedule_task (schedule_task_key),
  CONSTRAINT fk_dv_task_run_dv_schedule FOREIGN KEY (schedule_key) REFERENCES dv_schedule (schedule_key)
);

CREATE UNIQUE INDEX dv_task_run_unq
  ON dv_task_run (owner_key, schedule_key, schedule_task_key);

CREATE SEQUENCE dv_task_run_history_seq START 1;
CREATE TABLE dv_task_run_history
(
  task_run_history_key INTEGER                  DEFAULT nextval(
      'dv_task_run_history_seq' :: REGCLASS) PRIMARY KEY    NOT NULL,
  schedule_key         INTEGER                              NOT NULL,
  schedule_task_key    INTEGER                              NOT NULL,
  task_run_status      VARCHAR(30)                          NOT NULL,
  start_datetime       TIMESTAMP,
  finish_datetime      TIMESTAMP,
  updated_datetime     TIMESTAMP WITH TIME ZONE DEFAULT now(),
  owner_key            INTEGER DEFAULT 1                    NOT NULL,
  CONSTRAINT fk_dv_task_run_history_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_task_run_history_dv_schedule_task FOREIGN KEY (schedule_task_key) REFERENCES dv_schedule_task (schedule_task_key)

);


CREATE TABLE dv_object_load_state
(
  object_key       INTEGER,
  object_type      INTEGER,
  load_state       VARCHAR(30),
  updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
  owner_key        INTEGER
);

CREATE SEQUENCE dv_object_load_state_history_seq START 1;
CREATE TABLE dv_object_load_state_history
(
  history_key      INTEGER,
  object_key       INTEGER,
  object_type      INTEGER,
  load_state       VARCHAR(30),
  updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
  owner_key        INTEGER
);


-- queue for tasks
-- we need to handle if one process is finished be able to pick up next WAITING for execution

-- tasks to execute (script)
-- prepare for parallel run

SELECT t.schedule_key, t.schedule_name, t.owner_key,
       t.schedule_frequency, t.schedule_task_key, t.parent_task_key,
       t.object_key,
       t.object_type,
       t.load_type,

FROM
  (
    SELECT
      s.schedule_key,
      s.owner_key,
      s.schedule_name,
      s.schedule_frequency,
      st.schedule_task_key,
      st.object_key,
      st.object_type,
      st.load_type,
      coalesce(sth.schedule_parent_task_key, -1000) AS parent_task_key
    FROM dv_schedule s
      JOIN dv_schedule_task st ON s.schedule_key = st.schedule_key
      LEFT JOIN dv_schedule_task_hierarchy sth ON sth.schedule_task_key = st.schedule_task_key
    WHERE s.is_cancelled = 0 AND st.is_cancelled = 0 AND sth.is_cancelled = 0
  ) t
  LEFT JOIN dv_business_rule b
    ON b.business_rule_key = t.object_key AND t.object_type = 'BUSINESS_RULE' AND b.owner_key = t.owner_key and b.is_retired=0
  LEFT JOIN

    (
      select * from
    dv_stage_table st  ) st
    ON st.stage_table_key = t.object_key AND t.object_type = 'STAGE_TABLE' AND b.owner_key = st.owner_key
  LEFT JOIN dv_source_table ss
    ON ss.source_table_key = t.object_key AND t.object_type = 'SOURCE_TABLE' AND b.owner_key = ss.owner_key and ss.is_retired=0


-- pull of objects to be loaded
-- hubs, links, satellites = need to add all these to hierarchy
-- stage (via business rules), source (external system)






-- stage table load
select business_rule_key as object_key,  business_rule_name as object_name, business_rule_logic as logic,'BUSINESS_RULE' as object_type
  from dv_business_rule b
where b.is_retired=0
union ALL
    -- source table load
select ss.source_table_key, source_table_schema||'.'||source_table_name, '' as logic, 'SOURCE_TABLE'
  from
    dv_source_table ss
WHERE ss.is_retired=0
union ALL
    -- load hub
select distinct h.hub_key,h.hub_schema||'.'||h.hub_name, ||'dv_config_dv_load_hub'
  from dv_hub h join dv_hub_key_column hk on h.hub_key = hk.hub_key
       join dv_hub_column hc on hc.hub_key_column_key=hk.hub_key_column_key
       join dv_stage_table_column sc on sc.column_key=hc.column_key
       join dv_stage_table st on st.stage_table_key=sc.stage_table_key


-- procedure to generate executable statement for load of any type

CREATE OR REPLACE FUNCTION fn_get_dv_object_load_script(object_key_in VARCHAR(100), object_type_in VARCHAR(50)
)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_v TEXT;
BEGIN

  case object_type_in
    when 'business_rule' then
  -- 1. business_rule/ stage table


    when 'source_table' then
  -- 2. source - nothing

    when 'hub' then
  -- 3. hub

    when 'satellite' THEN

  -- 4. satellite
    ELSE
      sql_v:='';
      end case;


  RETURN sql_v;

END
$BODY$
LANGUAGE plpgsql;