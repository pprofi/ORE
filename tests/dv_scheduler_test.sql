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
  schedule_frequency   INTERVAL,
  start_date           TIMESTAMP                DEFAULT now(),
  last_start_date      TIMESTAMP,
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
  session_id        varchar(300),
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
  session_id           varchar(300),
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
CREATE OR REPLACE VIEW dv_schedule_valid_tasks AS
  SELECT
    t.schedule_key,
    t.schedule_name,
    t.owner_key,
    t.schedule_frequency,
    t.schedule_task_key,
    t.parent_task_key,
    t.depth                                                                             AS task_level,
    t.object_key,
    t.object_type,
    t.load_type,
    fn_get_dv_object_load_script(t.object_key, t.object_type, t.load_type, t.owner_key) AS load_script
  FROM
    (
      SELECT
        s.schedule_key,
        s.owner_key,
        s.schedule_name,
        s.schedule_frequency,
        s.start_date,
        st.schedule_task_key,
        sth.parent_task_key,
        sth.depth,
        st.object_key,
        st.object_type,
        st.load_type
      FROM dv_schedule s
        JOIN dv_schedule_task st ON s.schedule_key = st.schedule_key
        JOIN
        (
          WITH RECURSIVE node_rec AS
          (
            SELECT
              1                        AS depth,
              schedule_task_key        AS task_key,
              schedule_parent_task_key AS parent_task_key
            FROM dv_schedule_task_hierarchy
            WHERE schedule_parent_task_key IS NULL AND is_cancelled = FALSE
            UNION ALL
            SELECT
              depth + 1,
              n.schedule_task_key        AS task_key,
              n.schedule_parent_task_key AS parent_task_key
            FROM dv_schedule_task_hierarchy AS n
              JOIN node_rec r ON n.schedule_parent_task_key = r.task_key
            WHERE n.is_cancelled = FALSE
          )
          SELECT
            depth,
            task_key,
            parent_task_key
          FROM node_rec
        )
        sth ON sth.task_key = st.schedule_task_key
      WHERE s.is_cancelled = FALSE AND st.is_cancelled = FALSE
    ) t;

-- procedure to generate executable statement for load of any type

CREATE OR REPLACE FUNCTION fn_get_dv_object_load_script(object_key_in INTEGER, object_type_in VARCHAR(50),
                                                        load_type_in  VARCHAR(30), owner_key_in INTEGER)
  RETURNS TEXT AS
$BODY$
DECLARE
  sql_v TEXT;
BEGIN

  CASE object_type_in
    WHEN 'business_rule'
    THEN
      -- 1. business_rule/ stage table
      -- if it is stored procedure then different
      SELECT business_rule_logic
      INTO sql_v
      FROM dv_business_rule
      WHERE business_rule_key = object_key_in
            AND is_retired = 0
            AND owner_key = owner_key_in;

    WHEN 'hub'
    THEN
      -- 2. hub
      SELECT DISTINCT
        'ore_config.dv_config_dv_load_hub(' || st.stage_table_schema || ',' || st.stage_table_name || ',' ||
        h.hub_schema || ',' ||
        h.hub_name || ');'
      INTO sql_v
      FROM dv_hub h
        JOIN dv_hub_key_column hk ON h.hub_key = hk.hub_key
        JOIN dv_hub_column hc ON hc.hub_key_column_key = hk.hub_key_column_key
        JOIN dv_stage_table_column sc ON sc.column_key = hc.column_key
        JOIN dv_stage_table st ON st.stage_table_key = sc.stage_table_key
      WHERE h.owner_key = owner_key_in AND h.is_retired = 0 AND st.is_retired = 0 AND sc.is_retired = 0
            AND h.hub_key = object_key_in;
    WHEN 'satellite'
    THEN
      -- 3. satellite
      SELECT DISTINCT
        'ore_config.dv_config_dv_load_satellite(' || st.stage_table_schema || ',' || st.stage_table_name || ',' ||
        s.satellite_schema || ',' || s.satellite_name || ',' || load_type_in || ');'
      INTO sql_v
      FROM dv_satellite s
        JOIN dv_satellite_column sc ON sc.satellite_key = s.satellite_key
        JOIN dv_stage_table_column stc ON stc.column_key = sc.column_key
        JOIN dv_stage_table st ON st.stc.stage_table_key = st.stage_table_key
      WHERE s.is_retired = 0 AND st.is_retired = 0 AND stc.is_retired = 0
            AND s.owner_key = owner_key_in
            AND s.satellite_key = object_key_in;
  ELSE
    -- 4. source or anything else -  nothing
    sql_v:='';
  END CASE;


  RETURN sql_v;

END
$BODY$
LANGUAGE plpgsql;

-- process is running and picks up for execution some tasks
-- schedule executed if all tasks are in completed stated

-- queued
-- waiting
-- processing
-- completed
-- failed

CREATE TABLE dv_schedule_task_queue
(
  session_id        VARCHAR(300) NOT NULL,
  schedule_key      INT,
  schedule_task_key INT,
  parent_task_key   INT,
  task_level        INT,
  process_status    VARCHAR(50),
  script            TEXT,
  start_datetime    TIMESTAMP,
  update_datetime   TIMESTAMP,
  owner_key         INT
);

-- wrapper procedure to start and restart  dv_scheduler_run externally
CREATE OR REPLACE FUNCTION dv_scheruler_init()
  RETURNS INT AS
$BODY$
BEGIN
-- db connect
  -- dblink
  -- execute dv_scheduler_run
  -- disconnect
END;
$BODY$
LANGUAGE plpgsql;
--

CREATE OR REPLACE FUNCTION dv_scheduler_run()
  RETURNS VOID AS
$BODY$
DECLARE
  start_time_v     TIMESTAMP;
  session_id_v     VARCHAR(100);
  process_status_v VARCHAR(20) :='queued';
BEGIN

  -- procedure checks tasks and add tasks which should be run into queue to be executed
  -- separate procedure-process checks queue and executes individual tasks, updates states of tasks
  -- never ending cycle
  WHILE TRUE LOOP


    start_time_v:=now();
    session_id_v:=public.uuid_generate_v4();
    -- select tasks for execution
    -- queued process state
    INSERT INTO dv_schedule_task_queue (session_id,
                                        schedule_key,
                                        schedule_task_key,
                                        parent_task_key,
                                        task_level,
                                        process_status,
                                        script,
                                        start_datetime,
                                        owner_key)
      SELECT
        session_id_v,
        s.schedule_key,
        v.schedule_task_key,
        v.parent_task_key,
        v.task_level,
        process_status_v,
        v.load_script AS script,
        start_time_v,
        v.owner_key
      FROM dv_schedule s
        JOIN dv_schedule_valid_tasks v ON s.schedule_key = v.schedule_key
      --  left join dv_task_run r on
      WHERE is_cancelled = FALSE AND start_time_v >= coalesce(last_start_date, start_date) + s.schedule_frequency
            AND start_time_v < coalesce(last_start_date, start_date) + s.schedule_frequency + INTERVAL '5' SECOND
            AND NOT exists(SELECT 1
                           FROM dv_schedule_task_queue d
                           WHERE
                             d.schedule_key = s.schedule_key AND d.start_datetime > start_time_v - INTERVAL '5' SECOND);

    -- update last run time per schedule
    -- create dblink connection per independent stage & schedule
    -- update statuses of the tasks and source tables then completed load (which is one of the tasks - need to create view
    -- for update
    -- script wrapper for generating code for execution with status records updates
    -- tables cleanup
  END LOOP;
END
$BODY$
LANGUAGE plpgsql;

-- add automatically tasks preprocessing for updating source table processing rows statuses and stage table processing status

CREATE OR REPLACE FUNCTION dv_process_run()
  RETURNS VOID AS
$BODY$
DECLARE
BEGIN

  -- process scanning queue if there tasks to execute
  -- process pulls tasks from queue
  -- checks running status of task and parent task
  -- and executes task
  -- update process status in run task table
  -- deletes task from queue
  -- somewhere should be some kind of parallelizm for different cowners & from staging to hubs and sats

  WHILE TRUE LOOP

  END LOOP;

END
$BODY$
LANGUAGE plpgsql;





-- load source status update

CREATE OR REPLACE FUNCTION dv_load_source_status_update(owner_name_in   VARCHAR(100), system_name_in VARCHAR(100),
                                                        table_schema_in VARCHAR(100),
                                                        table_name_in   VARCHAR(100),
                                                        start_time_in   TIMESTAMP, finish_time_in TIMESTAMP)
  RETURNS VOID AS
$BODY$
DECLARE
  owner_key_v INTEGER;

BEGIN

  -- find owner key
  SELECT owner_key
  INTO owner_key_v
  FROM dv_owner
  WHERE owner_name = owner_name_in;

  -- find related tasks

  WITH t AS (
      SELECT S.schedule_task_key AS task_key
      FROM dv_schedule_task S
        JOIN dv_source_table st ON S.object_key = st.source_table_key
        JOIN dv_source_system ss ON st.system_key = ss.source_system_key
      WHERE S.object_type = 'source' AND S.owner_key = owner_key_v
            AND ss.source_system_name = system_name_in
            AND st.source_table_schema = table_schema_in
            AND st.source_table_name = table_name_in
  )
  UPDATE dv_task_run
  SET start_datetime = coalesce(start_time_in, start_datetime),
    finish_datetime  = coalesce(finish_time_in, finish_datetime),
    task_run_status  = CASE WHEN start_time_in IS NOT NULL AND finish_time_in IS NULL
      THEN 'PROCESSING'
                       WHEN finish_time_in IS NOT NULL
                         THEN 'COMPLETED' END
  FROM t
  WHERE schedule_task_key = t.task_key;

END
$BODY$
LANGUAGE plpgsql;

--/////////////////////////////////////////////////////////////

-- 2. after brain storming session
-- externally on source update insert for particular schedule all task to execute in queued state

CREATE TABLE dv_schedule_task_queue
(
  job_id            VARCHAR(300) NOT NULL,
  schedule_key      INT,
  schedule_task_key INT,
  parent_task_key   INT,
  task_level        INT,
  process_status    VARCHAR(50),
  script            TEXT,
  start_datetime    TIMESTAMP,
  update_datetime   TIMESTAMP,
  owner_key         INT
);


CREATE OR REPLACE FUNCTION dv_load_source_status_update(job_id_in       VARCHAR(100), owner_name_in VARCHAR(100),
                                                        system_name_in  VARCHAR(100),
                                                        table_schema_in VARCHAR(100),
                                                        table_name_in   VARCHAR(100))
  RETURNS VOID AS
$BODY$
DECLARE
  owner_key_v      INTEGER;
  start_time_v     TIMESTAMP;
  process_status_v VARCHAR(20) :='queued';
BEGIN

  start_time_v:=now();
  -- find owner key
  SELECT owner_key
  INTO owner_key_v
  FROM dv_owner
  WHERE owner_name = owner_name_in;

  -- find related schedule tasks for source

  -- select tasks for execution
  -- queued process state
  WITH src AS (
      SELECT
        S.schedule_task_key AS task_key,
        s.schedule_key
      FROM dv_schedule_task S
        JOIN dv_source_table st ON S.object_key = st.source_table_key
        JOIN dv_source_system ss ON st.system_key = ss.source_system_key
      WHERE S.object_type = 'source' AND S.owner_key = owner_key_v
            AND ss.source_system_name = system_name_in
            AND st.source_table_schema = table_schema_in
            AND st.source_table_name = table_name_in),
      t AS (
      INSERT INTO dv_schedule_task_queue (job_id,
                                          schedule_key,
                                          schedule_task_key,
                                          parent_task_key,
                                          task_level,
                                          process_status,
                                          script,
                                          start_datetime,
                                          owner_key)
        SELECT
          job_id_in,
          schedule_key,
          schedule_task_key,
          parent_task_key,
          task_level,
          process_status_v,
          load_script AS script,
          start_time_v,
          owner_key
        FROM dv_schedule_valid_tasks v
        WHERE v.schedule_key = src.schedule_key
    )
  UPDATE dv_schedule_task_queue
  SET process_status = 'processing', update_datetime = now()
  FROM t
  WHERE job_id = job_id_in AND schedule_key = src.schedule_key AND schedule_task_key = src.task_key;


END
$BODY$
LANGUAGE plpgsql;


-- update very first task state into processing
-- this triggers procedure to execute individual task, then updates to completed each tasks
-- this should fire next task to execute
-- use min job_id for particular schedule to execute


-- runs next task

CREATE OR REPLACE FUNCTION dv_run_schedule_task()
  RETURNS TRIGGER
AS $body$
BEGIN
  new.updated_by:=current_user;
  new.updated_datetime:= now();
  RETURN NULL;
END
$body$
LANGUAGE plpgsql;