/*

ORE scheduler (execution engine) v1.0

no frequency involved
only triggering even sequences

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

-- tasks queue

CREATE TABLE dv_schedule_task_queue
(
  job_id            INT,
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

-- task queue history
CREATE TABLE dv_schedule_task_queue_history
(
  job_id            INT,
  schedule_key      INT,
  schedule_task_key INT,
  parent_task_key   INT,
  task_level        INT,
  process_status    VARCHAR(50),
  script            TEXT,
  start_datetime    TIMESTAMP,
  update_datetime   TIMESTAMP,
  owner_key         INT,
  insert_datetime   TIMESTAMP
);


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


-- list of valid schedule tasks & execution sctipts
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


-- for external call
-- updates source load status and triggers the schedule execution
CREATE OR REPLACE FUNCTION dv_load_source_status_update(job_id_in       INT, owner_name_in VARCHAR(100),
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
  -- updates first task to trigger schedule execution
  UPDATE dv_schedule_task_queue
  SET process_status = 'done', update_datetime = now()
  FROM t
  WHERE job_id = job_id_in AND schedule_key = src.schedule_key AND schedule_task_key = src.task_key
        AND NOT exists(SELECT 1
                       FROM dv_schedule_task_queue d
                       WHERE d.schedule_key = d.schedule_key AND d.job_id <> job_id AND
                             d.process_status IN ('queued', 'processing'));

  -- need to check if there is another job for this schedule is running and update status appropriately
END
$BODY$
LANGUAGE plpgsql;


-- runs next task
CREATE OR REPLACE FUNCTION dv_run_next_schedule_task(job_id_in INT, schedule_key_in INT, parent_task_key_in INT)
  RETURNS INT
AS $body$
DECLARE
  exec_script_v  TEXT;
  task_key_v     INT;
  job_id_v       INT :=job_id_in;
  schedule_key_v INT :=schedule_key_in;
  status_v       VARCHAR :='done';
BEGIN

  -- identify first task to run

  SELECT
    coalesce(min(schedule_task_key), -1),
    script
  INTO task_key_v, exec_script_v
  FROM dv_schedule_task_queue
  WHERE job_id = job_id_in AND schedule_key = schedule_key_in AND parent_task_key = parent_task_key_in;

  IF task_key_v <> -1
  THEN

    -- separate transaction for error handling
    BEGIN

      status_v:='processing';

      UPDATE dv_schedule_task_queue
      SET process_status = status_v, update_datetime = now()
      WHERE job_id = job_id_v AND schedule_key = schedule_key_in AND schedule_task_key = task_key_v;

      -- run execute statement if successfull then done
      EXECUTE exec_script_v;
      EXCEPTION WHEN OTHERS
      THEN
        -- if fails update status
        status_v:='failed';
    END;
  ELSE
    -- if no child tasks check if there are another jobs for this schedule queued minimum of jobs_id
    SELECT
      coalesce(schedule_task_key, -1),
      coalesce(job_id, -1)
    INTO task_key_v, job_id_v
    FROM (
           SELECT
             schedule_task_key,
             job_id,
             ROW_NUMBER()
             OVER (
               ORDER BY job_id ASC) AS rn
           FROM dv_schedule_task_queue
           WHERE parent_task_key IS NULL AND process_status = 'queued'
                 AND schedule_key = schedule_key_in AND job_id <> job_id_in AND parent_task_key IS NULL) t
    WHERE rn = 1;

  END IF;

  -- update status to done for the next iteration
  -- update status of the task to done or failed depending on execution

  UPDATE dv_schedule_task_queue
  SET process_status = status_v, update_datetime = now()
  WHERE job_id = job_id_v AND schedule_key = schedule_key_in AND schedule_task_key = task_key_v;

  -- no tasks to run for job_id
  -- clean up and dump all executed tasks into history
  IF task_key_v = -1 OR job_id_v = -1
  THEN

    WITH del AS
    (
      DELETE FROM dv_schedule_task_queue
      WHERE process_status = 'done' AND schedule_key = schedule_key_in AND job_id = job_id_in
      RETURNING *
    )
    INSERT INTO dv_schedule_task_queue_history (job_id,
                                                schedule_key,
                                                schedule_task_key,
                                                parent_task_key,
                                                task_level,
                                                process_status,
                                                script,
                                                start_datetime,
                                                owner_key, insert_datetime)
      SELECT
        job_id,
        schedule_key,
        schedule_task_key,
        parent_task_key,
        task_level,
        process_status,
        script,
        start_datetime,
        owner_key,
        now()
      FROM del;
  END IF;

  RETURN 1;
END
$body$
LANGUAGE plpgsql;


-- trigger function to run next task

CREATE OR REPLACE FUNCTION dv_init_schedule_task_run()
  RETURNS TRIGGER
AS $body$
DECLARE
  result_v INT;
BEGIN
  IF new.process_status = 'done'
  THEN

    SELECT dv_run_next_schedule_task(new.job_id, new.schedule_key, new.parent_task_key)
    INTO result_v;

  END IF;
  RETURN NULL;
END
$body$
LANGUAGE plpgsql;

-- update very first task state into processing
-- this triggers procedure to execute individual task, then updates to completed each tasks
-- this should fire next task to execute
-- use min job_id for particular schedule to execute

CREATE TRIGGER dv_schedule_task_queue_tgu
AFTER UPDATE ON dv_schedule_task_queue
FOR EACH ROW EXECUTE PROCEDURE dv_init_schedule_task_run();
