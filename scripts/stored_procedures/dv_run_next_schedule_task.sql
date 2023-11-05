CREATE OR REPLACE FUNCTION ore_config.dv_run_next_schedule_task(job_id_in          INTEGER, schedule_key_in INTEGER,
                                                                parent_task_key_in INTEGER)
  RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  exec_script_v    TEXT;
  sql_v            TEXT;
  task_key_v       INT;
  job_id_v         INT :=job_id_in;
  schedule_key_v   INT :=schedule_key_in;
  status_v         VARCHAR :='done';
  exec_type_v      VARCHAR(30);
  exec_script_l2_v TEXT;
  state_v          VARCHAR;
  proc_v           VARCHAR(50) :='dv_run_next_schedule_task';
BEGIN

  -- identify first task to run

  SELECT
    coalesce(min(schedule_task_key), -1),
    min(script),
    min(exec_type)
  INTO task_key_v, exec_script_v, exec_type_v
  FROM dv_schedule_task_queue
  WHERE job_id = job_id_in AND parent_task_key = parent_task_key_in AND schedule_key = schedule_key_in;

  SELECT dv_log_proc(proc_v, 'Task to run-->' || task_key_v)
  INTO state_v;

  IF task_key_v <> -1
  THEN

    BEGIN

      -- set status to processing
      status_v:='processing';

      -- update task status
      UPDATE dv_schedule_task_queue
      SET process_status = status_v, update_datetime = now()
      WHERE job_id = job_id_v AND schedule_key = schedule_key_in AND schedule_task_key = task_key_v;

      SELECT dv_log_proc(proc_v,
                         'Executing task-->' || task_key_v || '; type-->' || exec_type_v || '; task script-->' ||
                         exec_script_v)
      INTO state_v;

      -- run statement
      EXECUTE exec_script_v
      INTO exec_script_l2_v;

      -- for any other type than business_rule_proc run second time as first run only generates code to run in that case
      IF exec_type_v <> 'business_rule_proc'
      THEN

        SELECT dv_log_proc(proc_v, 'Executing script-->' || exec_script_l2_v)
        INTO state_v;

        EXECUTE exec_script_l2_v;

      END IF;

      -- set status to DONE if executed successfully
      status_v:='done';

      EXCEPTION WHEN OTHERS
      THEN
        -- in case of failure
        status_v:='failed';
    END;
  ELSE
    -- if no child tasks check if there are another jobs for this schedule queued minimum of jobs_id
    SELECT
      coalesce(min(schedule_task_key), -1),
      coalesce(min(job_id), -1)
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
                                                exec_type,
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
        exec_type,
        start_datetime,
        owner_key,
        now()
      FROM del;
  END IF;

  SELECT dv_log_proc(proc_v, 'Finished execution ....')
  INTO state_v;

  RETURN 1;
END
$$
