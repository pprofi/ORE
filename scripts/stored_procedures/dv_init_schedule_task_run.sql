CREATE OR REPLACE FUNCTION dv_init_schedule_task_run()
  RETURNS TRIGGER
AS $body$
DECLARE
  result_v INT;
  proc_v   VARCHAR(50) :='dv_init_schedule_task_run';
  state_v  VARCHAR;
BEGIN


  IF new.process_status = 'done'
  THEN

    SELECT dv_log_proc(proc_v, 'Schedule-->' || new.schedule_key || '; job_id-->' || new.job_id || ';task_key-->' ||
                               new.schedule_task_key)
    INTO state_v;

    -- run next task
    SELECT dv_run_next_schedule_task(new.job_id, new.schedule_key, new.schedule_task_key)
    INTO result_v;

  END IF;
  RETURN NULL;
END
$body$
LANGUAGE plpgsql;