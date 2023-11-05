CREATE FUNCTION dv_load_source_status_update (owner_name_in character varying, system_name_in character varying, table_schema_in character varying, table_name_in character varying) RETURNS void
	LANGUAGE plpgsql
AS $$
DECLARE
  owner_key_v          INTEGER;
  start_time_v         TIMESTAMP;
  process_status_v     VARCHAR(20) :='queued';
  schedule_key_v       INT;
  state_v              VARCHAR;
  proc_v               VARCHAR(50) :='dv_load_source_status_update';
  job_id_v             INT;
BEGIN

  SET SEARCH_PATH TO ore_config;

  -- generate job_id
  SELECT nextval('dv_job_id_seq' :: REGCLASS)
  INTO job_id_v;

  SELECT dv_log_proc(proc_v, 'Starting execution of job_id-->' ||job_id_v )
    INTO state_v;

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
            AND st.source_table_name = table_name_in)
  INSERT INTO dv_schedule_task_queue (job_id,
                                      schedule_key,
                                      schedule_task_key,
                                      parent_task_key,
                                      task_level,
                                      process_status,
                                      script,
                                      exec_type,
                                      start_datetime,
                                      owner_key)
    SELECT
      job_id_v,
      v.schedule_key,
      v.schedule_task_key,
      v.parent_task_key,
      v.task_level,
      process_status_v,
      v.load_script AS script,
      v.object_type,
      start_time_v,
      v.owner_key
    FROM dv_schedule_valid_tasks v
      JOIN src ON v.schedule_key = src.schedule_key;

  -- updates first task to trigger next job_id for the same schedule
  -- need to check if there is another job for this schedule is running and update status appropriately
  UPDATE dv_schedule_task_queue q
  SET process_status = 'done', update_datetime = now()
  WHERE q.job_id = job_id_v
        AND q.parent_task_key IS NULL
        AND NOT exists(SELECT 1
                       FROM dv_schedule_task_queue d
                       WHERE d.schedule_key = q.schedule_key AND d.job_id <> job_id_v AND
                             d.process_status IN ('queued', 'processing')
                             AND d.start_datetime < q.start_datetime
  );

SELECT dv_log_proc(proc_v, 'Finished execution of job_id-->' ||job_id_v )
    INTO state_v;

END
$$
