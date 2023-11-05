CREATE TABLE dv_schedule_task_queue_history
(
  job_id            INT,
  schedule_key      INT,
  schedule_task_key INT,
  parent_task_key   INT,
  task_level        INT,
  process_status    VARCHAR(50),
  script            TEXT,
  exec_type varchar(30),
  start_datetime    TIMESTAMP,
  update_datetime   TIMESTAMP,
  owner_key         INT,
  insert_datetime   TIMESTAMP
);