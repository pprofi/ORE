CREATE SEQUENCE dv_job_id_seq START 1;

CREATE TABLE dv_schedule_task_queue
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
  owner_key         INT
);

CREATE TRIGGER dv_schedule_task_queue_tgu
AFTER UPDATE ON ore_config.dv_schedule_task_queue
FOR EACH ROW EXECUTE PROCEDURE ore_config.dv_init_schedule_task_run();


