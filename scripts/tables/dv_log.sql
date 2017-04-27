CREATE SEQUENCE dv_log_id_seq START 1;


CREATE TABLE dv_log
(
    id INTEGER DEFAULT nextval('dv_log_id_seq'::regclass) PRIMARY KEY NOT NULL,
    log_datetime TIMESTAMP,
    log_proc TEXT,
    message TEXT
);