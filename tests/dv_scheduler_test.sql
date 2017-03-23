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

set SEARCH_PATH to ore_config;


create SEQUENCE dv_schedule_seq start 1;

create table dv_schedule
(
    schedule_key INTEGER DEFAULT nextval('dv_schedule_seq'::regclass) PRIMARY KEY NOT NULL,
    schedule_name VARCHAR(128) NOT NULL,
    schedule_description VARCHAR(500),
    schedule_frequency VARCHAR(500),
    is_cancelled BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 1 NOT NULL,
    owner_key INTEGER DEFAULT 1 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT "current_user"() NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_schedule_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_schedule_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_schedule_unq ON dv_schedule (owner_key, schedule_name);

create SEQUENCE dv_schedule_task_seq start 1;
create table dv_schedule_task
(
    schedule_task_key INTEGER DEFAULT nextval('dv_schedule_task_seq'::regclass) PRIMARY KEY NOT NULL,
    schedule_key integer not null,
    object_key integer not null,
    object_type varchar(50) not null,
    load_type varchar(30) not null,
    is_cancelled BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 1 NOT NULL,
    owner_key INTEGER DEFAULT 1 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT "current_user"() NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_schedule_task_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_schedule_task_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key),
  CONSTRAINT fk_dv_schedule_task_dv_schedule FOREIGN KEY (schedule_key) REFERENCES dv_owner (owner_key)

);
CREATE UNIQUE INDEX dv_schedule_task_unq ON dv_schedule_task (owner_key,schedule_key, object_key,object_type,load_type);

create SEQUENCE dv_schedule_tasks_hierarchy_seq start 1;
create table dv_schedule_tasks_hierarchy
(

);

create SEQUENCE dv_run_seq start 1;
create table dv_run
();

create SEQUENCE dv_run_history_seq start 1;
create table dv_run_history
();

create SEQUENCE dv_object_load_state_seq start 1;
create table dv_object_load_state
(

);

create SEQUENCE dv_object_load_state_history_seq start 1;
create table dv_object_load_state_history
(

);


CREATE TABLE dv_hub
(
    hub_key INTEGER DEFAULT nextval('dv_hub_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_name VARCHAR(128) NOT NULL,
    hub_schema VARCHAR(128) NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 1 NOT NULL,
    owner_key INTEGER DEFAULT 1 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT "current_user"() NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_hub_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_hub_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_unq ON dv_hub (owner_key, hub_schema, hub_name);