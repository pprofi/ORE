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


CREATE TABLE dv_schedule
(
    schedule_key INTEGER DEFAULT nextval('dv_schedule_seq'::regclass) PRIMARY KEY NOT NULL,
    schedule_name VARCHAR(128) NOT NULL,
    schedule_description VARCHAR(500),
    schedule_frequency INTERVAL,
    start_date TIMESTAMP DEFAULT now(),
    last_start_date TIMESTAMP,
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