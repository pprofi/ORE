/*-------------------------------------------
        OPTIMAL REPORTING ENGINE
        CONFIG DB STRUCTURE
-------------------------------------------*/
-- simplified version for one offs
-- multitenant solution
-- one config DB should contain configuration data for DV for many customers

CREATE SCHEMA ore_config;

SHOW search_path;

SET search_path TO ore_config;

-- release
CREATE SEQUENCE dv_release_key_seq START 1;

CREATE TABLE dv_release
(
    release_key INTEGER DEFAULT nextval('dv_release_key_seq'::regclass) PRIMARY KEY NOT NULL,
    release_number INTEGER NOT NULL,
    release_description VARCHAR(256),
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

CREATE UNIQUE INDEX dv_release_number ON dv_release (release_number);


-- dv owner
CREATE SEQUENCE dv_owner_key_seq START 1;

create TABLE dv_owner
(
owner_key INTEGER DEFAULT nextval('dv_owner_key_seq'::regclass) PRIMARY KEY NOT NULL,
owner_name VARCHAR(256),
owner_description VARCHAR(256),
release_key INTEGER DEFAULT 0 NOT NULL,
version_number INTEGER DEFAULT 1 NOT NULL,
updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
CONSTRAINT fk_dv_owner_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key)

);

CREATE UNIQUE INDEX dv_owner_unq ON dv_owner (owner_key,owner_name);

-- source system
CREATE SEQUENCE dv_source_system_key_seq start 1;

CREATE TABLE dv_source_system
(
    source_system_key INTEGER DEFAULT nextval('dv_source_system_key_seq'::regclass) PRIMARY KEY NOT NULL,
    source_system_name VARCHAR(50) NOT NULL,
    source_system_schema VARCHAR(50),
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    update_date_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_source_system_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_source_system_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX source_system_unq ON dv_source_system (owner_key,source_system_name);


-- source table

CREATE SEQUENCE dv_source_table_key_seq start 1;

CREATE TABLE dv_source_table
(
    source_table_key INTEGER DEFAULT nextval('dv_source_table_key_seq'::regclass) PRIMARY KEY NOT NULL,
    system_key INTEGER NOT NULL,
    source_table_schema VARCHAR(128) NOT NULL,
    source_table_name VARCHAR(128) NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    update_date_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_source_table_dv_source_system FOREIGN KEY (system_key) REFERENCES dv_source_system (source_system_key),
    CONSTRAINT fk_dv_source_table_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_source_table_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_source_system_unq ON dv_source_table (owner_key, system_key, source_table_schema, source_table_name);

-- stage table - all get transformed into stage via stored procedures- business rules - separate entity
CREATE SEQUENCE dv_stage_table_key_seq start 1;

CREATE TABLE dv_stage_table
(
    stage_table_key INTEGER DEFAULT nextval('dv_stage_table_key_seq'::regclass) PRIMARY KEY NOT NULL,
    system_key INTEGER NOT NULL,
    stage_table_schema VARCHAR(128) NOT NULL,
    stage_table_name VARCHAR(128) NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    update_date_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_stage_table_dv_source_system FOREIGN KEY (system_key) REFERENCES dv_source_system (source_system_key),
    CONSTRAINT fk_dv_stage_table_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_stage_table_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_stage_system_unq ON dv_stage_table (owner_key, system_key, stage_table_schema, stage_table_name);

-- source table columns
CREATE SEQUENCE dv_stage_table_column_key_seq start 1;

CREATE TABLE dv_stage_table_column
(
    column_key INTEGER DEFAULT nextval('dv_stage_table_column_key_seq'::regclass) PRIMARY KEY NOT NULL,
    stage_table_key INTEGER NOT NULL,
    column_name VARCHAR(128) NOT NULL,
    column_type VARCHAR(30) NOT NULL,
    column_length INTEGER,
    column_precision INTEGER,
    column_scale INTEGER,
    collation_name VARCHAR(128),
    source_ordinal_position INTEGER NOT NULL,
    is_source_date BOOLEAN DEFAULT false NOT NULL,
    discard_flag BOOLEAN DEFAULT false NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    update_date_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_stage_table_column_dv_stage_table FOREIGN KEY (stage_table_key) REFERENCES dv_stage_table (stage_table_key),
    CONSTRAINT fk_dv_stage_table_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_stage_table_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_stage_table_column_unq ON dv_stage_table_column (owner_key,stage_table_key, column_name );


/*

business rule versioning
 */
create sequence dv_business_rule_key_seq start 1;

CREATE TABLE dv_business_rule
(
    business_rule_key INTEGER DEFAULT nextval('dv_business_rule_key_seq'::regclass) PRIMARY KEY NOT NULL,
    stage_table_key INTEGER NOT NULL,
    business_rule_name VARCHAR(128) NOT NULL,
    business_rule_stored_proc_name VARCHAR(256) NOT NULL,
    business_rule_stored_proc_schema VARCHAR(256) NOT NULL,
    business_rule
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_business_rule_dv_stage_table FOREIGN KEY (stage_table_key) REFERENCES dv_stage_table (stage_table_key),
    CONSTRAINT fk_dv_business_rule_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_business_rule_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_business_rule_key_unq ON dv_business_rule (owner_key,release_key,);


-- default columns
CREATE SEQUENCE dv_default_column_key_seq start 1;

CREATE TABLE dv_default_column
(
    default_column_key INTEGER DEFAULT nextval('dv_default_column_key_seq'::regclass) PRIMARY KEY NOT NULL,
    object_type VARCHAR(30) NOT NULL,
    object_column_type VARCHAR(30) NOT NULL,
    ordinal_position INTEGER DEFAULT 0 NOT NULL,
    column_prefix VARCHAR(30),
    column_name VARCHAR(256) NOT NULL,
    column_suffix VARCHAR(30),
    column_type VARCHAR(30) NOT NULL,
    column_length INTEGER,
    column_precision INTEGER,
    column_scale INTEGER,
    collation_name VARCHAR(128),
    is_nullable BOOLEAN DEFAULT true NOT NULL,
    is_pk BOOLEAN DEFAULT false NOT NULL,
    discard_flag BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    update_date_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_default_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_default_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_default_column_unq ON dv_default_column (owner_key, object_type, column_name);

-- set of tables for HUB configuration

-- hub config
create sequence dv_hub_key_seq start 1;

CREATE TABLE dv_hub
(
    hub_key INTEGER DEFAULT nextval('dv_hub_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_name VARCHAR(128) NOT NULL,
    hub_schema VARCHAR(128) NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_hub_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_hub_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);

CREATE UNIQUE INDEX dv_hub_unq ON dv_hub (owner_key, hub_schema, hub_name);

-- hub column config
create sequence dv_hub_column_key_seq start 1;

CREATE TABLE dv_hub_column
(
    hub_column_key INTEGER DEFAULT nextval('dv_hub_column_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_key_column_key INTEGER NOT NULL,
    column_key INTEGER NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_hub_column_dv_hub_key_column FOREIGN KEY (hub_key_column_key) REFERENCES dv_hub_key_column (hub_key_column_key),
    CONSTRAINT fk_dv_hub_column_dv_column FOREIGN KEY (column_key) REFERENCES dv_source_column (column_key),
    CONSTRAINT fk_dv_hub_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_hub_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_column_unq ON dv_hub_column (owner_key,hub_key_column_key, column_key);
CREATE UNIQUE INDEX dv_hub_stage_table_column_unq ON dv_hub_column (owner_key,column_key);

-- hub key config
-- concatenated key?
create sequence dv_hub_key_column_key_seq start 1;

CREATE TABLE dv_hub_key_column
(
    hub_key_column_key INTEGER DEFAULT nextval('dv_hub_key_column_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_key INTEGER NOT NULL,
    hub_key_column_name VARCHAR(128) NOT NULL,
    hub_key_column_type VARCHAR(30) NOT NULL,
    hub_key_column_length INTEGER,
    hub_key_column_precision INTEGER,
    hub_key_column_scale INTEGER,
    hub_key_collation_name VARCHAR(128),
    hub_key_ordinal_position INTEGER DEFAULT 0 NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_hub_key_column_dv_hub FOREIGN KEY (hub_key) REFERENCES dv_hub (hub_key),
    CONSTRAINT fk_dv_hub_key_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_hub_key_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_key_column_unq ON dv_hub_key_column (owner_key,hub_key, hub_key_column_name);

-- config satellite

create sequence dv_satellite_key_seq start 1;

CREATE TABLE dv_satellite
(
    satellite_key INTEGER DEFAULT nextval('dv_satellite_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_key INTEGER DEFAULT 0 NOT NULL,
    link_key INTEGER DEFAULT 0 NOT NULL,
    link_hub_satellite_flag CHAR DEFAULT 'H'::bpchar NOT NULL,
    satellite_name VARCHAR(128) NOT NULL,
    satellite_schema VARCHAR(128) NOT NULL,
    duplicate_removal_threshold INTEGER DEFAULT 0 NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_satellite_dv_hub FOREIGN KEY (hub_key) REFERENCES dv_hub (hub_key),
    CONSTRAINT fk_dv_satellite_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_satellite_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_satellite_unq ON dv_satellite (owner_key,satellite_name);


-- configure sat columns and link them with source columns
create sequence dv_satellite_column_key_seq start 1;
CREATE TABLE dv_satellite_column
(
    satellite_column_key INTEGER DEFAULT nextval('dv_satellite_column_key_seq'::regclass) PRIMARY KEY NOT NULL,
    satellite_key INTEGER NOT NULL,
    column_key INTEGER NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT 'suser_name()'::character varying,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_satellite_column_dv_satellite FOREIGN KEY (satellite_key) REFERENCES dv_satellite (satellite_key),
    CONSTRAINT fk_dv_satellite_column_dv_stage_table_column FOREIGN KEY (column_key) REFERENCES dv_stage_table_column (column_key),
    CONSTRAINT fk_dv_satellite_column_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_satellite_column_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)

);
CREATE UNIQUE INDEX dv_satellite_column_unq ON dv_satellite_column (owner_key,satellite_key, column_key);
CREATE UNIQUE INDEX dv_satellite_stage_table_column_unq ON dv_satellite_column (owner_key,column_key);



