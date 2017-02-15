CREATE TABLE dv_hub
(
    hub_key INTEGER DEFAULT nextval('dv_hub_key_seq'::regclass) PRIMARY KEY NOT NULL,
    hub_name VARCHAR(128) NOT NULL,
    hub_schema VARCHAR(128) NOT NULL,
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    owner_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT "current_user"() NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_hub_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key),
    CONSTRAINT fk_dv_hub_dv_owner FOREIGN KEY (owner_key) REFERENCES dv_owner (owner_key)
);
CREATE UNIQUE INDEX dv_hub_unq ON dv_hub (owner_key, hub_schema, hub_name);

select * from dv_default_column;

SET search_path TO ore_config;


DO $$DECLARE r     dv_column_type[2];

        rowcount_v INT;

BEGIN


 r[1].column_name='a';

  /*insert into r (column_name ,
  column_type  ,
  column_length  ,
  column_precision ,
  column_scale     ,
  ordinal_position) values
('a','int',1,2,3,1);
*/
END$$;


dv_config_dv_table_create(
  object_name_in    VARCHAR(128),
  object_schema_in  VARCHAR(128),
  object_type_in    VARCHAR(30),
  object_columns_in dv_column_type,
  recreate_flag_in  CHAR(1) = 'N'
)



create table test_create of dv_column_type;


select * from test_create;

INSERT INTO test_create (column_name,
                         column_type,
                         column_length,
                         column_precision,
                         column_scale,
                         ordinal_position)

  SELECT
    'col1',
    'int',
    0,
    0,
    0,
    1
  UNION ALL
  SELECT
    'col2',
    'varchar',
    10,
    0,
    0,
    2;


DO $$DECLARE
  cnt_v int;
  rec  cursor for select * from test_create;
BEGIN




select dv_config_dv_table_create(
  'customer',
  'ore_config',
  'hub',
 -- rec,
   'N'
) into cnt_v;
  raise NOTICE 'SQL -->%',cnt_v;
END$$;


declare xxx cursor for select * from f1;
DECLARE CURSOR
Time: 23.409 ms
postgres=# select fx('xxx');
NOTICE:  (10,20)
NOTICE:  (340,30)




SELECT   CASE WHEN d.object_column_type = 'Object_Key' then
              rtrim(coalesce(column_prefix, '') || replace(d.column_name, '%', 'hub') ||
                    coalesce(column_suffix, '')) else column_name end AS column_name,
              column_type,
              column_length,
              column_precision,
              column_scale,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 0
              ELSE 1 END                         AS is_nullable,
              CASE WHEN d.object_column_type = 'Object_Key'
                THEN 1
              ELSE 0 END                         AS is_key
            FROM dv_default_column d where object_type='hub'


USE [ODE_Vault]
GO

/****** Object:  Table [hub].[h_Customer]    Script Date: 15/02/2017 5:36:06 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [hub].[h_Customer](
	[h_Customer_key] [int] IDENTITY(1,1) NOT NULL,
	[dv_load_date_time] [datetimeoffset](7) NULL,
	[dv_record_source] [varchar](50) NULL,
	[CustomerID] [varchar](30) NULL,
PRIMARY KEY CLUSTERED
(
	[h_Customer_key] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]


CREATE TABLE dv_owner
(
    owner_key INTEGER DEFAULT nextval('dv_owner_key_seq'::regclass) PRIMARY KEY NOT NULL,
    owner_name VARCHAR(256),
    owner_description VARCHAR(256),
    is_retired BOOLEAN DEFAULT false NOT NULL,
    release_key INTEGER DEFAULT 0 NOT NULL,
    version_number INTEGER DEFAULT 1 NOT NULL,
    updated_by VARCHAR(50) DEFAULT "current_user"() NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT fk_dv_owner_dv_release FOREIGN KEY (release_key) REFERENCES dv_release (release_key)
);
CREATE UNIQUE INDEX dv_owner_unq ON dv_owner (owner_name);

