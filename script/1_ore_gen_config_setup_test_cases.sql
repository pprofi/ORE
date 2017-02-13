-- test delete--
SET search_path TO ore_config;

INSERT INTO dv_release
(
  release_number,
  release_description,
  version_number)
VALUES (20170209, 'testing generic del func', 1);

SELECT *
FROM dv_release;

-- case 1 - regular
SELECT dv_config_object_delete('dv_release', 1);

-- case 2 - not valid object type
SELECT dv_config_object_delete('dv_release_t', 1);

-- case 3 - non existant ID

SELECT dv_config_object_delete('dv_release', 10);

------- TEST update

-- parameter, incorrect, not exists or one of forbidden

-- case 1 - object type does not exist

SELECT dv_config_object_update('dv_release_1', 2, '{{"hub_key","2"},{"release_key","3"}}');

-- case 2 - correct parameters
SELECT dv_config_object_update('dv_release', 2, '{{"release_description","update test case 2"}}');

-- case 3 one not correct and rest correct
SELECT dv_config_object_update('dv_release', 2,
                               '{{"release_description","update test case 3"},{"release_key","3"},{"version_number","2"}}');

-- case 4 not found object
SELECT dv_config_object_update('dv_release', 3, '{{"hub_key","2"},{"release_key","3"}}');

-- case 5 details to update - incorrect or prohibited to update

SELECT dv_config_object_update('dv_release', 2, '{{"hub_key","2"},{"release_key","3"}}');

-- TEST INSERT

-- case 1 -  incorrect object (++++)
SELECT dv_config_object_insert('dv_release_x', '{{"release_description","Bla blas"},{"version_number","34"}}');

-- case  2 all good release (+++)
SELECT dv_config_object_insert('dv_release',
                               '{{"release_description","Bla bla"},{"release_number","20170210"}}');

-- case 3 all good rest
SELECT dv_config_object_insert('dv_owner',
                               '{{"owner_name","Bla bla"},{"owner_description","test tes"},{"release_number","20170210"},{"is_retired","0"}}');

SELECT dv_config_object_insert('dv_hub',
                               '{{"hub_name","Bla bla"},{"hub_schema","test"},{"release_number","20170210"},{"owner_key","1"}}');

-- case 4 - no release number
SELECT dv_config_object_insert('dv_hub',
                               '{{"hub_name","Bla blas"},{"hub_schema","test"},{"release_number","20170215"},{"owner_key","12"}}');

-- case 5 no owner key
SELECT dv_config_object_insert('dv_hub',
                               '{{"hub_name","Bla blas"},{"hub_schema","test"},{"owner_key","12"},{"release_number","20170215"}}');

-- case 6 no parameters
SELECT dv_config_object_insert('dv_release', '{{"hub_key","2"},{"release_key","3"}}');

-- case 7 not enough not nullable parameters
SELECT dv_config_object_insert('dv_release', '{{"release_description","Bla bla"},{"version_number","3"}}');


SELECT *
FROM dv_hub;
