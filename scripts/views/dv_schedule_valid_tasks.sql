-- list of valid schedule tasks & execution sctipts
CREATE OR REPLACE VIEW dv_schedule_valid_tasks AS
  SELECT
    t.schedule_key,
    t.schedule_name,
    t.owner_key,
    t.schedule_frequency,
    t.schedule_task_key,
    t.parent_task_key,
    t.depth                                                                             AS task_level,
    t.object_key,
    t.object_type,
    t.load_type,
    ore_config.fn_get_dv_object_load_script(t.object_key, t.object_type, t.load_type, t.owner_key) AS load_script
  FROM
    (
      SELECT
        s.schedule_key,
        s.owner_key,
        s.schedule_name,
        s.schedule_frequency,
        s.start_date,
        st.schedule_task_key,
        sth.parent_task_key,
        sth.depth,
        st.object_key,
        st.object_type,
        st.load_type
      FROM dv_schedule s
        JOIN dv_schedule_task st ON s.schedule_key = st.schedule_key
        JOIN
        (
          WITH RECURSIVE node_rec AS
          (
            SELECT
              1                        AS depth,
              schedule_task_key        AS task_key,
              schedule_parent_task_key AS parent_task_key
            FROM dv_schedule_task_hierarchy
            WHERE schedule_parent_task_key IS NULL AND is_cancelled = FALSE
            UNION ALL
            SELECT
              depth + 1,
              n.schedule_task_key        AS task_key,
              n.schedule_parent_task_key AS parent_task_key
            FROM dv_schedule_task_hierarchy AS n
              JOIN node_rec r ON n.schedule_parent_task_key = r.task_key
            WHERE n.is_cancelled = FALSE
          )
          SELECT
            depth,
            task_key,
            parent_task_key
          FROM node_rec
        )
        sth ON sth.task_key = st.schedule_task_key
      WHERE s.is_cancelled = FALSE AND st.is_cancelled = FALSE
    ) t;
