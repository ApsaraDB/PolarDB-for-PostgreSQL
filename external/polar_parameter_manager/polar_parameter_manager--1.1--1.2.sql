CREATE OR REPLACE VIEW polar_parameter.parameter_infos_memory AS
WITH c AS (
     SELECT name,
            setting,
            CASE WHEN sourcefile IS NOT NULL AND sourcefile not like '%polar_settings.conf' THEN 0
                 ELSE 1
            END as is_polar_setting
     FROM pg_file_settings WHERE applied = 't'
)

SELECT    a.name::character varying               AS name,
          a.boot_val::character varying           AS default_value,
     CASE WHEN c.setting IS NOT NULL THEN c.setting::character varying  
          ELSE a.setting::character varying  
     END                                          AS setting_value,
     CASE WHEN a.context = 'postmaster' THEN 0
          ELSE 1
     END                                          AS is_dynamic,
     CASE WHEN c.is_polar_setting = 0 THEN 0
          ELSE b.is_visible::int 
     END                                          AS is_visible,
     CASE WHEN c.is_polar_setting = 0 THEN 0
          ELSE b.is_user_changable::int 
     END                                          AS is_user_changable,
     CASE WHEN b.optional IS NOT NULL THEN b.optional
          WHEN a.vartype = 'bool' THEN '[on|off]'
          WHEN a.enumvals IS NOT NULL THEN '[' || array_to_string(enumvals,'|') || ']'
          WHEN a.max_val::numeric >= '999999999999'::numeric AND a.vartype = 'real'
               THEN '[' || a.min_val || '-999999999999.0]'
          WHEN a.min_val IS NOT NULL OR a.max_val IS NOT NULL 
               THEN '[' || a.min_val || '-' || a.max_val || ']'
          ELSE '^.*$'
     END                                          AS optional,
     CASE WHEN a.vartype = 'integer'  THEN 'INT'
          WHEN a.vartype = 'real' THEN 'DOUBLE' 
          ELSE 'STRING'
     END                                          AS unit,
     CASE WHEN a.vartype = 'integer' OR a.vartype = 'real' THEN 1
          ELSE 0
     END                                          AS divide_base,
          a.short_desc::character varying         AS comment,
     CASE WHEN c.is_polar_setting = 0
               THEN 'setting is in postgresql.(auto).conf'::character varying
          ELSE NULL
     END                                          AS unchangable_reason,
          b.is_list::int                          AS is_list
FROM pg_settings a LEFT JOIN  c ON lower(a.name) = c.name, polar_parameter.polar_get_guc_info(a.name) b;