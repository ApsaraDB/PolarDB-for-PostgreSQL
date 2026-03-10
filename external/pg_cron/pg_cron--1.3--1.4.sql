/* pg_cron--1.3--1.4.sql */

/* cron_schedule_named expects job name to be text */
DROP FUNCTION cron.schedule(name,text,text);
CREATE FUNCTION cron.schedule(job_name text,
                              schedule text,
                              command text)
RETURNS bigint
LANGUAGE C
AS 'MODULE_PATHNAME', $$cron_schedule_named$$;
COMMENT ON FUNCTION cron.schedule(text,text,text)
IS 'schedule a pg_cron job';

CREATE FUNCTION cron.alter_job(job_id bigint,
								schedule text default null,
								command text default null,
								database text default null,
								active boolean default null)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', $$cron_alter_job$$;

COMMENT ON FUNCTION cron.alter_job(bigint,text,text,text,boolean)
IS 'Alter the job identified by job_id. Any option left as NULL will not be modified.';

CREATE FUNCTION cron.schedule_in_database(job_name text,
										  schedule text,
										  command text,
										  database text)
RETURNS bigint
LANGUAGE C
AS 'MODULE_PATHNAME', $$cron_schedule_named$$;

COMMENT ON FUNCTION cron.schedule_in_database(text,text,text,text)
IS 'schedule a pg_cron job';
