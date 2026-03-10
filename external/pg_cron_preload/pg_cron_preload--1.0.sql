-- /* pg_cron_preload--1.1--1.2.sql */

CREATE SCHEMA cron;
CREATE SEQUENCE cron.jobid_seq;

CREATE TABLE cron.job (
    jobid bigint primary key default pg_catalog.nextval('cron.jobid_seq'),
    schedule text not null COLLATE "C",
    command text not null COLLATE "C",
    nodename text not null default 'localhost' COLLATE "C",
    nodeport int not null default pg_catalog.inet_server_port(),
    database text not null default pg_catalog.current_database() COLLATE "C", 
    username text not null default current_user COLLATE "C"
);

GRANT SELECT ON cron.job TO public;
ALTER TABLE cron.job ENABLE ROW LEVEL SECURITY;
CREATE POLICY cron_job_policy ON cron.job USING (username OPERATOR(pg_catalog.=) current_user);

-- CREATE FUNCTION cron.schedule(schedule text, command text)
--     RETURNS bigint
--     LANGUAGE C STRICT
--     AS 'MODULE_PATHNAME', $$cron_schedule$$;
-- COMMENT ON FUNCTION cron.schedule(text,text)
--     IS 'schedule a pg_cron job';

-- CREATE FUNCTION cron.unschedule(job_id bigint)
--     RETURNS bool
--     LANGUAGE C STRICT
--     AS 'MODULE_PATHNAME', $$cron_unschedule$$;
-- COMMENT ON FUNCTION cron.unschedule(bigint)
--     IS 'unschedule a pg_cron job';

CREATE FUNCTION cron.job_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$cron_job_cache_invalidate$$;
COMMENT ON FUNCTION cron.job_cache_invalidate()
    IS 'invalidate job cache';

CREATE TRIGGER cron_job_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON cron.job
    FOR STATEMENT EXECUTE PROCEDURE cron.job_cache_invalidate();

/* pg_cron--1.0--1.1.sql */

ALTER TABLE cron.job ADD COLUMN active boolean not null default 'true';

/* pg_cron--1.1--1.2.sql */

SELECT pg_catalog.pg_extension_config_dump('cron.job', '');
SELECT pg_catalog.pg_extension_config_dump('cron.jobid_seq', '');

/* pg_cron--1.2--1.3.sql */

CREATE SEQUENCE cron.runid_seq;

CREATE TABLE cron.job_run_details (
    jobid bigint,
    runid bigint primary key default nextval('cron.runid_seq'),
    job_pid integer,
    database text COLLATE "C",
    username text COLLATE "C",
    command text COLLATE "C",
    status text COLLATE "C",
    return_message text COLLATE "C",
    start_time timestamptz,
    end_time timestamptz
);

GRANT SELECT ON cron.job_run_details TO public;
GRANT DELETE ON cron.job_run_details TO public;
ALTER TABLE cron.job_run_details ENABLE ROW LEVEL SECURITY;
CREATE POLICY cron_job_run_details_policy ON cron.job_run_details USING (username OPERATOR(pg_catalog.=) current_user);

SELECT pg_catalog.pg_extension_config_dump('cron.job_run_details', '');
SELECT pg_catalog.pg_extension_config_dump('cron.runid_seq', '');

ALTER TABLE cron.job ADD COLUMN jobname name COLLATE "C";

CREATE UNIQUE INDEX jobname_username_idx ON cron.job (jobname COLLATE "C", username COLLATE "C");
ALTER TABLE cron.job ADD CONSTRAINT jobname_username_uniq UNIQUE USING INDEX jobname_username_idx;

-- CREATE FUNCTION cron.schedule(job_name name, schedule text, command text)
--     RETURNS bigint
--     LANGUAGE C STRICT
--     AS 'MODULE_PATHNAME', $$cron_schedule_named$$;
-- COMMENT ON FUNCTION cron.schedule(name,text,text)
--     IS 'schedule a pg_cron job';

-- CREATE FUNCTION cron.unschedule(job_name name)
--     RETURNS bool
--     LANGUAGE C STRICT
--     AS 'MODULE_PATHNAME', $$cron_unschedule_named$$;
-- COMMENT ON FUNCTION cron.unschedule(name)
--     IS 'unschedule a pg_cron job';

/* pg_cron--1.3--1.4.sql */

/* cron_schedule_named expects job name to be text */
-- DROP FUNCTION cron.schedule(name,text,text);
-- CREATE FUNCTION cron.schedule(job_name text,
--                               schedule text,
--                               command text)
-- RETURNS bigint
-- LANGUAGE C
-- AS 'MODULE_PATHNAME', $$cron_schedule_named$$;
-- COMMENT ON FUNCTION cron.schedule(text,text,text)
-- IS 'schedule a pg_cron job';

-- CREATE FUNCTION cron.alter_job(job_id bigint,
-- 								schedule text default null,
-- 								command text default null,
-- 								database text default null,
-- 								active boolean default null)
-- RETURNS void
-- LANGUAGE C
-- AS 'MODULE_PATHNAME', $$cron_alter_job$$;

-- COMMENT ON FUNCTION cron.alter_job(bigint,text,text,text,boolean)
-- IS 'Alter the job identified by job_id. Any option left as NULL will not be modified.';

-- CREATE FUNCTION cron.schedule_in_database(job_name text,
-- 										  schedule text,
-- 										  command text,
-- 										  database text)
-- RETURNS bigint
-- LANGUAGE C
-- AS 'MODULE_PATHNAME', $$cron_schedule_named$$;

-- COMMENT ON FUNCTION cron.schedule_in_database(text,text,text,text)
-- IS 'schedule a pg_cron job';

/* pg_cron--1.4--1.4-1.sql */

/*
 * pg_dump will read from these sequences. Grant everyone permission
 * to read from the sequence. That way, a user with usage on the cron
 * schema can also do pg_dump. This does not grant write/nextval
 * permission.
 */
GRANT SELECT ON SEQUENCE cron.jobid_seq TO public;
GRANT SELECT ON SEQUENCE cron.runid_seq TO public;

/* pg_cron--1.4.1--1.5.sql */
ALTER TABLE cron.job ALTER COLUMN jobname TYPE text COLLATE "C";

-- DROP FUNCTION cron.unschedule(name);
-- CREATE FUNCTION cron.unschedule(job_name text)
--     RETURNS bool
--     LANGUAGE C STRICT
--     AS 'MODULE_PATHNAME', $$cron_unschedule_named$$;
-- COMMENT ON FUNCTION cron.unschedule(text)
--     IS 'unschedule a pg_cron job';

-- revoke privilege
REVOKE ALL ON SCHEMA cron FROM public;
REVOKE ALL ON TABLE cron.job FROM public;

-- grant privilege
GRANT usage ON SCHEMA cron TO public;
GRANT SELECT ON cron.job TO public;
