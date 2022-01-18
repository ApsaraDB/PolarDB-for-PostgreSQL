set client_min_messages = error;
alter system set polar_flashback_log_debug=on;
select * from pg_reload_conf();
CREATE EXTENSION if not exists test_flashback_log ;
checkpoint;
SELECT test_flashback_log();
