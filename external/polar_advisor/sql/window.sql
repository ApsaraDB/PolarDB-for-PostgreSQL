/*
 * external/polar_advisor/sql/maintenance_window.sql
 * Test the maintenance window information and functions about it.
 * super_pg client use the window info to do vacuum and reindex.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

\d+ polar_advisor.advisor_window

SET timezone TO 'PRC';

--
-- Check default window info
--
-- Window has 1 row by default
SELECT * FROM polar_advisor.advisor_window;
SELECT * FROM polar_advisor.get_advisor_window();

-- Window disabled by default
SELECT polar_advisor.is_window_enabled();
-- Vacuum analyze disabled because window is disabled
SELECT polar_advisor.is_vacuum_analyze_enabled();
-- Reindex disabled by default
SELECT polar_advisor.is_reindex_enabled();

-- No exception by default
SELECT polar_advisor.advisor_window_has_exception();

-- Window length is 1 hour by default
SELECT polar_advisor.get_advisor_window_length();


--
-- Get/set window
--
-- Default window
SELECT * FROM polar_advisor.get_advisor_window();

-- Set window
SELECT polar_advisor.set_advisor_window('00:00:00+08', '01:00:00+08');
-- Get window
SELECT * FROM polar_advisor.get_advisor_window();

-- Update window, it's ok to cross 00:00:00
SELECT polar_advisor.set_advisor_window('23:02:33+08', '02:10:28+08');
-- Get window again
SELECT * FROM polar_advisor.get_advisor_window();

-- Set invalid window
-- try to set a window with length 29 minutes < 30 minutes
SELECT polar_advisor.set_advisor_window('00:00:00+08', '00:29:00+08');

-- It's ok to set window when there's no row in table
DELETE FROM polar_advisor.advisor_window;
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.set_advisor_window('08:00:00+08', '12:00:00+08');
SELECT * FROM polar_advisor.get_advisor_window();

-- We cannot set window when there's more than 1 row in table because we don't know which row to update.
INSERT INTO polar_advisor.advisor_window (start_time, end_time, enabled) VALUES ('00:00:00+08', '01:00:00+08', FALSE);
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.set_advisor_window('11:00:00+08', '18:00:00+08');
SELECT * FROM polar_advisor.get_advisor_window();
DELETE FROM polar_advisor.advisor_window;
INSERT INTO polar_advisor.advisor_window (start_time, end_time, enabled) VALUES ('08:00:00+08', '12:00:00+08', FALSE);

-- Window is enabled by default after set
SELECT polar_advisor.disable_advisor_window();
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.is_window_enabled();
-- enabled by default
SELECT polar_advisor.set_advisor_window('11:00:00+08', '18:00:00+08');
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.is_window_enabled();
-- set window but do not enable
SELECT polar_advisor.set_advisor_window('11:00:00+08', '18:00:00+08', false);
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.is_window_enabled();

-- window start time and end time have different timezone
SELECT polar_advisor.set_advisor_window('02:00:00+00', '12:00:00+08');
SELECT polar_advisor.set_advisor_window('02:00:00+08', '12:00:00+05');
SELECT polar_advisor.set_advisor_window('02:00:00 UTC', '12:00:00+07');
SELECT polar_advisor.set_advisor_window('02:00:00 GMT', '12:00:00+08');
SELECT * FROM polar_advisor.get_advisor_window();

-- window time zone different from system timezone
SHOW timezone;
-- ok
SELECT polar_advisor.set_advisor_window('02:00:00+08', '12:00:00+08');
-- error
SELECT polar_advisor.set_advisor_window('02:00:00+00', '12:00:00+00');
SELECT polar_advisor.set_advisor_window('02:00:00 UTC', '12:00:00 UTC');
SELECT polar_advisor.set_advisor_window('02:00:00 GMT', '12:00:00 GMT');
SELECT polar_advisor.set_advisor_window('02:00:00+03', '12:00:00+03');
SELECT * FROM polar_advisor.get_advisor_window();
SET timezone TO 'UTC';
-- ok
SELECT polar_advisor.set_advisor_window('02:00:00+00', '12:00:00+00');
SELECT polar_advisor.set_advisor_window('02:00:00 UTC', '12:00:00 UTC');
SELECT * FROM polar_advisor.get_advisor_window();
-- error
SELECT polar_advisor.set_advisor_window('02:00:00+02', '12:00:00+02');

-- the left and right time zones have different names
-- ok, they are the same
SELECT polar_advisor.set_advisor_window('02:00:00+00', '12:00:00 GMT');
SELECT polar_advisor.set_advisor_window('02:00:00 UTC', '12:00:00 GMT');
SELECT * FROM polar_advisor.get_advisor_window();
-- error
SELECT polar_advisor.set_advisor_window('02:00:00+08', '12:00:00 GMT');
SELECT polar_advisor.set_advisor_window('02:00:00+05', '12:00:00 UTC');

--
-- Enable/disable window
--

-- Enable window
SELECT polar_advisor.enable_advisor_window();
-- Window enabled now
SELECT polar_advisor.is_window_enabled();
-- Vacuum analyze enabled by default
SELECT polar_advisor.is_vacuum_analyze_enabled();
-- Reindex disabled by default
SELECT polar_advisor.is_reindex_enabled();

-- Disable window
SELECT polar_advisor.disable_advisor_window();
-- Window disabled now
SELECT polar_advisor.is_window_enabled();
-- Vacuum analyze disabled due to window disabled
SELECT polar_advisor.is_vacuum_analyze_enabled();
-- Reindex disabled due to window disabled
SELECT polar_advisor.is_reindex_enabled();
-- reenable window for later test
SELECT polar_advisor.enable_advisor_window();

-- Enable vacuum analyze
SELECT polar_advisor.enable_vacuum_analyze();
-- Vacuum enabled now
SELECT polar_advisor.is_vacuum_analyze_enabled();
-- reindex is still disabled when vacuum is enabled
SELECT polar_advisor.is_reindex_enabled();
-- window is enabled
SELECT polar_advisor.is_window_enabled();

-- Disable vacuum analyze
SELECT polar_advisor.disable_vacuum_analyze();
-- window still enabled after disabling only vacuum analyze
SELECT polar_advisor.is_window_enabled();
-- vacuum is disabled
SELECT polar_advisor.is_vacuum_analyze_enabled();
-- reindex is disabled by default
SELECT polar_advisor.is_reindex_enabled();
-- reenable vacuum for later test
SELECT polar_advisor.enable_vacuum_analyze();

-- enable reindex
SELECT polar_advisor.enable_reindex();
-- reindex is enabled after enabling reindex
SELECT polar_advisor.is_reindex_enabled();
-- vacuum is still enabled when reindex is enabled
SELECT polar_advisor.is_vacuum_analyze_enabled();
-- window is still enabled when reindex is enabled
SELECT polar_advisor.is_window_enabled();

-- disable reindex
SELECT polar_advisor.disable_reindex();
-- window still enabled after disabling only reindex
SELECT polar_advisor.is_window_enabled();
-- reindex is disabled
SELECT polar_advisor.is_reindex_enabled();
-- vacuum is still enabled when reindex is disabled
SELECT polar_advisor.is_vacuum_analyze_enabled();

-- Disabled due to exception
SELECT polar_advisor.report_exception('test exception');
SELECT start_time, end_time, enabled, last_error_time IS NOT NULL, last_error_detail, others FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.is_window_enabled();
SELECT polar_advisor.is_vacuum_analyze_enabled();
SELECT polar_advisor.is_reindex_enabled();
SELECT polar_advisor.clear_exception();

-- Disabled when more than 1 row in window because we don't know which row to use.
INSERT INTO polar_advisor.advisor_window (start_time, end_time, enabled) VALUES ('00:00:00+08', '01:00:00+08', FALSE);
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.is_window_enabled();
SELECT polar_advisor.is_vacuum_analyze_enabled();
SELECT polar_advisor.is_reindex_enabled();

-- Disabled when no row in window
DELETE FROM polar_advisor.advisor_window;
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.is_window_enabled();
SELECT polar_advisor.is_vacuum_analyze_enabled();
SELECT polar_advisor.is_reindex_enabled();


--
-- Window start/end time and length
--
SET timezone TO 'Asia/Shanghai';
SELECT polar_advisor.set_advisor_window('00:00:00+08', '01:00:00+08');
-- get length, 1 hour
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.get_advisor_window_length();

-- update window and get length again, 10+ hours
SELECT polar_advisor.set_advisor_window('08:22:00+08', '18:23:17+08');
SELECT * FROM polar_advisor.get_advisor_window();
SELECT polar_advisor.get_advisor_window_length();

SELECT polar_advisor.set_advisor_window('00:00:00+08', '01:00:00+08');

-- get seconds to window start time. We cannot show the real value because it's different every time.
SELECT secs_to_window_start > 0, time_now IS NOT NULL, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start();
-- 1 hour
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '23:00:00+08');
-- 0 hour
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '00:00:00+08');
-- 23 hour
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '01:00:00+08');

-- get seconds to window end time. We cannot show the real value because it's different every time.
SELECT secs_to_window_end IS NOT NULL, time_now IS NOT NULL, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end();
-- 2 hour
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '22:50:00+08');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '23:00:00+08');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '23:00:00+08', time_reserved => INTERVAL '0s');
-- 1 hour
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '23:50:00+08');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '00:00:00+08');
-- 0 hour
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '00:50:00+08');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '01:00:00+08');
-- 23 hour
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '01:50:00+08');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '02:00:00+08');

-- error, timezone mismatch
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '01:00:00+03');
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '01:00:00 GMT');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '02:00:00+05');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '02:00:00 UTC');
SET timezone TO 'UTC';
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '01:00:00+08');
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '01:00:00+03');
SELECT secs_to_window_start, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_start(current_t => '01:00:00 GMT');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '02:00:00+08');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '02:00:00+05');
SELECT secs_to_window_end, time_now, window_start, window_end FROM polar_advisor.get_secs_to_advisor_window_end(current_t => '02:00:00 UTC');


--
-- In-window check, this is very important because super_pg need this to decide when to vacuum
--
SET timezone TO 'Asia/Shanghai';
SHOW timezone;
-- normal window
SELECT polar_advisor.set_advisor_window('00:00:00+08', '01:00:00+08');
SELECT * FROM polar_advisor.get_advisor_window();
-- sometimes in window and sometimes not because the current time is used by default
SELECT polar_advisor.is_in_advisor_window() IS NOT NULL;
-- yes
SELECT polar_advisor.is_in_advisor_window(current_t => '00:00:00+08');
SELECT polar_advisor.is_in_advisor_window(current_t => '00:50:00+08');
-- no, timezone mismatch
SELECT polar_advisor.is_in_advisor_window(current_t => CURRENT_TIME AT TIME ZONE 'UTC');
-- timezone check pass
SELECT polar_advisor.is_in_advisor_window(current_t => CURRENT_TIME AT TIME ZONE 'PRC') IS NOT NULL;
-- no, we should reserve 10 min before window end to stop vacuum
SELECT polar_advisor.is_in_advisor_window(current_t => '00:51:00+08');
-- yes, reserve 0 min
SELECT polar_advisor.is_in_advisor_window(current_t => '01:00:00+08', time_reserved => INTERVAL '0 min');
-- no
SELECT polar_advisor.is_in_advisor_window(current_t => '01:01:00+08', time_reserved => INTERVAL '0 min');
-- no, other timezones
SELECT polar_advisor.is_in_advisor_window(current_t => '23:10:00+07');
SELECT polar_advisor.is_in_advisor_window(current_t => '00:10:00+00');
SELECT polar_advisor.is_in_advisor_window(current_t => '16:10:00 UTC');
SELECT polar_advisor.is_in_advisor_window(current_t => '17:10:00 GMT');

-- window cross midnight
SELECT polar_advisor.set_advisor_window('23:00:00+08', '01:00:00+08');
SELECT * FROM polar_advisor.get_advisor_window();
-- yes
SELECT polar_advisor.is_in_advisor_window(current_t => '23:05:00+08');
SELECT polar_advisor.is_in_advisor_window(current_t => '00:14:00+08');
SELECT polar_advisor.is_in_advisor_window(current_t => '00:50:00+08');
-- no, we should reserve 10 min before window end to stop vacuum
SELECT polar_advisor.is_in_advisor_window(current_t => '00:51:00+08');

-- no, timezone changed
SET timezone TO 'UTC';
SELECT polar_advisor.is_in_advisor_window();
SELECT polar_advisor.is_in_advisor_window(current_t => '23:10:00+08');
SELECT polar_advisor.is_in_advisor_window(current_t => '00:10:00+08');


--
-- Check exception
--

-- no exception now
SELECT polar_advisor.advisor_window_has_exception();
-- report exception
SELECT polar_advisor.report_exception('test exception');
-- exception exists
SELECT polar_advisor.advisor_window_has_exception();
SELECT start_time, end_time, enabled, last_error_time IS NOT NULL, last_error_detail, others FROM polar_advisor.get_advisor_window();
-- clear exception
SELECT polar_advisor.clear_exception();
-- no exception now
SELECT polar_advisor.advisor_window_has_exception();
SELECT * FROM polar_advisor.get_advisor_window();
