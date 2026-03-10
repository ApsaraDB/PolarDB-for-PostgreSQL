/*
 * external/polar_advisor/sql/window_monitor.sql
 * Test the monitor function need by super_pg client.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

-- get default limit
SELECT polar_advisor.get_active_user_conn_num_limit();
SELECT others, others->>'active_user_conn_limit' AS conn_limit FROM polar_advisor.advisor_window;
SHOW polar_instance_spec_cpu;

-- set limit
SELECT polar_advisor.set_active_user_conn_num_limit(100);

-- get the new limit
-- TODO polar_instance_spec_cpu
SELECT polar_advisor.get_active_user_conn_num_limit();
SELECT others, others->>'active_user_conn_limit' AS conn_limit FROM polar_advisor.advisor_window;

-- update the limit
SELECT polar_advisor.set_active_user_conn_num_limit(200);

-- get the new limit
SELECT polar_advisor.get_active_user_conn_num_limit();
SELECT others, others->>'active_user_conn_limit' AS conn_limit FROM polar_advisor.advisor_window;

-- unset limit
SELECT polar_advisor.unset_active_user_conn_num_limit();

-- get the new limit
SELECT polar_advisor.get_active_user_conn_num_limit();
SELECT others, others->>'active_user_conn_limit' AS conn_limit FROM polar_advisor.advisor_window;
