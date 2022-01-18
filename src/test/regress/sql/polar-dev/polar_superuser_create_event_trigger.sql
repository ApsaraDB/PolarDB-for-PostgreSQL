-- create regular user
create user t_user;
-- create polar_superuser
create user p_user with polar_superuser;
-- create event trigger function
\c - p_user;
create function test_event_trigger_for_polar() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger_for_polar: % %', tg_event, tg_tag;
END
$$ language plpgsql;

\c - postgres
create function test_event_trigger_for_super() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger_for_super: % %', tg_event, tg_tag;
END
$$ language plpgsql;

-- create event trigger 

--regular t_user
\c - t_user
-- failed not privileges
create event trigger polar_event_trigger on ddl_command_start
   execute procedure test_event_trigger_for_polar();

-- polar_superuser
\c - p_user
-- OK
create event trigger polar_event_trigger on ddl_command_start
   execute procedure test_event_trigger_for_polar();

-- success call trigger function
create table test(id int);
drop table test;

--regular t_user
\c - t_user
-- success call trigger function
create table test(id int);
drop table test;

-- superuser
\c - postgres
-- failed call trigger function
create table test(id int);
drop table test;

-- superuser create event trigger
create event trigger super_event_trigger on ddl_command_start
   execute procedure test_event_trigger_for_super();

--success call trigger function
create table test(id int);
drop table test;

-- polar_superuser
\c - p_user
create table test(id int);
drop table test;

--regular t_user
\c - t_user
create table test(id int);
drop table test;

-- revoke privileges on function
\c - postgres
drop  event trigger polar_event_trigger;
drop function test_event_trigger_for_polar cascade;
create function test_event_trigger_for_polar() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger_for_polar: % %', tg_event, tg_tag;
END
$$ language plpgsql;
REVOKE ALL  PRIVILEGES ON FUNCTION test_event_trigger_for_polar FROM PUBLIC;
\c - p_user
create event trigger polar_event_trigger on ddl_command_start
   execute procedure test_event_trigger_for_polar();
\c - postgres
drop function test_event_trigger_for_polar cascade; 

-- polar_superuser
\c - p_user
create or replace function test_event_trigger_for_polar() returns event_trigger as $$
BEGIN
    alter user p_user with superuser;
    -- create table test1(id int);
    -- alter event trigger polar_event_trigger disable;
    RAISE NOTICE 'test_event_trigger_for_polar: % %', tg_event, tg_tag;
END
$$ language plpgsql;

drop event trigger polar_event_trigger;

create event trigger polar_event_trigger on ddl_command_start
   execute procedure test_event_trigger_for_polar();

create table test(id int);
drop table test;

-- superuser
\c - postgres
create table test(id int);
drop table test;

set polar_super_call_all_trigger_event = true;
create table test(id int);
drop table test;

\c - p_user;
show polar_super_call_all_trigger_event;
set polar_super_call_all_trigger_event = true;

\c - postgres

drop  event trigger polar_event_trigger;
create table test(id int);
drop table test;
drop  event trigger super_event_trigger;
create table test(id int);
drop table test;

--clean up
drop function test_event_trigger_for_super cascade;
drop function test_event_trigger_for_polar cascade;
drop user t_user;
drop user p_user;










