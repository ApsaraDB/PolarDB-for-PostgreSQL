--
-- Test cases for COPY (INSERT/UPDATE/DELETE) TO
--
create table copydml_test (id serial, t text);
insert into copydml_test (t) values ('a');
insert into copydml_test (t) values ('b');
insert into copydml_test (t) values ('c');
insert into copydml_test (t) values ('d');
insert into copydml_test (t) values ('e');

--
-- Test COPY (insert/update/delete ...)
--
-- POLAR_TAG: REPLICA_ERR
copy (insert into copydml_test (t) values ('f') returning id) to stdout;
-- POLAR_TAG: REPLICA_ERR
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
-- POLAR_TAG: REPLICA_ERR
copy (delete from copydml_test where t = 'g' returning id) to stdout;

--
-- Test \copy (insert/update/delete ...)
--
-- POLAR_TAG: REPLICA_ERR
\copy (insert into copydml_test (t) values ('f') returning id) to stdout;
-- POLAR_TAG: REPLICA_ERR
\copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
-- POLAR_TAG: REPLICA_ERR
\copy (delete from copydml_test where t = 'g' returning id) to stdout;

-- Error cases
-- POLAR_TAG: REPLICA_ERR
copy (insert into copydml_test default values) to stdout;
-- POLAR_TAG: REPLICA_ERR
copy (update copydml_test set t = 'g') to stdout;
-- POLAR_TAG: REPLICA_ERR
copy (delete from copydml_test) to stdout;

create rule qqq as on insert to copydml_test do instead nothing;
-- POLAR_TAG: REPLICA_ERR
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test do also delete from copydml_test;
-- POLAR_TAG: REPLICA_ERR
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test do instead (delete from copydml_test; delete from copydml_test);
-- POLAR_TAG: REPLICA_ERR
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test where new.t <> 'f' do instead delete from copydml_test;
-- POLAR_TAG: REPLICA_ERR
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;

create rule qqq as on update to copydml_test do instead nothing;
-- POLAR_TAG: REPLICA_ERR
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test do also delete from copydml_test;
-- POLAR_TAG: REPLICA_ERR
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test do instead (delete from copydml_test; delete from copydml_test);
-- POLAR_TAG: REPLICA_ERR
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test where new.t <> 'f' do instead delete from copydml_test;
-- POLAR_TAG: REPLICA_ERR
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;

create rule qqq as on delete to copydml_test do instead nothing;
-- POLAR_TAG: REPLICA_ERR
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test do also insert into copydml_test default values;
-- POLAR_TAG: REPLICA_ERR
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test do instead (insert into copydml_test default values; insert into copydml_test default values);
-- POLAR_TAG: REPLICA_ERR
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test where old.t <> 'f' do instead insert into copydml_test default values;
-- POLAR_TAG: REPLICA_ERR
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;

-- triggers
create function qqq_trig() returns trigger as $$
begin
if tg_op in ('INSERT', 'UPDATE') then
    raise notice '% %', tg_op, new.id;
    return new;
else
    raise notice '% %', tg_op, old.id;
    return old;
end if;
end
$$ language plpgsql;
-- POLAR_END_FUNC

create trigger qqqbef before insert or update or delete on copydml_test
    for each row execute procedure qqq_trig();
create trigger qqqaf after insert or update or delete on copydml_test
    for each row execute procedure qqq_trig();

-- POLAR_TAG: REPLICA_ERR
copy (insert into copydml_test (t) values ('f') returning id) to stdout;
-- POLAR_TAG: REPLICA_ERR
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
-- POLAR_TAG: REPLICA_ERR
copy (delete from copydml_test where t = 'g' returning id) to stdout;

drop table copydml_test;
drop function qqq_trig();
