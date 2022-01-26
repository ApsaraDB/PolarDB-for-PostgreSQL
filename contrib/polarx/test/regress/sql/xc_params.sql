--
-- xc_params
--
-- Tests usage of non-simple variables (specifically record types without
-- %rowtype descriptor) in SQL statements inside a plpgsql function. 
--
--

create table prm_emp(empno int, empname varchar);


create function prm_func() returns varchar as
$$
declare emprow record;
begin
	insert into prm_emp values (1, 'abcd');

	-- Initialize emprow
	select into emprow * from prm_emp;

	emprow.empname = 'xyz';
	emprow.empno = emprow.empno + 1;

	-- Test parameter handling in insert.
	insert into prm_emp values (emprow.empno, emprow.empname);
	-- At the same time, add some rows to be deleted for testing DELETEs
	emprow.empno = emprow.empno + 1;
	insert into prm_emp values (emprow.empno, 'tobedeleted1');
	emprow.empno = emprow.empno + 1;
	insert into prm_emp values (emprow.empno, 'tobedeleted2');

	-- Test parameter handling for non-shippable UPDATE statement.
	emprow.empno = 2;
	emprow.empname = ' changed row 2';
	-- This will update the 2nd row
	update prm_emp set empname = empname || emprow.empname
					where   (empname || now()) like '%xyz%';

	emprow.empname = 'abcd';
	-- This will update 1st row
	update prm_emp set empname = emprow.empname || ' changed row 1'
	                where empname = emprow.empname;

	-- Test DELETEs. The final rows should not contain these rows.
	-- Test shippable DELETEs.
	delete from prm_emp where empname = 'tobedeleted1';
	-- Test non-shippable DELETEs. Again, now() is used only to make non-shippable.
	delete from prm_emp where empname = 'tobedeleted2' and (emprow.empname || now()) is not NULL;

	-- Test SELECTs
	emprow.empno = -1;
	emprow.empname = '%xyz%';
	select into emprow * from prm_emp where empname || now() like emprow.empname;

	-- Return the SELECTed emp row.
	return emprow.empno::text ||  ' | ' || emprow.empname;
end
$$ language plpgsql;

select prm_func();

-- Show the final rows
select * from prm_emp order by empno;

drop table prm_emp;
drop function prm_func();
