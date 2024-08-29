-- Test access privileges
--
-- Clean up in case a prior regression run failed
-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'error';
set polar_allow_huge_alloc = false;
show polar_allow_huge_alloc;
set polar_allow_huge_alloc = true;
show polar_allow_huge_alloc;

do language plpgsql $$
declare 
  v_text text := 'a';
begin 
  for i in 1..29 loop 
    v_text:=v_text||v_text; 
  end loop; 
  execute $_$select '$_$||v_text||$_$'$_$; 
  raise notice 'execute a sql large than 512MB success.';
exception when others then
  raise notice 'execute a sql large than 512MB failed.';
end;
$$;

set polar_allow_huge_alloc = -1;
show polar_allow_huge_alloc;
set polar_allow_huge_alloc = xxx;
show polar_allow_huge_alloc;
set polar_allow_huge_alloc = 1;
show polar_allow_huge_alloc;
set polar_allow_huge_alloc = 0;
show polar_allow_huge_alloc;
set polar_allow_huge_alloc = on;
show polar_allow_huge_alloc;
set polar_allow_huge_alloc = off;
show polar_allow_huge_alloc;
set polar_allow_huge_alloc = false;
show polar_allow_huge_alloc;

RESET client_min_messages;
