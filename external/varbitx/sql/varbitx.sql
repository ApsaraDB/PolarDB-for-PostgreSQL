-- good
create extension varbitx;
select get_bit('111110000011', 3, 5);
select set_bit_array('111100001111', 0, 1, array[1,15]);
select set_bit_array('111100001111', 0, 1, array[1,25]);
select bit_count('1111000011110000', 1, 5, 4);
select bit_count('1111000011110000', 1);
select bit_fill(0,10);
select bit_posite ('11110010011', 1, true);
select bit_posite ('11110010011', 1, false);
select bit_posite ('11110010011', 1, 3, true);
select bit_posite ('11110010011', 1, 3, false);

-- bad
select get_bit('911110000011', 3, 5);

-- bug
select bit_posite(B'00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 1, false);
select set_bit_array( set_bit_array('0'::varbit, 0, 0, array[31,49,61,97]), 1, 0, array[4,33,57,66,69,83] );
select set_bit_array( set_bit_array('0'::varbit, 0, 0, array[31,49,61,97]), 1, 0, array[4,33,57,66,69,83] )::text::varbit;

-- improvement

select get_bit_array('111110000011', 3, 5, 1);
select get_bit_array('111110000011', 1, array[1,5,6,7,10,11]);
select set_bit_array('111100001111', 1, 0, array[4,5,6,7], 2);
select set_bit_array_record('111100001111', 0, 1, array[1,15]);
select set_bit_array_record('111100001111', 1, 0, array[1,4,5,6,7], 2);
select set_bit_array_record('111100001111', 1, 0, array[1,4,5,6,7], 1);
select bit_count_array('1111000011110000', 1, array[1,2,7,8]);

-- special case
select get_bit ('b' , -5, -3);
select get_bit ('b' , 0, -3);
select get_bit ('b' , 2, 1);

select get_bit_array('111110000011', -3, -5, 1);
select get_bit_array('111110000011', -3, 5, 1);
select get_bit_array('111110000011', 3, -5, 1);
select get_bit_array('111110000011', 20, 5, 1);
drop extension varbitx;

create user normal_user1;
create user polar_super1 SUPERUSER;

-- error
set session_authorization TO normal_user1;
create extension varbitx;

-- ok
set session_authorization TO polar_super1;
create extension varbitx;
drop extension varbitx;

reset session authorization;
drop user normal_user1;
drop user polar_super1;
