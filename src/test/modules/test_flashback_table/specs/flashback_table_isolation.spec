setup
{
	CREATE EXTENSION IF NOT EXISTS test_flashback_table;
	DROP TABLE IF EXISTS target_rel;
	DROP TABLE IF EXISTS copy_rel;
	DROP TABLE IF EXISTS tbl1;
	CREATE TABLE target_rel(id int PRIMARY KEY, first_name varchar(6), last_name varchar(6));
	SELECT clean_fb_rel('target_rel');
	CREATE TABLE tbl1(id int);
}

teardown
{
	SELECT clean_fb_rel('target_rel');
	DROP TABLE IF EXISTS target_rel;
	DROP TABLE IF EXISTS copy_rel;
	DROP TABLE IF EXISTS tbl1;
}

session "s1"
step "s1_begin" { BEGIN; }
step "s1_insert_tbl" { INSERT INTO tbl1 SELECT * FROM generate_series(1,10000); }
step "s1_commit" { COMMIT; }

session "s2"
step "s2_begin" { BEGIN; }
step "s2_insert_target" { INSERT INTO target_rel SELECT *, fb_random_string(6), fb_random_string(6) FROM generate_series(1,10000); }
step "s2_commit" { COMMIT; }

session "s3"
step "s3_copy_rel" { CREATE TABLE copy_rel as SELECT * FROM target_rel; }
step "s3_sleep" { SELECT pg_sleep(10); }
step "s3_delete_rel" { DELETE FROM target_rel; }
step "s3_flashback" { FLASHBACK TABLE target_rel to timestamp now() - interval '10s'; }
step "s3_check" { SELECT fb_check_data('target_rel', 'copy_rel'); }

session "s4"
step "s4_begin" { BEGIN; }
step "s4_multi_xact" { SELECT id FROM target_rel where id < 10 for share; }
step "s4_delete" { DELETE FROM target_rel where id > 10; }
step "s4_sleep" { SELECT pg_sleep(11); }
step "s4_commit" { COMMIT; }

session "s5"
step "s5_begin" { BEGIN; }
step "s5_update" { UPDATE target_rel SET first_name = 'test'; }
step "s5_subxact" { SAVEPOINT a; }
step "s5_delete" { DELETE FROM target_rel where id <= 10; }
step "s5_rollback" { ROLLBACK TO SAVEPOINT a; }
step "s5_sleep" { SELECT pg_sleep(11); }
step "s5_commit" { COMMIT; }

session "s6"
step "s6_begin" { BEGIN; }
step "s6_insert_tbl" { INSERT INTO tbl1 SELECT * FROM generate_series(1,10000); }
step "s6_commit" { COMMIT; }

session "s7"
step "s7_begin" { BEGIN; }
step "s7_insert_tbl" { INSERT INTO tbl1 SELECT * FROM generate_series(1,10000); }
step "s7_commit" { COMMIT; }

session "s8"
step "s8_begin" { BEGIN; }
step "s8_insert_tbl" { INSERT INTO tbl1 SELECT * FROM generate_series(1,10000); }
step "s8_commit" { COMMIT; }

session "s9"
step "s9_begin" { BEGIN; }
step "s9_insert_tbl" { INSERT INTO tbl1 SELECT * FROM generate_series(1,10000); }
step "s9_commit" { COMMIT; }

session "s10"
step "s10_begin" { BEGIN; }
step "s10_delete" { DELETE FROM target_rel where id <= 10; }
step "s10_prepare_xact" { prepare transaction 'prepare_xact'; }
step "s10_sleep" { SELECT pg_sleep(11); }
step "s10_commit" { COMMIT prepared 'prepare_xact'; }
step "s10_rollback" { ROLLBACK prepared 'prepare_xact'; }

# long transaction
permutation "s1_begin" "s1_insert_tbl" "s2_begin" "s2_insert_target" "s2_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check" "s1_commit"
permutation "s1_begin" "s1_insert_tbl" "s6_begin" "s6_insert_tbl" "s2_begin" "s2_insert_target" "s2_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check" "s1_commit" "s6_commit"
permutation "s1_begin" "s1_insert_tbl" "s6_begin" "s6_insert_tbl" "s7_begin" "s7_insert_tbl" "s8_begin" "s8_insert_tbl" "s9_begin" "s9_insert_tbl" "s2_begin" "s2_insert_target" "s2_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check" "s1_commit" "s6_commit" "s7_commit" "s8_commit" "s9_commit"
permutation "s2_begin" "s2_insert_target" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s2_commit" "s3_check"
# multi transaction
permutation "s2_begin" "s2_insert_target" "s2_commit" "s4_begin" "s4_multi_xact" "s4_delete" "s4_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s4_begin" "s4_multi_xact" "s4_delete" "s3_copy_rel" "s3_sleep" "s4_sleep" "s4_commit" "s3_delete_rel" "s3_flashback" "s3_check"
# sub transaction
permutation "s2_begin" "s2_insert_target" "s2_commit" "s5_begin" "s5_update" "s5_subxact" "s5_delete" "s5_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s5_begin" "s5_update" "s5_subxact" "s5_delete" "s3_copy_rel" "s3_sleep" "s5_sleep" "s5_commit" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s5_begin" "s5_update" "s5_subxact" "s5_delete" "s5_rollback" "s5_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s5_begin" "s5_update" "s5_subxact" "s5_delete" "s5_rollback" "s3_copy_rel" "s3_sleep" "s5_sleep" "s5_commit" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s5_begin" "s5_subxact" "s5_update" "s5_delete" "s5_rollback" "s3_copy_rel" "s3_sleep" "s5_sleep" "s5_commit" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s5_begin" "s5_update" "s5_subxact" "s5_delete" "s5_rollback" "s5_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check"
# prepare transaction
permutation "s2_begin" "s2_insert_target" "s2_commit" "s10_begin" "s10_delete" "s10_prepare_xact" "s10_commit" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s10_begin" "s10_delete" "s10_prepare_xact" "s3_copy_rel" "s3_sleep" "s10_sleep" "s10_commit" "s3_delete_rel" "s3_flashback" "s3_check"
permutation "s2_begin" "s2_insert_target" "s2_commit" "s10_begin" "s10_delete" "s10_prepare_xact" "s10_rollback" "s3_copy_rel" "s3_sleep" "s3_delete_rel" "s3_flashback" "s3_check"