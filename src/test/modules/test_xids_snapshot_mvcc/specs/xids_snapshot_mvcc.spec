setup
{
    CREATE TABLE xids_snapshot (c1 int NOT NULL, c2 int);
    INSERT INTO xids_snapshot VALUES (1,1);
}

teardown
{
    DROP TABLE xids_snapshot;
}

session "s1"
setup     		{ BEGIN; }
step "s1_update1"	{ UPDATE xids_snapshot SET c2 = 1001 WHERE c1 = 1; }
step "s1_sp"		{ SAVEPOINT sp_xids_snapshot; }
step "s1_update2"	{ UPDATE xids_snapshot SET c2 = 1002 WHERE c1 = 1; }
step "s1_sp_rollback"	{ ROLLBACK TO SAVEPOINT sp_xids_snapshot; }
step "s1_sp_commit"	{ RELEASE SAVEPOINT sp_xids_snapshot; }
step "s1_commit" 	{ COMMIT; }
step "s1_abort"   	{ ROLLBACK; }

session "s2"
step "s2_select"	{ SELECT c2 FROM xids_snapshot WHERE c1 = 1; }
 
session "s3"
step "s3_begin"		{ BEGIN; }
step "s3_lock"		{ select c2 FROM xids_snapshot WHERE c1 = 1 FOR KEY SHARE; }
step "s3_commit" 	{ COMMIT; }


permutation "s1_update1" "s2_select" "s1_commit" "s2_select" "s2_select" 
permutation "s1_update1" "s2_select" "s1_abort" "s2_select"
permutation "s1_update1" "s1_sp" "s1_update2" "s1_sp_commit" "s2_select" "s1_commit" "s2_select" "s2_select" 
permutation "s1_update1" "s1_sp" "s1_update2" "s1_sp_commit" "s2_select" "s1_abort" "s2_select"
permutation "s1_update1" "s1_sp" "s1_update2" "s1_sp_rollback" "s2_select" "s1_commit" "s2_select" "s2_select" 
permutation "s1_update1" "s1_sp" "s1_update2" "s1_sp_rollback" "s2_select" "s1_abort" "s2_select"
permutation "s1_update1" "s3_begin" "s3_lock" "s2_select" "s1_commit" "s2_select" "s3_commit" "s2_select"
permutation "s1_update1" "s3_begin" "s3_lock" "s2_select" "s1_abort" "s2_select" "s3_commit" "s2_select"
