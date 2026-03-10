
create extension polar_tde_utils;

select polar_tde_update_kmgr_file('echo "adfadsfadssssssssfa123123123123123123123123123123123123111313126"');

show polar_cluster_passphrase_command;

select polar_tde_update_kmgr_file('echo "adfadsfadssssssssfa123123123123123123123123123123123123111313123"');

show polar_cluster_passphrase_command;

select kmgr_version_no from polar_tde_kmgr_info_view();

drop extension polar_tde_utils;