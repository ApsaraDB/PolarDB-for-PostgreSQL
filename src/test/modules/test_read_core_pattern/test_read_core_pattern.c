#include "postgres.h"

#include <unistd.h>

#include "miscadmin.h"
#include "pgstat.h"

#include "common/file_perm.h"
#include "storage/polar_fd.h"
#include "utils/polar_coredump.h"

PG_MODULE_MAGIC;

static void
test_core_pattern_path(void)
{
	int fd;
	char core_pattern[MAXPGPATH] = {0};
	char core_file_path_temp[MAXPGPATH] = {0};
	char core_file_target[MAXPGPATH] = {0};
	char core_pattern_path[MAXPGPATH] = {0};
	char cwd[MAXPGPATH] = {0};

	getcwd(cwd, MAXPGPATH);
	snprintf(core_pattern_path, MAXPGPATH, "%s/polar_test_core_pattern", cwd);

	/* case1 */
	fd = open(core_pattern_path, O_CREAT | O_WRONLY | PG_BINARY, pg_file_create_mode);
	write(fd, &core_pattern, sizeof(core_pattern));
	close(fd);
	polar_read_core_pattern(core_pattern_path, core_file_path_temp);
	Assert(!strcmp(core_file_path_temp, cwd));
	

	/* case2 */
	fd = open(core_pattern_path, O_CREAT | O_WRONLY | PG_BINARY, pg_file_create_mode);
	snprintf(core_pattern, MAXPGPATH, "core");
	write(fd, &core_pattern, sizeof(core_pattern));
	close(fd);
	polar_read_core_pattern(core_pattern_path, core_file_path_temp);
	Assert(!strcmp(core_file_path_temp, cwd));

	/* case3 */
	fd = open(core_pattern_path, O_CREAT | O_WRONLY | PG_BINARY, pg_file_create_mode);
	snprintf(core_pattern, MAXPGPATH, "corefile/core");
	write(fd, &core_pattern, sizeof(core_pattern));
	close(fd);
	polar_read_core_pattern(core_pattern_path, core_file_path_temp);
	snprintf(core_file_target, MAXPGPATH, "%s/corefile/", cwd);
	Assert(!strcmp(core_file_path_temp, core_file_target));

	/* case4 */
	fd = open(core_pattern_path, O_CREAT | O_WRONLY | PG_BINARY, pg_file_create_mode);
	snprintf(core_pattern, MAXPGPATH, "/tmp/corefile/core");
	write(fd, &core_pattern, sizeof(core_pattern));
	close(fd);
	polar_read_core_pattern(core_pattern_path, core_file_path_temp);
	snprintf(core_file_target, MAXPGPATH, "/tmp/corefile/");
	Assert(!strcmp(core_file_path_temp, core_file_target));
}

PG_FUNCTION_INFO_V1(test_read_core_pattern);
/*
 *  The module is used to test function polar_read_core_pattern() 
 */
Datum
test_read_core_pattern(PG_FUNCTION_ARGS)
{
	test_core_pattern_path();

	PG_RETURN_VOID();
}

