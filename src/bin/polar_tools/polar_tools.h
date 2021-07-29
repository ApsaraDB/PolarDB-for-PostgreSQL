#ifndef POLAR_TOOLS_H
#define POLAR_TOOLS_H

#include "postgres.h"

#include "common/fe_memutils.h"
#include "getopt_long.h"

extern int block_header_dump_main(int argc, char **argv);
extern int control_data_change_main(int argc, char **argv);
extern int logindex_meta_main(int argc, char **argv);
extern int logindex_bloom_main(int argc, char **argv);
extern int logindex_table_main(int argc, char **argv);
extern int logindex_page_main(int argc, char **argv);

#endif
