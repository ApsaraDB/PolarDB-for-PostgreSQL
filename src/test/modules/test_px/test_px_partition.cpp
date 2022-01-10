#include "px_optimizer_util/px_wrappers.h"
#include <stdlib.h>

extern "C"
{

PG_FUNCTION_INFO_V1(test_px_partition_child_index);
Datum
test_px_partition_child_index(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);
    List *partition_oid_list = px::GetRelChildIndexes(oid);
    char* oid_array = (char*) palloc0(512);
    ListCell *lc;
    sprintf(oid_array, "{");
	foreach (lc, partition_oid_list)
	{
        char oid_str[256];
        sprintf(oid_str, "%d, ", lfirst_oid(lc));
        strcat(oid_array, oid_str);
	}
    strcat(oid_array, "}");
    PG_RETURN_CSTRING(oid_array);
}

PG_FUNCTION_INFO_V1(test_px_partition_is_partition_rel);
Datum
test_px_partition_is_partition_rel(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);
    PG_RETURN_BOOL(px::RelIsPartitioned(oid));
}

PG_FUNCTION_INFO_V1(test_px_partition_is_partition_index);
Datum
test_px_partition_is_partition_index(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);
    PG_RETURN_BOOL(px::IndexIsPartitioned(oid));
}

}
