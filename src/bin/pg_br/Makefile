#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/transam
#
# IDENTIFICATION
#    src/backend/access/transam/Makefile
#
#-------------------------------------------------------------------------

pg_br: $(OBJS)
	gcc  -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -g -O0 -L../../../src/port -L../../../src/common -L ../../../src/backend/polar_dma/lib/ -Wl,--as-needed -Wl,-rpath,'../../../prebuild/lib',--enable-new-dtags -Wl,-E $(OBJS) -lpthread -lrt -lcrypt -ldl -lm -lpthread -lpolar_consensus_wrapper -lprotobuf -lstdc++ -lrt -lssl -lcrypto -o pg_br

PG_BR_PATH = ../../backend

BRIN = $(PG_BR_PATH)/access/brin/*.o 
COMMON = $(PG_BR_PATH)/access/common/*.o 
gin = $(PG_BR_PATH)/access/gin/*.o 
gist = $(PG_BR_PATH)/access/gist/*.o 
hash = $(PG_BR_PATH)/access/hash/*.o 
heap = $(PG_BR_PATH)/access/heap/*.o 
index = $(PG_BR_PATH)/access/index/*.o 
nbtree = $(PG_BR_PATH)/access/nbtree/*.o 
rmgrdesc = $(PG_BR_PATH)/access/rmgrdesc/*.o 
spgist = $(PG_BR_PATH)/access/spgist/*.o 
tablesample = $(PG_BR_PATH)/access/tablesample/*.o 
transam = $(PG_BR_PATH)/access/transam/*.o 
bootstrap = $(PG_BR_PATH)/bootstrap/*.o 
catalog = $(PG_BR_PATH)/catalog/*.o 
parser = $(PG_BR_PATH)/parser/*.o 
commands = $(PG_BR_PATH)/commands/*.o 
executor = $(PG_BR_PATH)/executor/*.o 
foreign = $(PG_BR_PATH)/foreign/foreign.o 
br_lib = $(PG_BR_PATH)/lib/*.o 
br_libpq = $(PG_BR_PATH)/libpq/*.o 
br_dtt = $(PG_BR_PATH)/distributed_txn/*.o 
nodes = $(PG_BR_PATH)/nodes/*.o 
geqo = $(PG_BR_PATH)/optimizer/geqo/*.o 
br_path = $(PG_BR_PATH)/optimizer/path/*.o 
br_plan = $(PG_BR_PATH)/optimizer/plan/*.o 
br_prep = $(PG_BR_PATH)/optimizer/prep/*.o 
br_util = $(PG_BR_PATH)/optimizer/util/*.o 
br_part = $(PG_BR_PATH)/partitioning/*.o 
br_port = $(PG_BR_PATH)/port/*.o 
br_postm = $(PG_BR_PATH)/postmaster/*.o 
regex = $(PG_BR_PATH)/regex/*.o 
logical = $(PG_BR_PATH)/replication/logical/*.o 
br_rep = $(PG_BR_PATH)/replication/*.o 
rewrite = $(PG_BR_PATH)/rewrite/*.o 
statistics = $(PG_BR_PATH)/statistics/*.o 
buffer = $(PG_BR_PATH)/storage/buffer/*.o 
br_file = $(PG_BR_PATH)/storage/file/*.o 
br_freep = $(PG_BR_PATH)/storage/freespace/*.o 
br_ipc = $(PG_BR_PATH)/storage/ipc/*.o 
large_object = $(PG_BR_PATH)/storage/large_object/inv_api.o 
lmgr = $(PG_BR_PATH)/storage/lmgr/*.o 
br_page = $(PG_BR_PATH)/storage/page/*.o 
smgr = $(PG_BR_PATH)/storage/smgr/*.o 
tcop = $(PG_BR_PATH)/tcop/*.o 
tsearch = $(PG_BR_PATH)/tsearch/*.o 
adt = $(PG_BR_PATH)/utils/adt/*.o 
br_cache = $(PG_BR_PATH)/utils/cache/*.o 
br_error = $(PG_BR_PATH)/utils/error/*.o 
fmgr = $(PG_BR_PATH)/utils/fmgr/*.o 
u_hash = $(PG_BR_PATH)/utils/hash/*.o 
u_init = $(PG_BR_PATH)/utils/init/*.o 
u_mb = $(PG_BR_PATH)/utils/mb/*.o 
misc = $(PG_BR_PATH)/utils/misc/*.o 
mmgr = $(PG_BR_PATH)/utils/mmgr/*.o 
resowner = $(PG_BR_PATH)/utils/resowner/resowner.o 
u_sort = $(PG_BR_PATH)/utils/sort/*.o 
u_time = $(PG_BR_PATH)/utils/time/*.o 
u_fmg = $(PG_BR_PATH)/utils/fmgrtab.o 
src_tmzone_l = $(PG_BR_PATH)/../../src/timezone/localtime.o 
src_tmzone_s = $(PG_BR_PATH)/../../src/timezone/strftime.o 
src_tmzone_p = $(PG_BR_PATH)/../../src/timezone/pgtz.o 
jit = $(PG_BR_PATH)/jit/jit.o 
polar_dma = $(PG_BR_PATH)/polar_dma/*.o 
p_datamax = $(PG_BR_PATH)/polar_datamax/polar_datamax.o 
br_port_a = $(PG_BR_PATH)/../../src/port/libpgport_srv.a 
br_common_a = $(PG_BR_PATH)/../../src/common/libpgcommon_srv.a 

OBJS = pg_br.c $(BRIN) $(COMMON) $(gin) $(gist) $(hash) $(heap) $(index) $(nbtree) \
$(rmgrdesc) $(spgist) $(tablesample) $(transam) \
$(bootstrap) $(catalog) $(parser) $(commands) \
$(executor) $(foreign) $(br_lib) $(br_libpq) \
$(br_dtt) $(nodes) $(geqo) $(br_path) \
$(br_plan) $(br_prep) $(br_util) $(br_part) \
$(br_port) $(br_postm) $(regex) $(logical) \
$(br_rep) $(rewrite) $(statistics) $(buffer) \
$(br_file) $(br_freep) $(br_ipc) $(large_object) \
$(lmgr) $(br_page) $(smgr) $(tcop) \
$(tsearch) $(adt) $(br_cache) $(br_error) \
$(fmgr) $(u_hash) $(u_init) $(u_mb) \
$(misc) $(mmgr) $(resowner) $(u_sort) \
$(u_time) $(u_fmg) $(src_tmzone_l) $(src_tmzone_s) \
$(src_tmzone_p) $(jit) $(polar_dma) $(p_datamax) $(br_port_a) $(br_common_a)
