# src/bin/pg_waldump/Makefile

PGFILEDESC = "polar_tools"
PGAPPICON=win32

subdir = src/bin/polar_tools
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = polar_tools.o \
	   block_header_dump.o \
	   control_data_change.o \
	   logindex_meta_dump.o \
	   logindex_bloom_dump.o \
	   logindex_table_dump.o \
	   logindex_page_dump.o

all: polar_tools

polar_tools: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) polar_tools$(X) '$(DESTDIR)$(bindir)/polar_tools$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/polar_tools$(X)'

clean distclean maintainer-clean:
	rm -f polar_toolsp$(X) $(OBJS)
