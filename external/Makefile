
subdir = external
top_builddir = ..
include $(top_builddir)/src/Makefile.global

SUBDIRS = \
		  polar_vfs \
		  polar_monitor \
		  polar_worker

ifeq ($(with_pfsd),yes)
# polar_vfs can read it
export with_pfsd=yes
endif

$(recurse)
$(recurse_always)
