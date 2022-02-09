COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c
%.bc : %.cc
	$(COMPILE.cxx.bc) -o $@ $<
	$(LLVM_BINPATH)/opt -module-summary -f $@ -o $@

libdir=$(top_srcdir)/src/backend/polar_dma/libconsensus
override CPPFLAGS := -Iinclude \
					 -I$(libdir)/dependency/easy/src/include \
					 -I$(libdir)/dependency/easy/src/io \
					 -I$(libdir)/dependency/easy/src/thread \
					 -I$(libdir)/dependency/easy/src/util \
					 -I$(libdir)/consensus/include \
					 -std=c++11 -D_GLIBCXX_USE_CXX11_ABI=0 $(CPPFLAGS)
