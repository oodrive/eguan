MAINTAINERCLEANFILES = Makefile.in aclocal.m4 config.h.in configure depcomp install-sh missing config.guess config.sub ltmain.sh m4 *.gcno
ACLOCAL_AMFLAGS = -I autom4te.cache
include $(top_srcdir)/aminclude.am

CWARNING_FLAGS = -Wall -Wextra -pedantic -Wstrict-aliasing -Wstrict-overflow -Wundef -Wshadow -Wformat -Wno-variadic-macros -Werror
CXXWARNING_FLAGS = $(CWARNING_FLAGS) -Wc++0x-compat -Wold-style-cast  -Woverloaded-virtual

if DEBUG
AM_CFLAGS = -g3 -O0 $(CWARNING_FLAGS)
AM_CXXFLAGS = -g3 -O0 -std=gnu++0x $(CXXWARNING_FLAGS)
else
if GCOV
AM_CFLAGS = -O0 -fprofile-arcs -ftest-coverage $(CWARNING_FLAGS)
AM_CXXFLAGS = -O0 -fprofile-arcs -ftest-coverage -std=gnu++0x $(CXXWARNING_FLAGS)
else
AM_CFLAGS = -O3 -g0 $(CWARNING_FLAGS) -DNDEBUG
AM_CXXFLAGS = -O3 -g0 -std=gnu++0x $(CXXWARNING_FLAGS) -DNDEBUG
endif
endif

# generate a program and a shared librarie for tests
bin_PROGRAMS=ibsreplay
lib_LTLIBRARIES = libibsreplay.la

# This file is generated and list the files to compile in the library (*.cpp)
include Makefile.srclist.am

if GCOV
ibsreplay_LDADD = -lgcov
libibsreplay_la_LIBADD = -lgcov
else
libibsreplay_la_LIBADD = 
endif

ibsreplay_CPPFLAGS = -I$(top_srcdir)/include
libibsreplay_la_CPPFLAGS = -I$(top_srcdir)/include
