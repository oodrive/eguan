dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(ibs, 2.0)

AC_CANONICAL_SYSTEM
AM_INIT_AUTOMAKE()

AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB

AC_LANG_PUSH([C++])

dnl doxygen support
DX_HTML_FEATURE(ON)
DX_CHM_FEATURE(OFF)
DX_CHI_FEATURE(OFF)
DX_MAN_FEATURE(OFF)
DX_RTF_FEATURE(OFF)
DX_XML_FEATURE(OFF)
DX_PDF_FEATURE(OFF)
DX_PS_FEATURE(OFF)

DX_INIT_DOXYGEN([$PACKAGE_NAME],[Doxyfile],[doc])


LT_INIT
AC_PROG_LIBTOOL

AC_ARG_ENABLE(debug,
AS_HELP_STRING([--enable-debug],
               [enable debugging, default: no]),
[case "${enableval}" in
             yes) debug=true ;;
             no)  debug=false ;;
             *)   AC_MSG_ERROR([bad value ${enableval} for --enable-debug]) ;;
esac],
[debug=false])
AM_CONDITIONAL(DEBUG, test x"$debug" = x"true")

AC_ARG_ENABLE(gcov,
AS_HELP_STRING([--enable-gcov],
               [enable gcov, default: no]),
[case "${enableval}" in
             yes) gcov=true ;;
             no)  gcov=false ;;
             *)   AC_MSG_ERROR([bad value ${enableval} for --enable-gcov]) ;;
esac],
[gcov=false])
AM_CONDITIONAL(GCOV, test x"$gcov" = x"true")

dnl pthread required
AC_CHECK_HEADERS([pthread.h],,
    AC_MSG_ERROR([pthread.h not found.]))
AC_CHECK_LIB(pthread, pthread_create, [LIBS="$LIBS -lpthread"],
    [AC_MSG_ERROR([[libpthread not found.]])])

dnl log4cplus required
AC_CHECK_HEADERS([log4cplus/logger.h],,
    AC_MSG_ERROR([log4cplus/logger.h not found.]))
AC_CHECK_LIB(log4cplus, main, [LIBS="$LIBS -llog4cplus"],
    [AC_MSG_ERROR([[liblog4cplus not found.]])])

dnl snappy required
AC_CHECK_HEADERS([snappy.h],,
    AC_MSG_ERROR([snappy.h not found.]))    
AC_CHECK_LIB(snappy,snappy_compress, [LIBS="$LIBS -lsnappy"],
    [AC_MSG_ERROR([[libsnappy not found.]])])

dnl leveldb required
AC_CHECK_HEADERS([leveldb/db.h],,
    AC_MSG_ERROR([leveldb/db.h not found.]))
AC_CHECK_LIB(leveldb, leveldb_open, [LIBS="$LIBS -lleveldb"],
    [AC_MSG_ERROR([[libleveldb not found.]])],[-lleveldb])

AC_CONFIG_FILES(Makefile)
AC_OUTPUT

