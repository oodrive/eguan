#!/bin/sh

rm -rf autom4te.cache
aclocal $ACLOCAL_FLAGS
autoheader
libtoolize -c --automake
automake --ignore-deps --add-missing
autoconf
