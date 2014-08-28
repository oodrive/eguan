/* This files only include header needed by libuuid
 * to break util-linux dependency the quick and dirty way.
 * No copyright ... do what what you want with this file.
 */
#ifndef __C_H__
#define __C_H__

/* to include usleep */
#define _BSD_SOURCE

#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>

#define xusleep usleep

#ifdef O_CLOEXEC
#define UL_CLOEXECSTR   "e"
#else
#define UL_CLOEXECSTR   ""
#endif

#endif
