AC_DEFUN([_PANDORA_SEARCH_MEMCACHED_UTIL],[
  AC_REQUIRE([AC_LIB_PREFIX])

  AC_ARG_ENABLE([libmemcached_utilities],
    [AS_HELP_STRING([--disable-libmemcached_utilities],
      [Build with libmemcached_utilities support @<:@default=on@:>@])],
    [ac_enable_libmemcached_utilities="$enableval"],
    [ac_enable_libmemcached_utilities="yes"])

  AS_IF([test "x$ac_enable_libmemcached_utilities" = "xyes"],[
    AC_LIB_HAVE_LINKFLAGS(memcached_utilities,,[
      #include <stdio.h>
    ],[
    ])
  ],[
    ac_cv_libmemcached_utilities="no"
  ])

  AM_CONDITIONAL(HAVE_LIBMEMCACHED_UTILITIES, [test "x${ac_cv_libmemcached_utilities}" = "xyes"])
])

AC_DEFUN([PANDORA_HAVE_MEMCACHED_UTIL],[
  AC_REQUIRE([_PANDORA_SEARCH_MEMCACHED_UTIL])
])
