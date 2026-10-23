# curl 8.11's finder selects macOS's deprecated compatibility libgssapi.
# Use the supported system GSS framework and its flat gssapi.h instead.
find_library(GSS_LIBRARIES NAMES GSS)
find_path(GSS_INCLUDE_DIRS NAMES gssapi.h
    HINTS "${GSS_LIBRARIES}/Headers" NO_DEFAULT_PATH)
set(GSS_FLAVOUR "Heimdal")
set(GSS_VERSION "system")
set(GSS_LIBRARY_DIRS "")
set(GSS_CFLAGS "")
set(GSS_PC_REQUIRES "")
set(HAVE_GSSAPI_GSSAPI_H OFF CACHE INTERNAL "Use GSS.framework's gssapi.h" FORCE)
set(HAVE_GSSAPI_GSSAPI_GENERIC_H OFF CACHE INTERNAL "Use GSS.framework's gssapi.h" FORCE)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GSS REQUIRED_VARS GSS_LIBRARIES GSS_INCLUDE_DIRS)
