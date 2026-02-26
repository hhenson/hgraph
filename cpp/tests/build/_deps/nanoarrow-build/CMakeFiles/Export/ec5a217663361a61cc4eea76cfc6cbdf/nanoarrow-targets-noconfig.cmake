#----------------------------------------------------------------
# Generated CMake target import file.
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "nanoarrow::nanoarrow_static" for configuration ""
set_property(TARGET nanoarrow::nanoarrow_static APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(nanoarrow::nanoarrow_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_NOCONFIG "C"
  IMPORTED_LOCATION_NOCONFIG "${_IMPORT_PREFIX}/lib/libnanoarrow_static.a"
  )

list(APPEND _cmake_import_check_targets nanoarrow::nanoarrow_static )
list(APPEND _cmake_import_check_files_for_nanoarrow::nanoarrow_static "${_IMPORT_PREFIX}/lib/libnanoarrow_static.a" )

# Import target "nanoarrow::nanoarrow_shared" for configuration ""
set_property(TARGET nanoarrow::nanoarrow_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(nanoarrow::nanoarrow_shared PROPERTIES
  IMPORTED_LOCATION_NOCONFIG "${_IMPORT_PREFIX}/lib/libnanoarrow_shared.dylib"
  IMPORTED_SONAME_NOCONFIG "@rpath/libnanoarrow_shared.dylib"
  )

list(APPEND _cmake_import_check_targets nanoarrow::nanoarrow_shared )
list(APPEND _cmake_import_check_files_for_nanoarrow::nanoarrow_shared "${_IMPORT_PREFIX}/lib/libnanoarrow_shared.dylib" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
