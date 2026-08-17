# The Python bridge module (roadmap P4). Opt-in only: the default build never
# needs Python (HGRAPH_BUILD_PYTHON_BINDINGS=ON enables this subtree).
if(HGRAPH_BUILD_SHARED)
    set(_hgraph_nanobind_options NB_SHARED NOMINSIZE NOSTRIP)
else()
    set(_hgraph_nanobind_options NB_STATIC NOMINSIZE NOSTRIP)
endif()
if(_hgraph_use_python_stable_abi)
    list(APPEND _hgraph_nanobind_options STABLE_ABI)
endif()
nanobind_add_module(_hgraph ${_hgraph_nanobind_options}
    module.cpp
    py_nodes.cpp
    py_persistence.cpp
    py_ports.cpp
    py_state_services.cpp
    py_type_system.cpp
    py_wiring.cpp
    value_conversion.cpp
)

if(WIN32 AND HGRAPH_USE_PYARROW_ARROW)
    # PyArrow's hashed MSVC support DLLs, staged into the wheel by the
    # install() rules near the end of this file. They are loaded BY arrow.dll
    # rather than linked, so they do not appear in TARGET_RUNTIME_DLLS below.
    file(GLOB _hgraph_pyarrow_support_dlls
        LIST_DIRECTORIES false
        "${HGRAPH_PYARROW_LIBRARY_DIR}/../pyarrow.libs/*.dll"
    )
endif()
if(WIN32 AND (HGRAPH_BUILD_SHARED OR HGRAPH_USE_PYARROW_ARROW))
    # Stub generation below imports _hgraph at BUILD time. Windows has no rpath
    # equivalent - the loader searches the directory holding the .pyd - so every
    # DLL the extension links has to be staged beside it in the build tree. On
    # the other platforms BUILD_RPATH and $ORIGIN cover this, which is why only
    # Windows fails here, with "DLL load failed while importing _hgraph".
    #
    # TARGET_RUNTIME_DLLS is the linked set: the hgraph shared libraries,
    # nanobind's shared ABI library, and PyArrow's Arrow DLLs (imported SHARED
    # targets carrying IMPORTED_LOCATION). The support DLLs above are added
    # explicitly because nothing links them.
    add_custom_command(TARGET _hgraph POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different
            "$<TARGET_RUNTIME_DLLS:_hgraph>"
            ${_hgraph_pyarrow_support_dlls}
            "$<TARGET_FILE_DIR:_hgraph>"
        COMMAND_EXPAND_LISTS
        COMMENT "Staging runtime DLLs beside _hgraph for build-tree imports"
        VERBATIM
    )
endif()

# Ship type information beside the private extension. The public Python
# package is marked as typed below; its lazy operator declarations are
# generated separately from the native registry by tools/api_inventory.py.
set(_hgraph_stub "${CMAKE_CURRENT_BINARY_DIR}/_hgraph.pyi")
nanobind_add_stub(hgraph_python_native_stub
    MODULE _hgraph
    OUTPUT "${_hgraph_stub}"
    PYTHON_PATH "$<TARGET_FILE_DIR:_hgraph>"
    DEPENDS _hgraph
    PATTERN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/_hgraph_stub_patterns.txt"
)
target_link_libraries(_hgraph PRIVATE hgraph_core hgraph::options)
if(HGRAPH_BUILD_SHARED)
    target_link_libraries(_hgraph PRIVATE hgraph_private_dependencies)
endif()
hgraph_enable_private_pch(_hgraph)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # GCC at -O3 (NOMINSIZE) trips -Wmaybe-uninitialized false positives on
    # nanobind's argument-caster storage (tuple<make_caster<Args>...> in
    # nb_func.h) — e.g. the std::optional<PyTsType> arg of tsl_port. The reads
    # are has_value()-guarded; suppress on the bindings target only, GCC only —
    # hgraph_core keeps the warning.
    target_compile_options(_hgraph PRIVATE -Wno-maybe-uninitialized)
endif()

set(_hgraph_install_rpath)
if(HGRAPH_BUILD_SHARED)
    if(APPLE)
        list(APPEND _hgraph_install_rpath "@loader_path/${CMAKE_INSTALL_LIBDIR}")
    elseif(UNIX)
        list(APPEND _hgraph_install_rpath "$ORIGIN/${CMAKE_INSTALL_LIBDIR}")
    endif()
endif()
if(HGRAPH_USE_PYARROW_ARROW AND HGRAPH_PYARROW_LIBRARY_DIR)
    if(APPLE)
        set(_hgraph_pyarrow_rpath "@loader_path/pyarrow")
    elseif(UNIX)
        set(_hgraph_pyarrow_rpath "$ORIGIN/pyarrow")
    else()
        set(_hgraph_pyarrow_rpath "")
    endif()
    set_target_properties(_hgraph PROPERTIES BUILD_RPATH "${HGRAPH_PYARROW_LIBRARY_DIR}")
    if(_hgraph_pyarrow_rpath)
        list(APPEND _hgraph_install_rpath "${_hgraph_pyarrow_rpath}")
    endif()
endif()
if(_hgraph_install_rpath)
    list(REMOVE_DUPLICATES _hgraph_install_rpath)
    set_target_properties(_hgraph PROPERTIES INSTALL_RPATH "${_hgraph_install_rpath}")
endif()

install(TARGETS _hgraph
    LIBRARY DESTINATION .
    RUNTIME DESTINATION .
)
install(FILES "${_hgraph_stub}" DESTINATION .)
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/hgraph/py.typed" DESTINATION hgraph)
if(WIN32 AND HGRAPH_USE_PYARROW_ARROW)
    # Windows does not have an rpath equivalent. Keep the exact Arrow runtime
    # DLLs used at link time and PyArrow's hashed MSVC support DLLs beside the
    # extension so `import _hgraph` works before pyarrow itself is imported.
    # `_hgraph_pyarrow_support_dlls` is globbed alongside the build-tree staging
    # near the top of this file.
    #
    # Every library linked into the extension has to appear here: one missing
    # DLL is not a link error, it is an ImportError on the user's machine.
    # RFC 0016 added Parquet, which is why it is in the list.
    set(_hgraph_wheel_arrow_dlls
        "${HGRAPH_PYARROW_ARROW_RUNTIME}"
        "${HGRAPH_PYARROW_COMPUTE_RUNTIME}"
        "${HGRAPH_PYARROW_ACERO_RUNTIME}"
    )
    if(HGRAPH_WITH_PARQUET)
        list(APPEND _hgraph_wheel_arrow_dlls "${HGRAPH_PYARROW_PARQUET_RUNTIME}")
    endif()
    install(FILES
        ${_hgraph_wheel_arrow_dlls}
        ${_hgraph_pyarrow_support_dlls}
        DESTINATION .
    )
endif()

foreach(_hgraph_py_test IN ITEMS test_bridge test_hgraph_api test_python_authoring test_packaging)
    add_test(
        NAME hgraph_python_${_hgraph_py_test}
        COMMAND "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/tests/${_hgraph_py_test}.py"
    )
    set_tests_properties(hgraph_python_${_hgraph_py_test} PROPERTIES
        ENVIRONMENT "PYTHONPATH=$<TARGET_FILE_DIR:_hgraph>:${CMAKE_CURRENT_SOURCE_DIR}"
    )
endforeach()

# The PORTED hgraph test suite (python/tests/ported): the parity yardstick.
# Near-verbatim copies of release/0.5's tests; GREEN and gating (skips carry
# precise "gap:"/"deviation:" reasons).
add_test(
    NAME hgraph_python_ported_suite
    COMMAND "${Python_EXECUTABLE}" -m pytest "${CMAKE_CURRENT_SOURCE_DIR}/tests/ported" -q -m "not wip"
)
set_tests_properties(hgraph_python_ported_suite PROPERTIES
    ENVIRONMENT "PYTHONPATH=$<TARGET_FILE_DIR:_hgraph>:${CMAKE_CURRENT_SOURCE_DIR}"
    LABELS "parity"
)
