add_test(
    NAME hgraph_language_cli_help
    COMMAND $<TARGET_FILE:hgl> --help
)
set_tests_properties(hgraph_language_cli_help PROPERTIES
    PASS_REGULAR_EXPRESSION "Usage:"
)

add_test(
    NAME hgraph_language_cli_version
    COMMAND $<TARGET_FILE:hgl> --version
)
set_tests_properties(hgraph_language_cli_version PROPERTIES
    PASS_REGULAR_EXPRESSION "hgl ${PROJECT_VERSION} \\(hgraph [0-9]+\\.[0-9]+\\.[0-9]+\\)"
)
