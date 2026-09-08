cmake_minimum_required(VERSION 3.25)

# The two execution backends consume one hgraph IR and must not carry their
# own copies of a language rule: a fail-closed diagnostic belongs to the
# shared analysis (hgraph_ir/control_flow.h) or to the frontend, and the
# backends only forward it. This test fails when the same diagnostic text is
# passed to `backend(`, `unsupported(`, `report(` or `type_error(` in both
# backend sources.
#
# Two exemptions keep the check about language rules: literals shorter than
# eight characters (punctuation fragments of a composed message) and messages
# beginning with "hgraph IR " (internal invariants of the IR both consumers
# legitimately assert, never a rule an author can hit).

if(NOT DEFINED HGL_LANGUAGE_SOURCE_DIR)
    message(FATAL_ERROR "HGL_LANGUAGE_SOURCE_DIR is required")
endif()

set(_direct "${HGL_LANGUAGE_SOURCE_DIR}/wiring/backend.cpp")
set(_generated "${HGL_LANGUAGE_SOURCE_DIR}/codegen/cpp_emitter.cpp")

function(_hgl_diagnostic_literals out_var source)
    file(READ "${source}" _contents)
    # Every call to one of the reporting helpers, up to its terminating `;`.
    string(REGEX MATCHALL "[^A-Za-z_:](backend|unsupported|report|type_error)\\([^;]*" _calls "${_contents}")
    set(_literals)
    foreach(_call IN LISTS _calls)
        # Every string literal in the call; the quoted text is recovered
        # by stripping the surrounding quotes.
        string(REGEX MATCHALL "\"([^\"\\\\]|\\\\.)*\"" _strings "${_call}")
        foreach(_string IN LISTS _strings)
            string(LENGTH "${_string}" _length)
            math(EXPR _inner_length "${_length} - 2")
            if(_inner_length LESS 8)
                continue()
            endif()
            string(SUBSTRING "${_string}" 1 ${_inner_length} _text)
            if(_text MATCHES "^hgraph IR ")
                continue()
            endif()
            list(APPEND _literals "${_text}")
        endforeach()
    endforeach()
    list(REMOVE_DUPLICATES _literals)
    set(${out_var} "${_literals}" PARENT_SCOPE)
endfunction()

_hgl_diagnostic_literals(_direct_literals "${_direct}")
_hgl_diagnostic_literals(_generated_literals "${_generated}")

set(_shared)
foreach(_literal IN LISTS _direct_literals)
    if(_literal IN_LIST _generated_literals)
        list(APPEND _shared "${_literal}")
    endif()
endforeach()

list(LENGTH _direct_literals _direct_count)
list(LENGTH _generated_literals _generated_count)
message(STATUS "direct backend diagnostics: ${_direct_count}; generated backend diagnostics: ${_generated_count}")

if(_shared)
    list(JOIN _shared "\n  " _listed)
    message(FATAL_ERROR
        "the direct and generated backends both own these diagnostics; move the rule into hgraph IR "
        "(control_flow.h) so it is reported once:\n  ${_listed}")
endif()
