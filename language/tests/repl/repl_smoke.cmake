# A scripted `hgl repl` session over a pipe (the non-terminal path of the
# line reader): bindings persist, composition and runtime declarations join
# the session, a rebuilt native image replaces its provider, `eval` prints its
# sequence, and `:quit` leaves cleanly.
#   cmake -DHGL=<hgl> -DOUT=<dir> -P repl_smoke.cmake
file(MAKE_DIRECTORY "${OUT}")
file(WRITE "${OUT}/input.txt"
"let x = 1 + 2
x
fn twice(v: f64) -> f64 => v * 2.0
eval(twice, v: [1.5, 2.0])
operator magnitude(value: f64) -> f64
impl fn magnitude(value: f64) -> f64 {
    when modified(value) && valid(value) {
        if value < 0.0 {
            return -value
        }
        return value
    }
}
fn magnitude_graph(value: f64) -> f64 => magnitude(value)
eval(magnitude_graph, value: [-2.0, 3.0])
fn unsupported<U>(a: U, b: U) -> U => a
eval(magnitude_graph, value: [4.0, -5.0])
:quit
")
execute_process(
    COMMAND "${HGL}" repl
    INPUT_FILE "${OUT}/input.txt"
    OUTPUT_VARIABLE output
    ERROR_VARIABLE errors
    RESULT_VARIABLE status
)
if(NOT status EQUAL 0)
    message(FATAL_ERROR "hgl repl exited ${status}\n${output}\n${errors}")
endif()
foreach(expected "hgl> 3\n" "[3.0, 4.0]" "[2.0, 3.0]" "[4.0, 5.0]"
        "a generic function is not supported by emit-cpp yet")
    string(FIND "${output}" "${expected}" at)
    if(at EQUAL -1)
        message(FATAL_ERROR "hgl repl output lacks '${expected}':\n${output}\n${errors}")
    endif()
endforeach()
