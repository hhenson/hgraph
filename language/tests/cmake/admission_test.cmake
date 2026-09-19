if(NOT HGL OR NOT OUT)
    message(FATAL_ERROR "HGL and OUT are required")
endif()
file(MAKE_DIRECTORY "${OUT}")
function(rejected name source message)
    set(path "${OUT}/${name}.hgl")
    file(WRITE "${path}" "module admission.${name}\n${source}\n")
    foreach(command check test emit-cpp)
        set(extra)
        if(command STREQUAL "emit-cpp")
            set(extra --out-dir "${OUT}/${name}")
        endif()
        execute_process(COMMAND "${HGL}" "${command}" "${path}" ${extra}
            RESULT_VARIABLE status OUTPUT_VARIABLE output ERROR_VARIABLE error TIMEOUT 20)
        # A signal, timeout, or uncaught exception is not a successful rejection.
        if(NOT "${status}" STREQUAL "1" OR NOT "${output}${error}" MATCHES "${message}")
            message(FATAL_ERROR "${name}: ${command} returned ${status}: ${output}${error}")
        endif()
    endforeach()
endfunction()
rejected(recursive [=[
fn loop(x: i64) -> i64 => loop(x)
test recursion { assert eval(loop, x: [1]) == [1] }
]=] "recursive functions are not supported")
rejected(exported [=[
export fn loop(x: i64) -> i64 => loop(x)
]=] "recursive functions are not supported")
rejected(mutual [=[
export fn first(x: i64) -> i64 => second(x)
fn second(x: i64) -> i64 => first(x)
]=] "recursive functions are not supported")
rejected(value_cycle [=[
const fn first(x: i64) -> i64 => second(x)
const fn second(x: i64) -> i64 => first(x)
]=] "recursive functions are not supported")
rejected(generic_index [=[
operator first<T, const n: i64>(x: list<T, n>) -> T
impl fn first<T, const n: i64>(x: list<T, n>) -> T {
    when modified(x) && valid(x[0]) { return x[0] }
}
instantiate first<i64, _>
]=] "requires a concrete fixed-list extent")
rejected(unguarded [=[
export fn invalid(trigger: bool, x: i64) -> i64 {
    when modified(trigger) && valid(trigger) { return x }
}
]=] "may be invalid here")
rejected(mixed_state [=[
export fn invalid(x: i64) -> i64 {
    state total: i64 = 0
    cache count: i64 = 0
    when { return x + total + count }
}
]=] "'cache' and 'state' cannot be combined")

rejected(generic_traversal [=[
use hgraph.std::{null_sink}
operator observe<T, const n: i64>(samples: list<T, n>)
impl fn observe<T, const n: i64>(samples: list<T, n>) {
    for sample in elements(samples) { null_sink(sample) }
}
instantiate observe<i64, _>
]=] "requires a concrete fixed-list extent")
