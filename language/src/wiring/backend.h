#ifndef HGL_WIRING_BACKEND_H
#define HGL_WIRING_BACKEND_H

#include "semantics/resolve.h"
#include "syntax/ast.h"
#include "syntax/diagnostic.h"
#include "syntax/source.h"

#include <hgraph/util/date_time.h>

#include <cstdint>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// The direct-wiring backend, first pass (developer guide, "Direct-wiring
/// backend" / "First pass"): walks a resolved module and wires composition
/// functions straight into hgraph through `wire_operator`, runs `test`
/// bodies with the record/replay harness, and runs an entry with the
/// tool's own `hgl.print_tick` sink.
namespace hgl::wiring
{
    namespace ast = syntax::ast;

    /// Bootstrap hgraph once per process: standard types, standard
    /// operators, the analytics image when the tool links it, and the
    /// backend's own installer. Safe to call repeatedly.
    void ensure_session();

    /// The resolver's registry question, answered by the process registry.
    [[nodiscard]] bool has_operator(std::string_view name);

    struct TestResult
    {
        std::string name;
        bool        passed{false};
        /// Failure detail (an assertion message or a runtime error); empty
        /// when the test passed. Diagnostics go to the sink instead.
        std::string message{};
        /// The printed form of the body's tail expression, when requested.
        std::string tail{};
    };

    struct TestOptions
    {
        /// Run only the named tests (all when empty).
        std::vector<std::string> names{};
        /// Describe the tail expression of each test body (the REPL).
        bool describe_tail{false};
    };

    /// Run the module's tests. A test whose walk reports a diagnostic fails
    /// and the diagnostic is left in `diagnostics`.
    [[nodiscard]] std::vector<TestResult> run_tests(const syntax::SourceFile &file, const ast::Module &module,
                                                    const semantics::ResolvedModule &resolved,
                                                    const TestOptions &options, syntax::DiagnosticSink &diagnostics);

    enum class RunMode : std::uint8_t
    {
        Simulation,
        RealTime,
    };

    /// One `--set name=text`; `text` is an HGL constant expression.
    struct Setting
    {
        std::string name;
        std::string text;
    };

    struct RunOptions
    {
        std::string                     entry{};  ///< empty = the module's only entry
        RunMode                         mode{RunMode::Simulation};
        std::optional<hgraph::DateTime> start{};
        std::optional<hgraph::DateTime> end{};
        std::optional<hgraph::TimeDelta> end_after{};  ///< `--end <duration>`
        std::vector<Setting>            settings{};
    };

    /// Wire and run an entry (syntax guide, "Running a module"). Each tick
    /// of the entry's result is written to `out` as `time value`. Returns
    /// false when a diagnostic was reported.
    [[nodiscard]] bool run_program(const syntax::SourceFile &file, const ast::Module &module,
                                   const semantics::ResolvedModule &resolved, const RunOptions &options,
                                   syntax::DiagnosticSink &diagnostics, std::ostream &out);

    /// The canonical `datetime` spelling without its `@` (the `time` column
    /// of `hgl run` output).
    [[nodiscard]] std::string format_time(hgraph::DateTime when);
}  // namespace hgl::wiring

#endif  // HGL_WIRING_BACKEND_H
