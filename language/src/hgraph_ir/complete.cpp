#include "hgraph_ir/complete.h"

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace hgl::hgraph_ir
{
    namespace
    {
        [[nodiscard]] std::string operation_name(const Operation &operation) {
            if (!operation.identity.empty()) { return operation.identity; }
            if (!operation.registry_name.empty()) { return operation.registry_name; }
            return "<unnamed>";
        }

        class ExecutionCompleter
        {
          public:
            ExecutionCompleter(Module &module, ProviderPlan providers, syntax::DiagnosticSink &diagnostics)
                : module_{module}, providers_{std::move(providers)}, diagnostics_{diagnostics} {}

            [[nodiscard]] bool run() {
                if (module_.completion != Completion::Bodies) {
                    diagnostics_.report(syntax::Category::Build, {}, "execution completion requires hgraph IR bodies");
                    return false;
                }

                normalize_provider_universe();
                validate_operations();
                validate_requirement_inventory();
                if (diagnostics_.has_errors()) { return false; }

                module_.provider_plan = std::move(providers_);
                module_.completion    = Completion::Executable;
                return true;
            }

          private:
            void normalize_provider_universe() {
                auto &universe = providers_.universe;
                if (std::ranges::any_of(universe, [](const std::string &key) { return key.empty(); })) {
                    diagnostics_.report(syntax::Category::Build, {}, "the locked provider universe contains an empty provider key");
                }
                std::ranges::sort(universe);
                universe.erase(std::unique(universe.begin(), universe.end()), universe.end());
            }

            void validate_operations() {
                for (const Value &value : module_.values) {
                    const Operation &operation = value.operation;
                    if (operation.kind != OperationKind::NominalOperator) { continue; }

                    // Constant-folded language operators do not execute and
                    // therefore need neither a source candidate nor a native
                    // provider at runtime. Folding also makes any pre-fold
                    // deferred marker irrelevant to the executable plan.
                    if (value.constant) { continue; }

                    const std::string name = operation_name(operation);
                    if (operation.deferred) {
                        diagnostics_.report(syntax::Category::Operator, value.range,
                                            "operator '" + name + "' remains deferred and cannot enter an executable plan");
                        continue;
                    }

                    if (operation.candidate.valid()) {
                        if (operation.candidate.value >= module_.callables.size()) {
                            diagnostics_.report(syntax::Category::Build, value.range,
                                                "operator '" + name + "' refers to an invalid source implementation");
                            continue;
                        }
                        const Callable &candidate = module_.callables[operation.candidate.value];
                        if (candidate.visibility != CallableVisibility::Implementation ||
                            candidate.operator_identity != operation.identity) {
                            diagnostics_.report(syntax::Category::Build, value.range,
                                                "operator '" + name + "' refers to a callable that is not its implementation");
                        }
                        continue;
                    }

                    if (operation.provider_key.empty()) {
                        diagnostics_.report(syntax::Category::Operator, value.range,
                                            "operator '" + name + "' has no keyed provider identity for executable planning");
                        continue;
                    }

                    requirements_.insert(operation.provider_key);
                    if (!std::ranges::binary_search(providers_.universe, operation.provider_key)) {
                        diagnostics_.report(syntax::Category::Build, value.range,
                                            "operator '" + name + "' requires provider '" + operation.provider_key +
                                                "', which is absent from the locked provider universe");
                    }
                }
            }

            void validate_requirement_inventory() {
                const std::vector<std::string> actual{requirements_.begin(), requirements_.end()};
                if (module_.provider_requirements != actual) {
                    diagnostics_.report(syntax::Category::Build, {},
                                        "the hgraph IR provider requirement inventory is inconsistent with its operations");
                }
            }

            Module                 &module_;
            ProviderPlan            providers_;
            syntax::DiagnosticSink &diagnostics_;
            std::set<std::string>   requirements_{};
        };
    }  // namespace

    bool complete_execution(Module &module, ProviderPlan providers, syntax::DiagnosticSink &diagnostics) {
        return ExecutionCompleter{module, std::move(providers), diagnostics}.run();
    }
}  // namespace hgl::hgraph_ir
