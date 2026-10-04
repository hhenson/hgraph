#include <consumer.h>
#include <contracts.h>
#include <provider.h>

#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/util/scope.h>

#include <optional>
#include <vector>

int main() {
    using namespace hgraph;
    using namespace hgraph::testing;
    auto                                 &registry = OperatorRegistry::instance();
    const auto                            provider = checks::imported_provider::register_operators();
    const auto                            consumer = checks::imported_consumer::register_operators();
    auto                                  cleanup  = make_scope_exit<true>([&] {
        (void)registry.remove_provider(consumer);
        (void)registry.remove_provider(provider);
    });
    const std::vector<std::optional<Int>> input{1, 3};
    const auto                            direct   = eval_node<checks::imported_contracts::operators::adjust>(input, Int{2});
    const auto                            composed = eval_node<checks::imported_consumer::operators::integer>(input);
    const auto                            correct  = [](const auto &result) {
        return result.size() == 2U && result[0] && result[1] && result[0]->equals(Value{Int{3}}) &&
               result[1]->equals(Value{Int{5}});
    };
    const hgl::ordinary::PreparedValuePlan list_plan{scalar_descriptor<hgl::ordinary::List<Int>>::value_meta()};
    auto empty = list_plan.empty_list();
    auto populated = list_plan.empty_list();
    list_plan.push(populated.view(), Value{Int{7}}.view());
    const std::vector<std::optional<Value>> snapshots{populated, empty, std::nullopt, empty};
    const auto atomic = eval_node<checks::imported_consumer::operators::atomic_values, TS<hgl::ordinary::List<Int>>>(snapshots);
    const bool atomic_correct = atomic.size() == 4U && atomic[0] && atomic[1] && !atomic[2] && atomic[3] &&
        atomic[0]->equals(populated) && atomic[1]->equals(empty) && atomic[3]->equals(empty);
    return correct(direct) && correct(composed) && atomic_correct ? 0 : 1;
}
