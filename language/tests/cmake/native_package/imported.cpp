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
    return correct(direct) && correct(composed) ? 0 : 1;
}
