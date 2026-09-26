#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <iostream>
using namespace hgraph;

void report(const char *label, const TSInputView &view, DateTime now) {
  const auto time = view.last_modified_time();
  std::cout << label << " valid=" << view.valid()
            << " all_valid=" << view.all_valid()
            << " modified=" << view.modified() << " last=";
  if (time == MIN_DT) std::cout << "never";
  else std::cout << (time - MIN_ST).count();
  std::cout << " sampled=" << view.delta_is_sampled_rebind();
  if (view.modified()) {
    const auto delta = view.delta_value();
    std::cout << " delta=" << (delta.valid() ? delta.to_string() : "nil");
  }
  std::cout << " cycle=" << (now - MIN_ST).count() << '\n';
}
int main() {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<std::int32_t>("int32");
  const auto *scalar = registry.ts(integer);
  const auto *list = registry.tsl(scalar, 2);
  TSOutput output{*list};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *list, TSEndpointSchema::peered(list))};
  auto t = MIN_ST;
  Value seven{std::int32_t{7}}, nine{std::int32_t{9}};
  {
    auto out = output.view(t);
    auto children = out.as_list();
    static_cast<void>(children[0].begin_mutation(t).copy_value_from(seven.view()));
    static_cast<void>(children[1].begin_mutation(t).copy_value_from(nine.view()));
  }
  t = MIN_ST + TimeDelta{1};
  input.view(nullptr, t).bind_output_sampled(output.view(t), t);
  auto view = input.view(nullptr, t);
  report("sample-parent", view, t);
  auto children = view.as_list();
  report("sample-left", children[0], t);
  report("sample-right", children[1], t);
  t = MIN_ST + TimeDelta{2};
  static_cast<void>(output.view(t).begin_mutation(t).invalidate());
  report("invalidate-parent", input.view(nullptr, t), t);
  auto invalid_view = input.view(nullptr, t);
  auto invalid = invalid_view.as_list();
  report("invalidate-left", invalid[0], t);
  report("invalidate-right", invalid[1], t);
}
