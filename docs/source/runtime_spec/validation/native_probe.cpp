#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <iostream>
using namespace hgraph;
template <class R> int count(R &&r) {
  int n = 0;
  for ([[maybe_unused]] auto &&v : r)
    ++n;
  return n;
}
int main() {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<std::int32_t>("int32");
  const auto *schema = registry.tsd(integer, registry.ts(integer));
  Value key{std::int32_t{1}}, seven{std::int32_t{7}};
  TSOutput output{*schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
  for (int n = 0; n < 5; ++n) {
    auto now = MIN_ST + TimeDelta{n};
    auto ov = output.view(now);
    auto dict = ov.as_dict();
    if (n == 1) {
      auto m = dict.begin_mutation(now);
      static_cast<void>(m.at(key.view()));
    }
    if (n == 2) {
      auto m = dict.begin_mutation(now);
      m.set(key.view(), seven.view());
    }
    if (n == 3) {
      auto child = dict.at(key.view());
      static_cast<void>(child.begin_mutation(now).invalidate());
    }
    if (n == 4) {
      auto m = dict.begin_mutation(now);
      static_cast<void>(m.erase(key.view()));
    }
    auto view = input.view(nullptr, now);
    auto in = view.as_dict();
    std::cout << n << " " << view.valid() << " " << view.all_valid() << " "
              << view.modified() << " " << count(in.added_keys()) << " "
              << count(in.removed_keys()) << " " << dict.key_set().modified()
              << "\n";
  }
  TSOutput a{*schema}, b{*schema};
  TSInput ref{TSInputBuilderFactory::checked_builder_for(
      *schema, TSEndpointSchema::peered(schema))};
  Value k2{std::int32_t{2}}, k3{std::int32_t{3}};
  {
    auto av = a.view(MIN_ST);
    auto d = av.as_dict();
    auto m = d.begin_mutation(MIN_ST);
    m.set(key.view(), seven.view());
    m.set(k3.view(), seven.view());
  }
  {
    auto bv = b.view(MIN_ST);
    auto d = bv.as_dict();
    auto m = d.begin_mutation(MIN_ST);
    m.set(k2.view(), seven.view());
    m.set(k3.view(), seven.view());
  }
  ref.view(nullptr, MIN_ST).bind_output_sampled(a.view(MIN_ST), MIN_ST);
  auto t = MIN_ST + TimeDelta{2};
  ref.view(nullptr, t).bind_output_sampled(b.view(t), t);
  auto v = ref.view(nullptr, t);
  auto d = v.as_dict();
  std::cout << "rebind " << v.valid() << " " << v.modified() << " "
            << count(d.added_keys()) << " " << count(d.removed_keys()) << " "
            << count(d.removed_items()) << " " << v.delta_is_sampled_rebind()
            << "\n";
  t = MIN_ST + TimeDelta{3};
  ref.view(nullptr, t).unbind_output();
  v = ref.view(nullptr, t);
  d = v.as_dict();
  std::cout << "withdraw " << v.valid() << " " << v.modified() << " "
            << count(d.removed_keys()) << " " << count(d.removed_items())
            << "\n";
}
