#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/impl/collection_input_semantics.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/subgraph_wiring.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib {
using namespace hgraph::operator_type_resolution;

namespace container_impl_detail {
[[nodiscard]] inline std::size_t
normalize_item_index(Int index, std::size_t size, std::string_view subject) {
  const Int signed_size = static_cast<Int>(size);
  const Int normalized = index < 0 ? signed_size + index : index;
  if (normalized < 0 || normalized >= signed_size) {
    std::string message{"getitem_: "};
    message += subject;
    message += " index out of range";
    throw std::out_of_range(message);
  }
  return static_cast<std::size_t>(normalized);
}

template <typename T>
void set_if_changed(const Out<TS<T>> &out, const T &value) {
  if (!out.valid() || out.value().template checked_as<T>() != value) {
    out.set(value);
  }
}

[[nodiscard]] inline const TSValueTypeMetaData *
direct_tsb_schema(const WiringArg &arg) noexcept {
  const TSValueTypeMetaData *schema =
      time_series_schema(arg, SchemaRefMode::Direct);
  return schema != nullptr && schema->kind == TSTypeKind::TSB ? schema
                                                              : nullptr;
}

[[nodiscard]] inline const TSValueTypeMetaData *
ref_indexed_schema(const WiringArg &arg, TSTypeKind kind) noexcept {
  const TSValueTypeMetaData *surface =
      time_series_schema(arg, SchemaRefMode::Direct);
  if (surface == nullptr || surface->kind != TSTypeKind::REF) {
    return nullptr;
  }
  const TSValueTypeMetaData *target =
      TypeRegistry::instance().dereference(surface);
  return target != nullptr && target->kind == kind ? target : nullptr;
}

/** REF[TSB] argument: the referenced bundle schema, or null. */
[[nodiscard]] inline const TSValueTypeMetaData *
ref_tsb_schema(const WiringArg &arg) noexcept {
  return ref_indexed_schema(arg, TSTypeKind::TSB);
}

/** REF[TSL] argument: the referenced list schema, or null. */
[[nodiscard]] inline const TSValueTypeMetaData *
ref_tsl_schema(const WiringArg &arg) noexcept {
  return ref_indexed_schema(arg, TSTypeKind::TSL);
}

[[nodiscard]] inline std::optional<std::size_t>
find_tsb_field_index(const TSValueTypeMetaData &schema,
                     std::string_view name) noexcept {
  for (std::size_t index = 0; index < schema.field_count(); ++index) {
    const TSFieldMetaData &field = schema.fields()[index];
    if (field.name != nullptr && name == std::string_view{field.name}) {
      return index;
    }
  }
  return std::nullopt;
}

[[nodiscard]] inline std::optional<std::size_t>
find_tsb_field_index(const TSValueTypeMetaData &schema, Int index) {
  if (schema.field_count() == 0) {
    return std::nullopt;
  }
  const Int signed_size = static_cast<Int>(schema.field_count());
  const Int normalized = index < 0 ? signed_size + index : index;
  if (normalized < 0 || normalized >= signed_size) {
    return std::nullopt;
  }
  return static_cast<std::size_t>(normalized);
}

template <typename Key>
[[nodiscard]] inline const TSValueTypeMetaData *
tsb_field_schema(const TSValueTypeMetaData &schema, const Key &key) {
  std::optional<std::size_t> index = find_tsb_field_index(schema, key);
  return index.has_value() ? schema.fields()[*index].type : nullptr;
}

template <typename Key>
[[nodiscard]] inline const TSValueTypeMetaData *
tsb_field_schema_from_context(OperatorCallContext context,
                              std::string_view key_name) {
  if (context.args.size() != 2) {
    return nullptr;
  }
  const TSValueTypeMetaData *schema = direct_tsb_schema(context.args[0]);
  const Key *key = context.scalar_as<Key>(key_name);
  return schema != nullptr && key != nullptr ? tsb_field_schema(*schema, *key)
                                             : nullptr;
}

template <typename Key>
[[nodiscard]] inline bool has_tsb_field(OperatorCallContext context,
                                        std::string_view key_name) {
  return tsb_field_schema_from_context<Key>(context, key_name) != nullptr;
}

template <typename Key>
inline void resolve_tsb_field_output(ResolutionMap &resolution,
                                     OperatorCallContext context,
                                     std::string_view key_name) {
  if (output_bound(resolution)) {
    return;
  }
  bind_output(resolution,
              tsb_field_schema_from_context<Key>(context, key_name));
}

[[nodiscard]] inline const TSValueTypeMetaData *
materialized_indexed_reference_schema(const TSValueTypeMetaData &container) {
  auto &registry = TypeRegistry::instance();
  if (container.kind == TSTypeKind::TSL) {
    return registry.tsl(registry.ref(container.element_ts()),
                        container.fixed_size());
  }

  std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
  fields.reserve(container.field_count());
  for (std::size_t index = 0; index < container.field_count(); ++index) {
    const TSFieldMetaData &field = container.fields()[index];
    fields.emplace_back(field.name != nullptr ? field.name : "",
                        registry.ref(field.type));
  }
  return registry.un_named_tsb(fields);
}

/** Follow and validate the peered target carried by an indexed-container
    reference. TSB and TSL projection share this path so forwarding,
    descriptive-schema references, and schema errors behave identically. */
[[nodiscard]] inline std::optional<TSOutputView>
resolve_indexed_reference_target(
    const TimeSeriesReference &reference,
    const TSValueTypeMetaData &container,
    DateTime evaluation_time,
    std::string_view operation,
    std::string_view subject) {
  if (!reference.is_peered() || !reference.has_output()) {
    return std::nullopt;
  }

  auto target = resolve_forwarding_source(
      reference.target_output().view(evaluation_time));

  const auto *data_schema = target.data_view().schema();
  if (data_schema != nullptr && data_schema->kind == TSTypeKind::REF) {
    // A descriptive-schema reference: the surface says TSB/TSL but the
    // underlying DATA is itself a REF output (Port::as / reference-service
    // pattern). Hop through its from-REF alternative before projection.
    const auto *deref = TypeRegistry::instance().dereference(data_schema);
    target = target.binding_for(*deref).view(evaluation_time);
  }
  if (!time_series_schema_equivalent(&container, target.schema())) {
    const auto owner = target.owner_node();
    throw std::invalid_argument(
        std::string{operation} + ": " + std::string{subject} +
        " requested schema '" + std::string{container.name()} +
        "' but targets schema '" +
        (target.schema() != nullptr ? std::string{target.schema()->name()}
                                    : std::string{"<untyped>"}) +
        "' from node '" +
        (owner.valid() ? std::string{owner.label()}
                       : std::string{"<unknown>"}) +
        "' at index " +
        (owner.valid() ? std::to_string(owner.node_index())
                       : std::string{"<unknown>"}));
  }
  return target;
}

/** Project one child reference from either a peered indexed-container target
    or a non-peered composite reference value. */
[[nodiscard]] inline TimeSeriesReference project_indexed_reference_element(
    const TimeSeriesReference &reference,
    const TSOutputView *target,
    const TSValueTypeMetaData &container,
    const TSValueTypeMetaData *element_type,
    std::size_t index,
    std::string_view operation,
    std::string_view subject,
    std::string_view element_kind,
    std::string_view element_label) {
  TimeSeriesReference result = TimeSeriesReference::empty(element_type);
  if (target != nullptr) {
    const auto child_count = target->data_view().indexed_child_count();
    if (index >= child_count) {
      throw std::out_of_range(
          std::string{operation} + ": " + std::string{subject} + " " +
          std::string{element_kind} + " '" + std::string{element_label} +
          "' has ordinal " + std::to_string(index) +
          " in requested schema '" + std::string{container.name()} +
          "', but target schema '" +
          (target->schema() != nullptr ? std::string{target->schema()->name()}
                                       : std::string{"<untyped>"}) +
          "' exposes " + std::to_string(child_count) + " children");
    }
    auto child = target->indexed_child_at(index);
    if (child.bound()) {
      result = TimeSeriesReference::peered(child);
    }
  } else if (reference.is_non_peered() && index < reference.items().size()) {
    result = reference[index];
  }

  const auto *actual = result.target_schema();
  if (actual == nullptr) {
    if (result.is_empty()) {
      return TimeSeriesReference::empty(element_type);
    }
    throw std::invalid_argument(
        std::string{operation} + ": " + std::string{subject} + " " +
        std::string{element_kind} + " '" + std::string{element_label} +
        "' reference has no target schema");
  }

  auto &registry = TypeRegistry::instance();
  if (!time_series_schema_equivalent(registry.dereference(actual),
                                     registry.dereference(element_type))) {
    throw std::invalid_argument(
        std::string{operation} + ": " + std::string{subject} + " " +
        std::string{element_kind} + " '" + std::string{element_label} +
        "' reference targets schema '" + std::string{actual->name()} +
        "' but expected '" + std::string{element_type->name()} + "'");
  }
  return result;
}

[[nodiscard]] inline TimeSeriesReference project_tsb_reference_field(
    const TimeSeriesReference &reference,
    const TSValueTypeMetaData &bundle,
    std::size_t index,
    DateTime evaluation_time,
    std::string_view operation,
    std::string_view field_label) {
  if (index >= bundle.field_count()) {
    throw std::out_of_range(std::string{operation} + ": REF[TSB] field '" +
                            std::string{field_label} + "' is out of range");
  }
  auto target = resolve_indexed_reference_target(
      reference, bundle, evaluation_time, operation, "REF[TSB]");
  return project_indexed_reference_element(
      reference, target ? &*target : nullptr, bundle,
      bundle.fields()[index].type, index, operation, "REF[TSB]", "field",
      field_label);
}

inline void set_reference_if_changed(const TSOutputView &out,
                                     TimeSeriesReference reference) {
  Value value{std::move(reference)};
  if (out.data_view().has_current_value() && out.value().equals(value.view())) {
    return;
  }
  auto mutation = out.data_view().begin_mutation(out.evaluation_time());
  static_cast<void>(mutation.move_value_from(std::move(value)));
}
} // namespace container_impl_detail

struct getitem_string {
  static void eval(In<"ts", TS<Str>> ts, In<"key", TS<Int>> key,
                   Out<TS<Str>> out) {
    const Str value = ts.value();
    const std::size_t index = container_impl_detail::normalize_item_index(
        key.value(), value.size(), "string");
    out.set(value.substr(index, 1));
  }
};

struct contains_string {
  static void eval(In<"ts", TS<Str>> ts, In<"item", TS<Str>> item,
                   Out<TS<Bool>> out) {
    out.set(ts.value().find(item.value()) != Str::npos);
  }
};

struct len_string {
  static void eval(In<"ts", TS<Str>> ts, Out<TS<Int>> out) {
    out.set(static_cast<Int>(ts.value().size()));
  }
};

struct is_empty_string {
  static void eval(In<"ts", TS<Str>> ts, Out<TS<Bool>> out) {
    out.set(ts.value().empty());
  }
};

struct len_tss {
  static constexpr bool schedule_on_start = true;

  static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                   Out<TS<Int>> out) {
    // upstream parity: a NEVER-VALID input never ticks; empty-after-removal
    // is a valid set and emits 0.
    if (!ts.valid()) { return; }
    container_impl_detail::set_if_changed(out, static_cast<Int>(ts.size()));
  }
};

struct is_empty_tss {
  static constexpr bool schedule_on_start = true;

  static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                   Out<TS<Bool>> out) {
    if (!collection_input_semantics::has_bound_source(ts)) { return; }
    // upstream parity (ported test_is_empty): a never-valid input READS
    // empty and ticks True at start — unlike len_, which stays silent.
    container_impl_detail::set_if_changed(out, !ts.valid() || ts.empty());
  }
};

struct contains_tss_item {
  static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                   In<"item", TS<ScalarVar<"K">>> item, Out<TS<Bool>> out) {
    if (!collection_input_semantics::has_bound_source(ts)) { return; }
    // upstream parity (issue #149): contains SEEDS False before the set
    // first ticks (upstream initializes the per-item contains ref-output)
    // — a never-valid input reads as EMPTY, unlike len_, which stays
    // silent until the set is valid.
    const Bool value =
        ts.valid() && ts.base().as_set().contains(item.base().value());
    // hgraph parity: an ITEM tick always re-publishes (upstream
    // re-samples the per-item contains output on rebind); a SET tick
    // only publishes a membership change.
    if (item.modified()) {
      out.set(value);
    } else {
      container_impl_detail::set_if_changed(out, value);
    }
  }
};

struct contains_tss_subset {
  static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                   In<"item", TSS<ScalarVar<"K">>> item, Out<TS<Bool>> out) {
    if (!ts.valid()) { return; }
    // Unlike scalar membership, upstream evaluates subset membership only
    // after the subject set becomes valid.
    const auto &item_set = item.base().as_set();
    Bool contains_all = true;
    for (const ValueView &value : item_set.values()) {
      if (!ts.base().as_set().contains(value)) {
        contains_all = false;
        break;
      }
    }
    container_impl_detail::set_if_changed(out, contains_all);
  }
};

struct len_tsd {
  static constexpr bool schedule_on_start = true;

  static void
  eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
       Out<TS<Int>> out) {
    // upstream parity: a NEVER-VALID input never ticks; empty-after-removal
    // is a valid dict and emits 0.
    if (!ts.valid()) { return; }
    container_impl_detail::set_if_changed(out, static_cast<Int>(ts.size()));
  }
};

struct is_empty_tsd {
  static constexpr bool schedule_on_start = true;

  static void
  eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
       Out<TS<Bool>> out) {
    if (!collection_input_semantics::has_bound_source(ts)) { return; }
    // upstream parity (ported test_is_empty): a never-valid input READS
    // empty and ticks True at start — unlike len_, which stays silent.
    container_impl_detail::set_if_changed(out, !ts.valid() || ts.empty());
  }
};

/** ``item in ts`` over a SET-VALUED scalar TS (frozenset payloads). */
struct contains_set_scalar {
  static constexpr auto name = "contains_set_scalar";

  static bool requires_(const ResolutionMap &resolution, OperatorCallContext) {
    const auto *subject = resolution.find_ts("S");
    return subject != nullptr && subject->kind == TSTypeKind::TS &&
           subject->value_schema != nullptr &&
           subject->value_schema->try_value_kind() == ValueTypeKind::Set;
  }

  static void eval(In<"ts", TsVar<"S">> ts, In<"item", TsVar<"I">> item,
                   Out<TS<Bool>> out) {
    const Bool value = ts.base().value().as_set().contains(item.base().value());
    // hgraph parity: an ITEM tick always re-publishes; a subject
    // tick only publishes a membership change.
    if (item.base().modified()) {
      out.set(value);
    } else {
      container_impl_detail::set_if_changed(out, value);
    }
  }
};

struct contains_tsd_key {
  static void
  eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
       In<"item", TS<ScalarVar<"K">>> item, Out<TS<Bool>> out) {
    if (!collection_input_semantics::has_bound_source(ts)) { return; }
    // upstream parity (issue #149): contains SEEDS False before the dict
    // first ticks (upstream initializes the per-key contains ref-output)
    // — a never-valid input reads as EMPTY, unlike len_, which stays
    // silent until the dict is valid.
    const Bool value =
        ts.valid() && ts.base().as_dict().contains(item.base().value());
    // hgraph parity: an ITEM tick always re-publishes (upstream
    // re-samples the per-key contains output on rebind); a DICT tick
    // only publishes a membership change.
    if (item.modified()) {
      out.set(value);
    } else {
      container_impl_detail::set_if_changed(out, value);
    }
  }
};

struct len_tsl {
  static constexpr bool schedule_on_start = true;

  // Any element kind (upstream: TSL[TIME_SERIES_TYPE, SIZE]) — a TSL of
  // bundles or nested lists has a size too (issue #81).
  static void eval(
      In<"ts", TSL<TsVar<"E">, SIZE<"N">>, InputValidity::Unchecked> ts,
      Out<TS<Int>> out) {
    container_impl_detail::set_if_changed(out, static_cast<Int>(ts.size()));
  }
};

/** len_ over a TSW: the CURRENT buffer length (grows until the window
    fills, then stays put — no-change means no tick). */
struct len_tsw {
  static constexpr auto name = "len_tsw";
  static constexpr bool schedule_on_start = true;

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return context.args.size() == 1 &&
           time_series_arg_matches<AnyTSW>(context, 0);
  }

  static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                   Out<TS<Int>> out) {
    if (!ts.valid()) {
      container_impl_detail::set_if_changed(out, Int{0});
      return;
    }
    const TSWInputView window_input{ts.base().borrowed_ref()};
    container_impl_detail::set_if_changed(
        out, static_cast<Int>(window_input.data_view().size()));
  }
};

struct getitem_tsl_by_index {
  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    if (context.args.size() != 2) {
      return false;
    }
    const TSValueTypeMetaData *schema =
        time_series_schema_as<AnyTSL>(context.args[0]);
    if (schema == nullptr) {
      return false;
    }

    // Fixed-size scalar indexing is a structural wiring projection
    // (tsl_element) when the port is DIRECTLY TSL-shaped. A
    // REF-wrapped fixed TSL has no structural form - it takes this
    // dynamic path (the from-REF adaptation resolves the input).
    const auto *port_schema = context.args[0].port.schema;
    const bool direct =
        port_schema != nullptr && port_schema->kind == TSTypeKind::TSL;
    if (context.args[1].kind == WiringArg::Kind::Scalar &&
        schema->fixed_size() != 0 && direct) {
      return false;
    }
    return true;
  }

  static void
  eval(In<"ts", TSL<TsVar<"E">, SIZE<"N">>, InputValidity::Unchecked> ts,
       In<"key", TS<Int>> key, Out<REF<TsVar<"E">>> out) {
    // The ts input is ACTIVE: element/reference REBINDS (e.g. race
    // re-targeting its winner) must re-emit the item reference, not
    // only key ticks. Same-reference re-publishes are deduped so
    // ordinary element value ticks do not retrigger consumers.
    const std::size_t index = container_impl_detail::normalize_item_index(
        key.value(), ts.size(), "TSL");
    TimeSeriesReference reference = ts[index].base().reference();
    if (out.valid() &&
        out.value().checked_as<TimeSeriesReference>() == reference) {
      return;
    }
    out.set(std::move(reference));
  }
};

/** getattr_ over TSD<K, REF<TSB>>: per-key REFERENCE to the named field
    child (hgraph's tsd_get_bundle_item; the race field_item shape). */
struct getattr_tsd {
  static constexpr auto name = "getattr_tsd";

  [[nodiscard]] static const TSValueTypeMetaData *
  element_bundle_schema(const TSValueTypeMetaData *tsd) {
    const auto *dict = time_series_schema_as<AnyTSD>(tsd);
    return dict != nullptr ? time_series_schema_as<AnyTSB>(dict->element_ts())
                           : nullptr;
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    if (context.args.size() != 2 ||
        context.args[0].kind != WiringArg::Kind::TimeSeries) {
      return false;
    }
    const auto *bundle = element_bundle_schema(context.args[0].port.schema);
    const Str *attr = context.scalar_as<Str>("attr");
    return bundle != nullptr && attr != nullptr &&
           container_impl_detail::find_tsb_field_index(*bundle, *attr)
               .has_value();
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (resolution.find_ts("__out__") != nullptr) {
      return;
    }
    if (context.args.size() != 2) {
      return;
    }
    const auto *tsd =
        time_series_schema_as<AnyTSD>(context.args[0].port.schema);
    const auto *bundle = element_bundle_schema(context.args[0].port.schema);
    const Str *attr = context.scalar_as<Str>("attr");
    if (tsd == nullptr || bundle == nullptr || attr == nullptr) {
      return;
    }
    const auto index =
        container_impl_detail::find_tsb_field_index(*bundle, *attr);
    if (!index.has_value()) {
      return;
    }
    auto &registry = TypeRegistry::instance();
    const auto *field = bundle->fields()[*index].type;
    const auto *out_field =
        field->kind == TSTypeKind::REF ? field : registry.ref(field);
    resolution.bind_ts("__out__", registry.tsd(tsd->key_type(), out_field));
  }

  /** The field child of a bundle reference (the race field_item shape:
      peered -> the target's field handle; non-peered -> items()[i]). */
  [[nodiscard]] static TimeSeriesReference
  field_item(const TimeSeriesReference &reference, std::size_t field) {
    if (reference.is_empty()) {
      return TimeSeriesReference{};
    }
    if (reference.is_non_peered()) {
      return field < reference.items().size() ? reference[field]
                                              : TimeSeriesReference{};
    }
    const TSOutputHandle &target = reference.target_output();
    auto data = target.data_view();
    auto bundle = data.as_bundle();
    if (field >= bundle.size()) {
      return TimeSeriesReference{};
    }
    return TimeSeriesReference{
        TSOutputHandle{target.output(), bundle.at(field)}};
  }

  static void
  eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"S">>, InputValidity::Unchecked> ts,
       Scalar<"attr", Str> attr, Out<TsVar<"__out__">> out) {
    const auto *bundle_schema = element_bundle_schema(ts.base().schema());
    const auto field = *container_impl_detail::find_tsb_field_index(
        *bundle_schema, attr.value());

    const auto &erased = static_cast<const TSOutputView &>(out);
    auto dict_out = erased.data_view().as_dict();
    auto mutation = dict_out.begin_mutation(erased.evaluation_time());

    const TSDInputView &dict = ts;
    for (const auto key : dict.removed_keys()) {
      (void)mutation.erase(key);
    }
    for (auto &&[key, child] : dict.modified_items()) {
      // The child's OWN reference (input machinery: ref-element
      // values pass through; plain elements produce peered refs).
      const TimeSeriesReference reference = child.reference();
      auto element = mutation.at(key);
      auto element_mutation =
          TSOutputView{erased.output(), element, erased.evaluation_time()}
              .begin_mutation(erased.evaluation_time());
      static_cast<void>(element_mutation.move_value_from(
          Value{field_item(reference, field)}));
    }
  }
};

/** getattr_(TS[Enum], attr): ``name`` renders the MEMBER NAME (the enum
    ops' to_string), ``value`` the assigned integer. */
struct getattr_enum {
  static constexpr auto name = "getattr_enum";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
    if (schema == nullptr || schema->value_schema == nullptr ||
        !schema->value_schema->is_enum()) {
      return false;
    }
    const auto *attr = context.scalar_as<Str>("attr");
    return attr != nullptr && (*attr == "name" || *attr == "value");
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (output_bound(resolution)) {
      return;
    }
    const auto *attr = context.scalar_as<Str>("attr");
    if (attr == nullptr) {
      return;
    }
    auto &registry = TypeRegistry::instance();
    bind_output(resolution,
                registry.ts(*attr == "name"
                                ? scalar_descriptor<Str>::value_meta()
                                : scalar_descriptor<Int>::value_meta()));
  }

  static void eval(In<"ts", TS<ScalarVar<"E">>> ts, Scalar<"attr", Str> attr,
                   Out<TsVar<"__out__">> out) {
    const ValueView value = ts.base().value();
    if (attr.value() == "name") {
      const auto &erased = static_cast<const TSOutputView &>(out);
      auto mutation =
          erased.data_view().begin_mutation(erased.evaluation_time());
      static_cast<void>(
          mutation.copy_value_from(Value{Str{value.to_string()}}.view()));
    } else {
      const auto &erased = static_cast<const TSOutputView &>(out);
      auto mutation =
          erased.data_view().begin_mutation(erased.evaluation_time());
      static_cast<void>(
          mutation.copy_value_from(Value{value.checked_as<Int>()}.view()));
    }
  }
};

/** getattr_(TS[CompoundScalar], attr): the named FIELD of the bundle
    value (hgraph's getattr_cs; CS IS a Bundle value - the C++-first
    ruling). An UNSET field does not tick. */
struct getattr_ts_bundle {
  static constexpr auto name = "getattr_ts_bundle";

  [[nodiscard]] static const ValueTypeMetaData *
  bundle_meta(OperatorCallContext context) {
    const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
    if (schema == nullptr || schema->value_schema == nullptr ||
        schema->value_schema->value_kind() != ValueTypeKind::Bundle) {
      return nullptr;
    }
    return schema->value_schema;
  }

  [[nodiscard]] static std::optional<std::size_t>
  field_index(const ValueTypeMetaData *bundle, std::string_view field_name) {
    for (std::size_t index = 0; index < bundle->field_count; ++index) {
      if (bundle->fields[index].name != nullptr &&
          bundle->fields[index].name == field_name) {
        return index;
      }
    }
    return std::nullopt;
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    const auto *bundle = bundle_meta(context);
    const Str *attr = context.scalar_as<Str>("attr");
    return bundle != nullptr && attr != nullptr &&
           field_index(bundle, *attr).has_value();
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (output_bound(resolution)) {
      return;
    }
    const auto *bundle = bundle_meta(context);
    const Str *attr = context.scalar_as<Str>("attr");
    if (bundle == nullptr || attr == nullptr) {
      return;
    }
    const auto index = field_index(bundle, *attr);
    if (!index.has_value()) {
      return;
    }
    bind_output(resolution,
                TypeRegistry::instance().ts(bundle->fields[*index].type));
  }

  static void eval(In<"ts", TS<ScalarVar<"S">>> ts, Scalar<"attr", Str> attr,
                   Out<TsVar<"__out__">> out) {
    const auto &erased = static_cast<const TSOutputView &>(out);
    const auto value = ts.base().value();
    auto fields = value.as_indexed_view();
    const auto *meta = value.schema();
    for (std::size_t index = 0; index < meta->field_count; ++index) {
      if (meta->fields[index].name == nullptr ||
          attr.value() != meta->fields[index].name) {
        continue;
      }
      auto field = fields.at(index);
      if (!field.valid()) {
        return;
      } // UNSET fields do not tick
      if (erased.data_view().has_current_value() &&
          erased.value().equals(field)) {
        return;
      }
      auto mutation =
          erased.data_view().begin_mutation(erased.evaluation_time());
      static_cast<void>(mutation.copy_value_from(field));
      return;
    }
  }
};

/** getattr_ over TS[tuple[CompoundScalar, ...]]: the named field of
    EACH element - unset fields become holes (python None) or, in the
    default form, the fallback scalar. */
struct getattr_ts_tuple_bundle {
  static constexpr auto name = "getattr_ts_tuple_bundle";

  [[nodiscard]] static const ValueTypeMetaData *
  element_bundle(OperatorCallContext context) {
    const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
    if (schema == nullptr || schema->value_schema == nullptr) {
      return nullptr;
    }
    const auto *value = schema->value_schema;
    if (value->value_kind() != ValueTypeKind::List ||
        value->element_type == nullptr ||
        value->element_type->value_kind() != ValueTypeKind::Bundle) {
      return nullptr;
    }
    return value->element_type;
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    const auto *bundle = element_bundle(context);
    const Str *attr = context.scalar_as<Str>("attr");
    return bundle != nullptr && attr != nullptr &&
           getattr_ts_bundle::field_index(bundle, *attr).has_value();
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (output_bound(resolution)) {
      return;
    }
    const auto *bundle = element_bundle(context);
    const Str *attr = context.scalar_as<Str>("attr");
    if (bundle == nullptr || attr == nullptr) {
      return;
    }
    const auto index = getattr_ts_bundle::field_index(bundle, *attr);
    if (!index.has_value()) {
      return;
    }
    auto &registry = TypeRegistry::instance();
    bind_output(resolution,
                registry.ts(registry.list(bundle->fields[*index].type, 0,
                                          /*variadic_tuple=*/true)));
  }

  static void emit(const TSInputView &ts, std::string_view field_name,
                   const ValueView *fallback, const TSOutputView &out) {
    const auto value = ts.value();
    const auto *element_meta = value.schema()->element_type;
    const auto index = getattr_ts_bundle::field_index(element_meta, field_name);
    if (!index.has_value()) {
      return;
    }
    const auto field_binding = value_type_for_active_realization(
        element_meta->fields[*index].type);
    if (field_binding == nullptr) {
      return;
    }

    ListBuilder builder{field_binding, *out.schema()->value_schema};
    auto list = value.as_indexed_view();
    for (std::size_t i = 0; i < list.size(); ++i) {
      const ValueView &element = list.at(i);
      auto fields = element.as_indexed_view();
      const ValueView &field = fields.at(*index);
      if (field.has_value()) {
        builder.push_back(field);
      } else if (fallback != nullptr) {
        builder.push_back(*fallback);
      } else {
        builder.push_back_unset();
      }
    }
    auto mutation = out.data_view().begin_mutation(out.evaluation_time());
    static_cast<void>(mutation.move_value_from(builder.build()));
  }

  static void eval(In<"ts", TS<ScalarVar<"S">>> ts, Scalar<"attr", Str> attr,
                   Out<TsVar<"__out__">> out) {
    emit(ts.base(), attr.value(), nullptr,
         static_cast<const TSOutputView &>(out));
  }
};

/** The default form: getattr_(tuple_of_cs_ts, attr, default). */
struct getattr_ts_tuple_bundle_default {
  static constexpr auto name = "getattr_ts_tuple_bundle_default";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    const auto *bundle = getattr_ts_tuple_bundle::element_bundle(context);
    const Str *attr = context.scalar_as<Str>("attr");
    if (bundle == nullptr || attr == nullptr) {
      return false;
    }
    const auto index = getattr_ts_bundle::field_index(bundle, *attr);
    if (!index.has_value()) {
      return false;
    }
    const WiringArg *fallback = scalar_arg_at(context, 2);
    return fallback != nullptr &&
           fallback->scalar_meta == bundle->fields[*index].type;
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    getattr_ts_tuple_bundle::resolve_default_types(resolution, context);
  }

  static void eval(In<"ts", TS<ScalarVar<"S">>> ts, Scalar<"attr", Str> attr,
                   Scalar<"default", ScalarVar<"D">> fallback,
                   Out<TsVar<"__out__">> out) {
    const ValueView value = fallback.value();
    getattr_ts_tuple_bundle::emit(ts.base(), attr.value(), &value,
                                  static_cast<const TSOutputView &>(out));
  }
};

/** getattr_(ts, attr, default): the bundle field, or the DEFAULT scalar
    when that field is UNSET (hgraph's default-valued attribute read). */
struct getattr_ts_bundle_default {
  static constexpr auto name = "getattr_ts_bundle_default";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    const auto *bundle = getattr_ts_bundle::bundle_meta(context);
    const Str *attr = context.scalar_as<Str>("attr");
    return bundle != nullptr && attr != nullptr &&
           getattr_ts_bundle::field_index(bundle, *attr).has_value() &&
           scalar_arg_at(context, 2) != nullptr;
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (output_bound(resolution)) {
      return;
    }
    const auto *bundle = getattr_ts_bundle::bundle_meta(context);
    const Str *attr = context.scalar_as<Str>("attr");
    if (bundle == nullptr || attr == nullptr) {
      return;
    }
    const auto index = getattr_ts_bundle::field_index(bundle, *attr);
    if (!index.has_value()) {
      return;
    }
    bind_output(resolution,
                TypeRegistry::instance().ts(bundle->fields[*index].type));
  }

  static void eval(In<"ts", TS<ScalarVar<"S">>> ts, Scalar<"attr", Str> attr,
                   Scalar<"default", ScalarVar<"D">> fallback,
                   Out<TsVar<"__out__">> out) {
    const auto &erased = static_cast<const TSOutputView &>(out);
    const auto value = ts.base().value();
    auto fields = value.as_indexed_view();
    const auto *meta = value.schema();
    for (std::size_t index = 0; index < meta->field_count; ++index) {
      if (meta->fields[index].name == nullptr ||
          attr.value() != meta->fields[index].name) {
        continue;
      }
      auto field = fields.at(index);
      const auto publish = [&](const ValueView &chosen) {
        if (erased.data_view().has_current_value() &&
            erased.value().equals(chosen)) {
          return;
        }
        auto mutation =
            erased.data_view().begin_mutation(erased.evaluation_time());
        static_cast<void>(mutation.copy_value_from(chosen));
      };
      if (field.valid()) {
        publish(field);
      } else {
        publish(fallback.value());
      }
      return;
    }
  }
};

/** setattr_(ts, attr, value): a copy of the bundle with the named
    field overridden by ``value`` (hgraph's CS field set). */
struct setattr_ts_bundle {
  static constexpr auto name = "setattr_ts_bundle";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    const auto *bundle = getattr_ts_bundle::bundle_meta(context);
    const Str *attr = context.scalar_as<Str>("attr");
    return bundle != nullptr && attr != nullptr &&
           getattr_ts_bundle::field_index(bundle, *attr).has_value();
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (output_bound(resolution)) {
      return;
    }
    bind_output_like_arg(resolution, context, 0);
  }

  static void eval(In<"ts", TS<ScalarVar<"S">>> ts, Scalar<"attr", Str> attr,
                   In<"value", TS<ScalarVar<"V">>> value,
                   Out<TsVar<"__out__">> out) {
    const auto &erased = static_cast<const TSOutputView &>(out);
    const auto source = ts.base().value();
    const auto *meta = source.schema();
    auto fields = source.as_indexed_view();
    BundleBuilder builder{value_type_for_active_realization(meta)};
    const auto target = getattr_ts_bundle::field_index(meta, attr.value());
    for (std::size_t index = 0; index < meta->field_count; ++index) {
      if (target.has_value() && index == *target) {
        if (value.valid()) {
          builder.set(index, Value{value.base().value()});
        }
      } else {
        auto field = fields.at(index);
        if (field.valid()) {
          builder.set(index, Value{field});
        }
      }
    }
    Value result = builder.build();
    if (erased.data_view().has_current_value() &&
        erased.value().equals(result.view())) {
      return;
    }
    auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
    static_cast<void>(mutation.move_value_from(std::move(result)));
  }
};

/** getattr_ over NESTED TSDs: collapse -> project -> uncollapse
    (hgraph's tsd_get_bundle_item_2/_nested route). */
struct getattr_tsd_nested {
  static constexpr auto name = "getattr_tsd_nested";

  [[nodiscard]] static const TSValueTypeMetaData *
  nested_bundle_leaf(const TSValueTypeMetaData *schema, std::size_t &depth) {
    auto &registry = TypeRegistry::instance();
    const auto *current = registry.dereference(schema);
    depth = 0;
    while (const auto *tsd = time_series_schema_as<AnyTSD>(current)) {
      ++depth;
      current = registry.dereference(tsd->element_ts());
    }
    return time_series_schema_as<AnyTSB>(current);
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    if (context.args.size() != 2 ||
        context.args[0].kind != WiringArg::Kind::TimeSeries) {
      return false;
    }
    std::size_t depth = 0;
    const auto *bundle = nested_bundle_leaf(context.args[0].port.schema, depth);
    const Str *attr = context.scalar_as<Str>("attr");
    return depth >= 2 && bundle != nullptr && attr != nullptr &&
           container_impl_detail::find_tsb_field_index(*bundle, *attr)
               .has_value();
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (resolution.find_ts("__out__") != nullptr) {
      return;
    }
    if (context.args.size() != 2) {
      return;
    }
    std::size_t depth = 0;
    const auto *bundle = nested_bundle_leaf(context.args[0].port.schema, depth);
    const Str *attr = context.scalar_as<Str>("attr");
    if (depth < 2 || bundle == nullptr || attr == nullptr) {
      return;
    }
    const auto index =
        container_impl_detail::find_tsb_field_index(*bundle, *attr);
    if (!index.has_value()) {
      return;
    }
    auto &registry = TypeRegistry::instance();
    const auto *field = bundle->fields()[*index].type;
    const TSValueTypeMetaData *out =
        field->kind == TSTypeKind::REF ? field : registry.ref(field);
    const auto *level = registry.dereference(context.args[0].port.schema);
    std::vector<const ValueTypeMetaData *> keys;
    while (const auto *tsd = time_series_schema_as<AnyTSD>(level)) {
      keys.push_back(tsd->key_type());
      level = registry.dereference(tsd->element_ts());
    }
    for (std::size_t i = keys.size(); i-- > 0;) {
      out = registry.tsd(keys[i], out);
    }
    resolution.bind_ts("__out__", out);
  }

  static WiringPortRef compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts,
                               Scalar<"attr", Str> attr) {
    const auto ts_arg = [&](const WiringPortRef &port) {
      WiringArg arg;
      arg.kind = WiringArg::Kind::TimeSeries;
      arg.port = port;
      return arg;
    };
    WiringArg attr_arg;
    attr_arg.kind = WiringArg::Kind::Scalar;
    attr_arg.scalar_value = Value{attr.value()};
    attr_arg.scalar_meta = attr_arg.scalar_value.schema();

    std::array<WiringArg, 1> collapse_args{ts_arg(ts.erased())};
    auto collapsed = wire_operator(
        w, "collapse_keys", {collapse_args.data(), collapse_args.size()}, true);

    std::array<WiringArg, 2> project_args{ts_arg(collapsed.output.erased()),
                                          attr_arg};
    auto projected = wire_operator(
        w, "getattr_", {project_args.data(), project_args.size()}, true);

    std::array<WiringArg, 1> uncollapse_args{ts_arg(projected.output.erased())};
    auto result =
        wire_operator(w, "uncollapse_keys",
                      {uncollapse_args.data(), uncollapse_args.size()}, true);
    return result.output.erased();
  }
};

struct getitem_tsd_by_key {
  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return context.args.size() == 2 &&
           time_series_schema_as<AnyTSD>(context.args[0]) != nullptr;
  }

  static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>,
                      InputActivity::Structural, InputValidity::Unchecked>
                       ts,
                   In<"key", TS<ScalarVar<"K">>> key,
                   State<Int> pending_slot,
                   Out<REF<TsVar<"V">>> out) {
    const auto *schema =
        TypeRegistry::instance().dereference(ts.base().schema());
    const auto *target = schema != nullptr ? schema->element_ts() : nullptr;
    TimeSeriesReference reference{target};
    auto &dict = static_cast<TSDInputView &>(ts);
    const std::size_t slot = dict.find_slot(key.base().value());

    const auto release_pending = [&] {
      const auto encoded = pending_slot.get();
      if (encoded <= 0) {
        return;
      }
      const auto previous = static_cast<std::size_t>(encoded - 1);
      if (dict.slot_occupied(previous)) {
        auto child = dict.at_slot(previous);
        child.make_passive();
      }
      pending_slot.set(Int{0});
    };

    const auto encoded = pending_slot.get();
    if (encoded > 0 && static_cast<std::size_t>(encoded - 1) != slot) {
      release_pending();
    }

    if (slot != TS_DATA_NO_CHILD_ID) {
      auto child = dict.at_slot(slot);
      reference = child.reference();
      if (reference.is_empty()) {
        // An explicit-key map can create its stable slot before the mapped
        // from-REF output has a target. Observe only that selected forwarding
        // position until it binds; structural activity alone observes key-set
        // changes, not this target-link transition.
        auto position = child.bound_output();
        if (position.bound() && position.forwarding()) {
          child.make_active();
          pending_slot.set(static_cast<Int>(slot + 1));
        }
      } else if (pending_slot.get() > 0) {
        child.make_passive();
        pending_slot.set(Int{0});
      }
    }

    if (!out.valid() ||
        !(out.value().checked_as<TimeSeriesReference>() == reference)) {
      out.set(reference);
    }
  }
};

/** getitem_(tsd, keys: TSS[K]) -> TSD[K, REF[V]] (hgraph's
    tsd_get_items): the requested key set's PRESENT entries as
    references; keys absent from the dict contribute nothing until
    they appear. */
struct getitem_tsd_by_keys {
  static constexpr auto name = "getitem_tsd_items";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    if (context.args.size() != 2 ||
        context.args[1].kind != WiringArg::Kind::TimeSeries) {
      return false;
    }
    const auto *tsd = time_series_schema_as<AnyTSD>(context.args[0]);
    const auto *key = time_series_schema_as<AnyTSS>(context.args[1]);
    return tsd != nullptr && key != nullptr && key->value_schema != nullptr &&
           key->value_schema->element_type == tsd->key_type();
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (resolution.find_ts("__out__") != nullptr) {
      return;
    }
    if (context.args.size() != 2) {
      return;
    }
    const auto *tsd = time_series_schema_as<AnyTSD>(context.args[0]);
    if (tsd == nullptr) {
      return;
    }
    auto &registry = TypeRegistry::instance();
    const auto *element = registry.dereference(tsd->element_ts());
    resolution.bind_ts("__out__",
                       registry.tsd(tsd->key_type(), registry.ref(element)));
  }

  static void
  eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
       In<"key", TSS<ScalarVar<"K">>, InputValidity::Unchecked> keys,
       Out<TsVar<"__out__">> out) {
    const auto &erased = static_cast<const TSOutputView &>(out);
    const auto evaluation_time = erased.evaluation_time();
    auto dict_out = erased.data_view().as_dict();
    auto mutation = dict_out.begin_mutation(evaluation_time);

    const TSDInputView &dict = ts;
    const TSSInputView &key_set = keys;

    std::vector<Value> stale;
    for (const auto [key, child] : dict_out.items()) {
      const bool keep = key_set.contains(key) && dict.contains(key);
      if (!keep) {
        stale.emplace_back(key);
      }
    }
    for (const Value &key : stale) {
      (void)mutation.erase(key.view());
    }

    const auto publish_reference = [&](const ValueView &key) {
      const std::size_t slot = dict.find_slot(key);
      if (slot == TS_DATA_NO_CHILD_ID) {
        return;
      }
      Value reference{dict.at_slot(slot).reference()};
      auto element = mutation.at(key);
      if (element.has_current_value() &&
          element.value().checked_as<TimeSeriesReference>() ==
              reference.view().checked_as<TimeSeriesReference>()) {
        return;
      }
      auto element_mutation =
          TSOutputView{erased.output(), element, evaluation_time}
              .begin_mutation(evaluation_time);
      static_cast<void>(element_mutation.move_value_from(std::move(reference)));
    };

    for (const ValueView &key : key_set.added_values()) {
      publish_reference(key);
    }
    for (const ValueView &key : dict.modified_keys()) {
      if (key_set.contains(key)) {
        publish_reference(key);
      }
    }
  }
};

struct index_of_tsl {
  static void eval(
      In<"ts", TSL<TS<ScalarVar<"T">>, SIZE<"N">>, InputValidity::Unchecked> ts,
      In<"item", TS<ScalarVar<"T">>> item, Out<TS<Int>> out) {
    Int index = -1;
    for (std::size_t i = 0; i < ts.size(); ++i) {
      auto child = ts[i];
      if (child.valid() && child.base().value().equals(item.base().value())) {
        index = static_cast<Int>(i);
        break;
      }
    }
    container_impl_detail::set_if_changed(out, index);
  }
};

struct getitem_tsb_by_name {
  static constexpr auto name = "getitem_tsb_by_name";

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    container_impl_detail::resolve_tsb_field_output<Str>(resolution, context,
                                                         "key");
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return container_impl_detail::has_tsb_field<Str>(context, "key");
  }

  static WiringPortRef compose(Wiring &, NamedPort<"ts", TsVar<"S">> ts,
                               Scalar<"key", Str> key) {
    const TSValueTypeMetaData *schema = ts.erased().schema;
    std::size_t index =
        *container_impl_detail::find_tsb_field_index(*schema, key.value());
    return subgraph_wiring_detail::tsb_field_ref(ts.erased(), index,
                                                 schema->fields()[index].type);
  }
};

struct getitem_tsb_by_index {
  static constexpr auto name = "getitem_tsb_by_index";

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    container_impl_detail::resolve_tsb_field_output<Int>(resolution, context,
                                                         "key");
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return container_impl_detail::has_tsb_field<Int>(context, "key");
  }

  static WiringPortRef compose(Wiring &, NamedPort<"ts", TsVar<"S">> ts,
                               Scalar<"key", Int> key) {
    const TSValueTypeMetaData *schema = ts.erased().schema;
    std::size_t index =
        *container_impl_detail::find_tsb_field_index(*schema, key.value());
    return subgraph_wiring_detail::tsb_field_ref(ts.erased(), index,
                                                 schema->fields()[index].type);
  }
};

struct getattr_tsb {
  static constexpr auto name = "getattr_tsb";

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    container_impl_detail::resolve_tsb_field_output<Str>(resolution, context,
                                                         "attr");
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return container_impl_detail::has_tsb_field<Str>(context, "attr");
  }

  static WiringPortRef compose(Wiring &, NamedPort<"ts", TsVar<"S">> ts,
                               Scalar<"attr", Str> attr) {
    const TSValueTypeMetaData *schema = ts.erased().schema;
    std::size_t index =
        *container_impl_detail::find_tsb_field_index(*schema, attr.value());
    return subgraph_wiring_detail::tsb_field_ref(ts.erased(), index,
                                                 schema->fields()[index].type);
  }
};

/** Field projection over REF[TSB]: a runtime node reading the incoming
    reference VALUE and emitting a reference to the FIELD (hgraph's
    _tsb_ref_item). Wiring-time structural projection cannot work here -
    the referenced output only exists at runtime. */
template <typename KeyT, fixed_string KeyName, fixed_string OpName>
struct tsb_ref_field_node {
  static constexpr const char *name = OpName.value;
  // A directly bound REF is already value-valid before graph start. The
  // initial field reference is therefore an explicit startup sample;
  // input activation only establishes the subscription used by later
  // rebinds.
  static constexpr bool schedule_on_start = true;

  [[nodiscard]] static const TSValueTypeMetaData *
  field_schema(OperatorCallContext context) {
    if (context.args.size() != 2) {
      return nullptr;
    }
    const auto *bundle = container_impl_detail::ref_tsb_schema(context.args[0]);
    const KeyT *key = context.scalar_as<KeyT>(KeyName.sv());
    if (bundle == nullptr || key == nullptr) {
      return nullptr;
    }
    return container_impl_detail::tsb_field_schema(*bundle, *key);
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return field_schema(context) != nullptr;
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (output_bound(resolution)) {
      return;
    }
    const auto *field = field_schema(context);
    if (field == nullptr) {
      return;
    }
    bind_output(resolution, TypeRegistry::instance().ref(field));
  }

  static void eval(In<"ts", REF<TsVar<"S">>, InputValidity::Unchecked> ts,
                   Scalar<KeyName, KeyT> key, Out<TsVar<"__out__">> out) {
    const auto &erased = static_cast<const TSOutputView &>(out);
    const auto reference = ts.base().reference();
    const auto *bundle =
        TypeRegistry::instance().dereference(ts.base().schema());
    const auto index =
        container_impl_detail::find_tsb_field_index(*bundle, key.value());
    if (!index.has_value()) {
      return;
    }
    const std::string field_label = [&] {
      if constexpr (std::same_as<KeyT, Str>) {
        return key.value();
      } else {
        return std::to_string(key.value());
      }
    }();
    const std::string_view operation =
        KeyName.sv() == std::string_view{"attr"} ? "getattr_" : "getitem_";
    auto result = container_impl_detail::project_tsb_reference_field(
        reference, *bundle, *index, erased.evaluation_time(), operation,
        field_label);
    container_impl_detail::set_reference_if_changed(erased,
                                                    std::move(result));
  }
};

/** Materialize REF[TSB]/REF[TSL] as the same indexed container shape whose
    children contain references to the corresponding target elements. */
template <TSTypeKind ContainerKind>
struct dereference_indexed_ref_node {
  static_assert(ContainerKind == TSTypeKind::TSB ||
                ContainerKind == TSTypeKind::TSL);

  static constexpr auto name = ContainerKind == TSTypeKind::TSB
                                   ? "dereference_tsb_ref"
                                   : "dereference_tsl_ref";
  static constexpr bool schedule_on_start = true;

  [[nodiscard]] static const TSValueTypeMetaData *
  container_schema(OperatorCallContext context) {
    if (context.args.size() != 1) {
      return nullptr;
    }
    const TSValueTypeMetaData *surface = time_series_schema(
        context.args[0], SchemaRefMode::Direct);
    if (surface == nullptr) {
      return nullptr;
    }

    // A direct structured source binds to this node's REF<S> input through
    // the normal to-REF adaptation. Resolve that direct TSB/TSL boundary the
    // same way as an explicit REF without widening the REF-only helpers used
    // by getitem_/getattr_.
    const TSValueTypeMetaData *target = surface->kind == TSTypeKind::REF
                                            ? TypeRegistry::instance().dereference(surface)
                                            : surface;
    return target != nullptr && target->kind == ContainerKind ? target
                                                               : nullptr;
  }

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return container_schema(context) != nullptr;
  }

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    if (output_bound(resolution)) {
      return;
    }
    const auto *container = container_schema(context);
    if (container != nullptr) {
      bind_output(resolution,
                  container_impl_detail::materialized_indexed_reference_schema(
                      *container));
    }
  }

  static void eval(In<"tsb", REF<TsVar<"S">>, InputValidity::Unchecked> ts,
                   Out<TsVar<"__out__">> out) {
    const auto &erased = static_cast<const TSOutputView &>(out);
    const auto *container =
        TypeRegistry::instance().dereference(ts.base().schema());
    constexpr std::string_view subject =
        ContainerKind == TSTypeKind::TSB ? "REF[TSB]" : "REF[TSL]";
    if (container == nullptr || container->kind != ContainerKind) {
      throw std::logic_error("dereference: input is not " +
                             std::string{subject});
    }
    const auto *expected_output =
        container_impl_detail::materialized_indexed_reference_schema(
            *container);
    if (!time_series_schema_equivalent(expected_output, erased.schema())) {
      throw std::logic_error(
          "dereference: resolved output does not match the referenced " +
          std::string{ContainerKind == TSTypeKind::TSB ? "TSB" : "TSL"} +
          " shape");
    }

    const auto reference = ts.base().reference();
    auto target = container_impl_detail::resolve_indexed_reference_target(
        reference, *container, erased.evaluation_time(), "dereference",
        subject);
    const std::size_t source_count =
        target ? target->data_view().indexed_child_count()
               : reference.is_non_peered() ? reference.items().size() : 0;
    const std::size_t count = [&] {
      if constexpr (ContainerKind == TSTypeKind::TSB) {
        return container->field_count();
      } else {
        const std::size_t fixed_size = container->fixed_size();
        auto output = erased.as_list();
        return fixed_size != 0
                   ? fixed_size
                   : std::max(output.size(), source_count);
      }
    }();

    for (std::size_t index = 0; index < count; ++index) {
      const TSValueTypeMetaData *element_type = [&] {
        if constexpr (ContainerKind == TSTypeKind::TSB) {
          return container->fields()[index].type;
        } else {
          return container->element_ts();
        }
      }();
      const std::string element_label = [&] {
        if constexpr (ContainerKind == TSTypeKind::TSB) {
          const char *field_name = container->fields()[index].name;
          return field_name != nullptr ? std::string{field_name} : std::string{};
        } else {
          return std::to_string(index);
        }
      }();
      auto result = container_impl_detail::project_indexed_reference_element(
          reference, target ? &*target : nullptr, *container, element_type,
          index, "dereference", subject,
          ContainerKind == TSTypeKind::TSB ? "field" : "element",
          element_label);
      if constexpr (ContainerKind == TSTypeKind::TSB) {
        auto output = erased.as_bundle();
        container_impl_detail::set_reference_if_changed(output.at(index),
                                                        std::move(result));
      } else {
        auto output = erased.as_list();
        container_impl_detail::set_reference_if_changed(output.at(index),
                                                        std::move(result));
      }
    }
  }
};

using dereference_tsb_ref_node =
    dereference_indexed_ref_node<TSTypeKind::TSB>;
using dereference_tsl_ref_node =
    dereference_indexed_ref_node<TSTypeKind::TSL>;

struct len_tsb {
  static constexpr auto name = "len_tsb";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return context.args.size() == 1 &&
           container_impl_detail::direct_tsb_schema(context.args[0]) != nullptr;
  }

  static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts) {
    return wire<const_, TS<Int>>(
        w, static_cast<Int>(ts.erased().schema->field_count()));
  }
};

struct is_empty_tsb {
  static constexpr auto name = "is_empty_tsb";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return context.args.size() == 1 &&
           container_impl_detail::direct_tsb_schema(context.args[0]) != nullptr;
  }

  static Port<TS<Bool>> compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts) {
    return wire<const_, TS<Bool>>(w, ts.erased().schema->field_count() == 0);
  }
};

void register_container_operators();
} // namespace hgraph::stdlib

#endif // HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H
