#include <hgraph/runtime/graph_diagnostics.h>

#include <hgraph/runtime/diagnostic_path.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/series.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/visitor.h>

#include <arrow/api.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        using DiagnosticsClock = std::chrono::steady_clock;
        using DiagnosticsTime = DiagnosticsClock::time_point;

        enum class DiagnosticsPhase : std::uint8_t
        {
            Start,
            Evaluation,
            Stop,
        };

        [[nodiscard]] TimeDelta elapsed(DiagnosticsTime start, DiagnosticsTime end) noexcept
        {
            return std::chrono::duration_cast<TimeDelta>(end - start);
        }

        [[nodiscard]] DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
        }

        [[nodiscard]] constexpr std::size_t phase_index(DiagnosticsPhase phase) noexcept
        {
            return static_cast<std::size_t>(phase);
        }

        [[nodiscard]] std::string schema_label(const TypeRecord *record)
        {
            return record == nullptr ? std::string{} : std::string{record->semantic_name()};
        }

        [[nodiscard]] std::string implementation_label(const TypeRecord *record)
        {
            return record == nullptr ? std::string{} : std::string{record->implementation_name()};
        }

        [[nodiscard]] std::string value_schema_label(const ValueView &value)
        {
            const ValueTypeMetaData *schema = value.schema();
            return schema == nullptr ? std::string{} : std::string{schema->name()};
        }

        /**
         * Inspector table snapshots deliberately live in the native
         * diagnostics layer.  The released Python inspector's value endpoint
         * flattens compound scalars, TSBs, TSD partition keys and references
         * into an Arrow table.  Recreating that shape from JSON in the Python
         * presentation loses type information (notably TSD removal/key
         * columns), so GraphDiagnostics owns the typed conversion while the
         * endpoint is still live and retains only the immutable Frame.
         */
        struct DiagnosticTableColumn
        {
            std::string name{};
            const ValueTypeMetaData *meta{nullptr};
        };

        using DiagnosticTableRow = std::vector<Value>;

        [[nodiscard]] ValueView reborrow(const ValueView &value)
        {
            return value.valid() ? ValueView{value.binding(), value.data()}
                                 : ValueView{};
        }

        struct DiagnosticValueTablePlan
        {
            enum class Kind : std::uint8_t
            {
                Leaf,
                Bundle,
                Tuple,
            };

            Kind kind{Kind::Leaf};
            bool stringify{false};
            std::vector<DiagnosticTableColumn> columns{};
            std::vector<DiagnosticValueTablePlan> children{};

            void append(const ValueView &value, DiagnosticTableRow &row) const
            {
                if (kind == Kind::Leaf)
                {
                    if (!value.has_value()) { row.emplace_back(); }
                    else if (stringify)
                    {
                        row.emplace_back(Str{value.to_string()});
                    }
                    else { row.emplace_back(value); }
                    return;
                }

                for (std::size_t index = 0; index < children.size(); ++index)
                {
                    ValueView child;
                    if (value.has_value())
                    {
                        // Assigned per branch rather than through a ternary:
                        // MSVC does not elide the conditional operator's
                        // composite prvalue, so the ternary form needs
                        // ValueView's deleted copy constructor and fails to
                        // compile there. This matches how table_impl.cpp and
                        // the sibling case below already select a child.
                        if (kind == Kind::Bundle) { child = reborrow(value.as_bundle().at(index)); }
                        else { child = reborrow(value.as_tuple().at(index)); }
                    }
                    children[index].append(child, row);
                }
            }
        };

        [[nodiscard]] DiagnosticValueTablePlan diagnostic_value_table_plan(
            const ValueTypeMetaData *meta, std::string prefix = {})
        {
            if (meta == nullptr)
            {
                throw std::invalid_argument(
                    "diagnostic table: value schema is null");
            }

            const bool bundle = meta->value_kind() == ValueTypeKind::Bundle;
            const bool tuple = meta->value_kind() == ValueTypeKind::Tuple &&
                               meta->field_count != 0;
            if (!bundle && !tuple)
            {
                const bool released_arrow_type =
                    meta == TypeRegistry::instance().value_type("bool") ||
                    meta == TypeRegistry::instance().value_type("int") ||
                    meta == TypeRegistry::instance().value_type("float") ||
                    meta == TypeRegistry::instance().value_type("str") ||
                    meta == TypeRegistry::instance().value_type("date") ||
                    meta == TypeRegistry::instance().value_type("datetime") ||
                    meta == TypeRegistry::instance().value_type("timedelta") ||
                    meta == TypeRegistry::instance().value_type("time");
                return DiagnosticValueTablePlan{
                    .kind = DiagnosticValueTablePlan::Kind::Leaf,
                    .stringify = !released_arrow_type,
                    .columns = {{prefix.empty() ? "value" : std::move(prefix),
                                 released_arrow_type
                                     ? meta
                                     : TypeRegistry::instance().value_type("str")}},
                };
            }

            DiagnosticValueTablePlan result{
                .kind = bundle ? DiagnosticValueTablePlan::Kind::Bundle
                               : DiagnosticValueTablePlan::Kind::Tuple,
            };
            result.children.reserve(meta->field_count);
            for (std::size_t index = 0; index < meta->field_count; ++index)
            {
                const auto &field = meta->fields[index];
                const std::string component =
                    bundle && field.name != nullptr ? std::string{field.name}
                                                    : std::to_string(index);
                auto child = diagnostic_value_table_plan(
                    field.type, prefix.empty() ? component
                                               : prefix + "." + component);
                result.columns.insert(result.columns.end(),
                                      child.columns.begin(),
                                      child.columns.end());
                result.children.push_back(std::move(child));
            }
            return result;
        }

        struct DiagnosticTsTablePlan
        {
            enum class Kind : std::uint8_t
            {
                Value,
                Bundle,
                Dict,
                Frame,
            };

            struct Field
            {
                std::string name{};
                std::shared_ptr<const DiagnosticTsTablePlan> plan{};
            };

            Kind kind{Kind::Value};
            const TSValueTypeMetaData *schema{nullptr};
            DiagnosticValueTablePlan value_plan{};
            DiagnosticValueTablePlan key_plan{};
            std::shared_ptr<const DiagnosticTsTablePlan> element{};
            std::vector<Field> fields{};
            std::vector<DiagnosticTableColumn> columns{};
            std::optional<std::size_t> dynamic_field{};

            [[nodiscard]] bool multi_row() const noexcept
            {
                return kind == Kind::Dict || kind == Kind::Frame ||
                       dynamic_field.has_value();
            }

            [[nodiscard]] std::vector<DiagnosticTableRow> render(
                const ValueView &value) const
            {
                switch (kind)
                {
                    case Kind::Value:
                    {
                        DiagnosticTableRow row;
                        row.reserve(columns.size());
                        value_plan.append(value, row);
                        return {std::move(row)};
                    }
                    case Kind::Frame:
                    {
                        if (!value.has_value()) { return {}; }
                        const Frame &frame = value.checked_as<Frame>();
                        if (!frame.has_value()) { return {}; }
                        std::vector<DiagnosticTableRow> rows;
                        rows.reserve(static_cast<std::size_t>(frame_rows(frame)));
                        for (std::int64_t row_index = 0;
                             row_index < frame_rows(frame); ++row_index)
                        {
                            DiagnosticTableRow row;
                            row.reserve(columns.size());
                            for (const auto &column : columns)
                            {
                                row.push_back(frame_cell(
                                    frame, column.name, column.meta, row_index));
                            }
                            rows.push_back(std::move(row));
                        }
                        return rows;
                    }
                    case Kind::Bundle:
                    {
                        std::vector<std::vector<DiagnosticTableRow>> field_rows;
                        field_rows.reserve(fields.size());
                        for (std::size_t index = 0; index < fields.size(); ++index)
                        {
                            ValueView child;
                            if (value.has_value())
                            {
                                child = reborrow(value.as_bundle().at(index));
                            }
                            auto rows = fields[index].plan->render(child);
                            if (rows.empty()) { return {}; }
                            field_rows.push_back(std::move(rows));
                        }

                        const std::size_t row_count = dynamic_field.has_value()
                                                          ? field_rows[*dynamic_field].size()
                                                          : 1;
                        std::vector<DiagnosticTableRow> rows;
                        rows.reserve(row_count);
                        for (std::size_t row_index = 0; row_index < row_count;
                             ++row_index)
                        {
                            DiagnosticTableRow row;
                            row.reserve(columns.size());
                            for (std::size_t field_index = 0;
                                 field_index < field_rows.size(); ++field_index)
                            {
                                const auto &source =
                                    field_rows[field_index][
                                        dynamic_field == field_index ? row_index : 0];
                                for (const Value &cell : source)
                                {
                                    row.push_back(cell.has_value()
                                                      ? Value{cell.view()}
                                                      : Value{});
                                }
                            }
                            rows.push_back(std::move(row));
                        }
                        return rows;
                    }
                    case Kind::Dict:
                    {
                        if (!value.has_value()) { return {}; }
                        std::vector<DiagnosticTableRow> rows;
                        for (const auto [key, child] : value.as_map())
                        {
                            auto child_rows = element->render(child);
                            for (auto &child_row : child_rows)
                            {
                                DiagnosticTableRow row;
                                row.reserve(columns.size());
                                row.emplace_back(Bool{false});
                                key_plan.append(key, row);
                                for (Value &cell : child_row)
                                {
                                    row.push_back(cell.has_value()
                                                      ? Value{cell.view()}
                                                      : Value{});
                                }
                                rows.push_back(std::move(row));
                            }
                        }
                        return rows;
                    }
                }
                std::terminate();
            }
        };

        struct DiagnosticTableProjection
        {
            std::shared_ptr<const DiagnosticTsTablePlan> plan{};
            ValueTypeRef row_binding{nullptr};
            const JsonConverter *json_converter{nullptr};
            const TableConverter *table_converter{nullptr};
        };

        struct DiagnosticTableProjectionResult
        {
            std::shared_ptr<const DiagnosticTableProjection> projection{};
            std::string error{};
        };

        using DiagnosticTableProjectionCache =
            std::unordered_map<const TSValueTypeMetaData *,
                               DiagnosticTableProjectionResult>;

        [[nodiscard]] std::shared_ptr<const DiagnosticTsTablePlan>
        diagnostic_ts_table_plan(const TSValueTypeMetaData *schema,
                                 std::size_t partition_depth = 0)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument(
                    "diagnostic table: time-series schema is null");
            }
            schema = TypeRegistry::instance().dereference(schema);

            if (schema->kind == TSTypeKind::TSD)
            {
                const std::size_t level = partition_depth + 1;
                auto key_plan = diagnostic_value_table_plan(schema->key_type());
                auto element = diagnostic_ts_table_plan(schema->element_ts(), level);
                auto result = std::make_shared<DiagnosticTsTablePlan>();
                result->kind = DiagnosticTsTablePlan::Kind::Dict;
                result->schema = schema;
                result->key_plan = std::move(key_plan);
                result->element = std::move(element);
                result->columns.push_back({
                    "__key_" + std::to_string(level) + "_removed__",
                    TypeRegistry::instance().value_type("bool"),
                });
                for (std::size_t index = 0;
                     index < result->key_plan.columns.size(); ++index)
                {
                    auto column = result->key_plan.columns[index];
                    column.name = result->key_plan.columns.size() == 1 &&
                                          column.name == "value"
                                      ? "__key_" + std::to_string(level) + "__"
                                      : "__key_" + std::to_string(level) + "_" +
                                            column.name + "__";
                    result->columns.push_back(std::move(column));
                }
                result->columns.insert(result->columns.end(),
                                       result->element->columns.begin(),
                                       result->element->columns.end());
                return result;
            }

            if (schema->kind == TSTypeKind::TSB)
            {
                auto result = std::make_shared<DiagnosticTsTablePlan>();
                result->kind = DiagnosticTsTablePlan::Kind::Bundle;
                result->schema = schema;
                result->fields.reserve(schema->field_count());
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    const char *raw_name = schema->fields()[index].name;
                    const std::string name = raw_name == nullptr
                                                 ? std::to_string(index)
                                                 : std::string{raw_name};
                    auto child = diagnostic_ts_table_plan(
                        schema->fields()[index].type, partition_depth);
                    if (child->multi_row())
                    {
                        if (result->dynamic_field.has_value())
                        {
                            throw std::invalid_argument(
                                "diagnostic table: cannot flatten a TSB with "
                                "multiple partitioned or multi-row fields");
                        }
                        result->dynamic_field = index;
                    }
                    for (auto column : child->columns)
                    {
                        column.name = child->columns.size() == 1
                                          ? name
                                          : name + "." + column.name;
                        result->columns.push_back(std::move(column));
                    }
                    result->fields.push_back({name, std::move(child)});
                }
                return result;
            }

            if (schema->kind == TSTypeKind::TS &&
                TypeRegistry::instance().is_frame(schema->value_schema))
            {
                auto result = std::make_shared<DiagnosticTsTablePlan>();
                result->kind = DiagnosticTsTablePlan::Kind::Frame;
                result->schema = schema;
                const auto *row_meta = schema->value_schema->element_type;
                if (row_meta == nullptr)
                {
                    throw std::invalid_argument(
                        "diagnostic table: an untyped nested Frame has no "
                        "declared columns");
                }
                for (const auto &column : table_converter(row_meta).columns)
                {
                    result->columns.push_back(
                        {column.name, column.leaf_meta});
                }
                return result;
            }

            auto result = std::make_shared<DiagnosticTsTablePlan>();
            result->kind = DiagnosticTsTablePlan::Kind::Value;
            result->schema = schema;
            result->value_plan =
                diagnostic_value_table_plan(schema->value_schema);
            result->columns = result->value_plan.columns;
            return result;
        }

        [[nodiscard]] std::shared_ptr<const DiagnosticTableProjection>
        diagnostic_table_projection(const TSValueTypeMetaData *schema)
        {
            schema = TypeRegistry::instance().dereference(schema);
            auto projection = std::make_shared<DiagnosticTableProjection>();
            projection->plan = diagnostic_ts_table_plan(schema);
            projection->json_converter =
                &hgraph::json_converter(schema->value_schema);

            if (projection->plan->kind == DiagnosticTsTablePlan::Kind::Frame)
            {
                return projection;
            }

            std::vector<std::pair<std::string, const ValueTypeMetaData *>>
                row_fields;
            row_fields.reserve(projection->plan->columns.size());
            for (const auto &column : projection->plan->columns)
            {
                row_fields.emplace_back(column.name, column.meta);
            }
            const auto *row_meta =
                TypeRegistry::instance().un_named_bundle(row_fields);
            projection->row_binding =
                ValuePlanFactory::instance().type_for(row_meta);
            if (projection->row_binding == nullptr)
            {
                throw std::logic_error(
                    "diagnostic table: row schema has no value binding");
            }
            projection->table_converter = &hgraph::table_converter(row_meta);
            return projection;
        }

        [[nodiscard]] const DiagnosticTableProjectionResult &
        cached_diagnostic_table_projection(
            DiagnosticTableProjectionCache &cache,
            const TSValueTypeMetaData *schema)
        {
            schema = TypeRegistry::instance().dereference(schema);
            if (const auto found = cache.find(schema); found != cache.end())
            {
                return found->second;
            }

            DiagnosticTableProjectionResult result;
            try
            {
                result.projection = diagnostic_table_projection(schema);
            }
            catch (const std::exception &error)
            {
                result.error = error.what();
            }
            catch (...)
            {
                result.error = "unknown conversion-plan failure";
            }
            return cache.emplace(schema, std::move(result)).first->second;
        }

        [[nodiscard]] Frame diagnostic_table_frame(
            const DiagnosticTableProjection &projection,
            const ValueView &value)
        {
            const DiagnosticTsTablePlan &plan = *projection.plan;
            if (plan.kind == DiagnosticTsTablePlan::Kind::Frame &&
                value.has_value())
            {
                return value.checked_as<Frame>();
            }

            const auto rows = plan.render(value);
            if (projection.row_binding == nullptr ||
                projection.table_converter == nullptr)
            {
                throw std::logic_error(
                    "diagnostic table: projection is incomplete");
            }

            std::vector<Value> row_values;
            row_values.reserve(rows.size());
            for (const auto &cells : rows)
            {
                Value row{projection.row_binding};
                auto root = row.view();
                auto mutation = root.as_bundle().begin_mutation();
                for (std::size_t index = 0; index < cells.size(); ++index)
                {
                    if (cells[index].has_value())
                    {
                        mutation.at(index).copy_from(cells[index].view());
                    }
                }
                row_values.push_back(std::move(row));
            }

            return frame_from_values(*projection.table_converter, row_values);
        }

        using TargetResolver = std::uint64_t (*)(void *, const TSOutputView &);

        void append_json_string(std::string &target, std::string_view value);

        [[nodiscard]] std::uint64_t resolve_target_node(
            void *context, const TSOutputView &output)
        {
            auto &entities = *static_cast<
                std::unordered_map<const void *, std::uint64_t> *>(context);
            const NodeView owner = output.owner_node();
            if (!owner.valid()) { return 0; }
            const auto found = entities.find(owner.data());
            return found == entities.end() ? 0 : found->second;
        }

        [[nodiscard]] std::vector<std::string> diagnostic_output_path(
            const TSOutputView &output)
        {
            std::vector<std::string> result;
            const NodeView owner = output.owner_node();
            if (!owner.valid()) { return result; }

            const std::vector<std::size_t> path =
                output.data_view().path_from_root();
            TSOutputView current = owner.output(output.evaluation_time());
            result.reserve(path.size());
            for (const std::size_t child_id : path)
            {
                const auto *schema =
                    TypeRegistry::instance().dereference(current.schema());
                if (schema == nullptr)
                {
                    result.push_back(std::to_string(child_id));
                    break;
                }
                if (schema->kind == TSTypeKind::TSB)
                {
                    if (child_id >= schema->field_count()) { break; }
                    std::string component;
                    const char *name = schema->fields()[child_id].name;
                    append_json_string(component, name == nullptr ? "" : name);
                    result.push_back(std::move(component));
                    auto bundle = current.as_bundle();
                    current = bundle.at(child_id);
                }
                else if (schema->kind == TSTypeKind::TSL)
                {
                    result.push_back(std::to_string(child_id));
                    auto list = current.as_list();
                    current = list.at(child_id);
                }
                else if (schema->kind == TSTypeKind::TSD)
                {
                    auto dictionary = current.as_dict();
                    if (child_id >= dictionary.slot_capacity() ||
                        !dictionary.slot_occupied(child_id))
                    {
                        break;
                    }
                    result.push_back(
                        to_json_string(dictionary.key_at_slot(child_id)));
                    current = dictionary.at_slot(child_id);
                }
                else
                {
                    // Preserve a resolvable best-effort path for fixed indexed
                    // endpoint shapes not represented by TSB/TSL/TSD.
                    result.push_back(std::to_string(child_id));
                    current = current.indexed_child_at(child_id);
                }
            }
            return result;
        }

        void record_target(GraphDiagnosticValue *capture,
                           TargetResolver resolver, void *resolver_context,
                           const TSOutputView &output,
                           const std::vector<std::string> &source_path)
        {
            if (capture == nullptr || resolver == nullptr) { return; }
            const std::uint64_t id = resolver(resolver_context, output);
            if (id == 0) { return; }
            if (std::ranges::find(capture->target_node_ids, id) ==
                capture->target_node_ids.end())
            {
                capture->target_node_ids.push_back(id);
            }
            GraphDiagnosticTarget target{
                .source_path = source_path,
                .node_id = id,
                .target_path = diagnostic_output_path(output),
            };
            const auto duplicate = std::ranges::find_if(
                capture->targets,
                [&](const GraphDiagnosticTarget &candidate) {
                    return candidate.source_path == target.source_path &&
                           candidate.node_id == target.node_id &&
                           candidate.target_path == target.target_path;
                });
            if (duplicate == capture->targets.end())
            {
                capture->targets.push_back(std::move(target));
            }
        }

        void record_target_tree(GraphDiagnosticValue *capture,
                                TargetResolver resolver,
                                void *resolver_context,
                                const TSOutputView &output,
                                const std::vector<std::string> &source_path,
                                std::size_t depth = 0)
        {
            record_target(capture, resolver, resolver_context, output,
                          source_path);
            if (depth >= 32) { return; }
            const auto *schema =
                TypeRegistry::instance().dereference(output.schema());
            if (schema == nullptr) { return; }

            if (schema->kind == TSTypeKind::TSB)
            {
                auto bundle = output.as_bundle();
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    auto child_path = source_path;
                    std::string component;
                    const char *name = schema->fields()[index].name;
                    append_json_string(component, name == nullptr ? "" : name);
                    child_path.push_back(std::move(component));
                    record_target_tree(capture, resolver, resolver_context,
                                       bundle.at(index), child_path,
                                       depth + 1);
                }
            }
            else if (schema->kind == TSTypeKind::TSL)
            {
                auto list = output.as_list();
                for (std::size_t index = 0; index < list.size(); ++index)
                {
                    auto child_path = source_path;
                    child_path.push_back(std::to_string(index));
                    record_target_tree(capture, resolver, resolver_context,
                                       list.at(index), child_path,
                                       depth + 1);
                }
            }
            else if (schema->kind == TSTypeKind::TSD)
            {
                auto dictionary = output.as_dict();
                for (std::size_t slot = 0; slot < dictionary.slot_capacity();
                     ++slot)
                {
                    if (!dictionary.slot_occupied(slot)) { continue; }
                    const std::string key_text =
                        to_json_string(dictionary.key_at_slot(slot));
                    auto child_path = source_path;
                    if (!key_text.empty() && key_text.front() == '"')
                    {
                        child_path.push_back(key_text);
                    }
                    else
                    {
                        std::string component;
                        append_json_string(component, key_text);
                        child_path.push_back(std::move(component));
                    }
                    record_target_tree(capture, resolver, resolver_context,
                                       dictionary.at_slot(slot), child_path,
                                       depth + 1);
                }
            }
        }

        void capture_bound_targets(
            std::vector<GraphDiagnosticTarget> &targets,
            TargetResolver resolver,
            void *resolver_context,
            const TSInputView &input,
            const std::vector<std::string> &source_path = {},
            std::size_t depth = 0)
        {
            if (resolver == nullptr || depth >= 32) { return; }
            if (input.is_bindable())
            {
                const TSOutputView output = input.bound_output();
                if (!output.bound()) { return; }
                GraphDiagnosticValue capture;
                capture.targets = std::move(targets);
                record_target_tree(&capture, resolver, resolver_context,
                                   output, source_path);
                targets = std::move(capture.targets);
                return;
            }

            const auto *schema =
                TypeRegistry::instance().dereference(input.schema());
            if (schema == nullptr) { return; }
            if (schema->kind == TSTypeKind::TSB)
            {
                auto bundle = input.as_bundle();
                for (std::size_t index = 0; index < bundle.size(); ++index)
                {
                    auto child_path = source_path;
                    std::string component;
                    const char *name = schema->fields()[index].name;
                    append_json_string(component, name == nullptr ? "" : name);
                    child_path.push_back(std::move(component));
                    capture_bound_targets(
                        targets, resolver, resolver_context, bundle.at(index),
                        child_path, depth + 1);
                }
            }
            else if (schema->kind == TSTypeKind::TSL)
            {
                auto list = input.as_list();
                for (std::size_t index = 0; index < list.size(); ++index)
                {
                    auto child_path = source_path;
                    child_path.push_back(std::to_string(index));
                    capture_bound_targets(
                        targets, resolver, resolver_context, list.at(index),
                        child_path, depth + 1);
                }
            }
            else if (schema->kind == TSTypeKind::TSD)
            {
                auto dictionary = input.as_dict();
                for (std::size_t slot = 0; slot < dictionary.slot_capacity();
                     ++slot)
                {
                    if (!dictionary.slot_occupied(slot)) { continue; }
                    auto child_path = source_path;
                    const std::string key_text =
                        to_json_string(dictionary.key_at_slot(slot));
                    if (!key_text.empty() && key_text.front() == '"')
                    {
                        child_path.push_back(key_text);
                    }
                    else
                    {
                        std::string component;
                        append_json_string(component, key_text);
                        child_path.push_back(std::move(component));
                    }
                    capture_bound_targets(
                        targets, resolver, resolver_context,
                        dictionary.at_slot(slot), child_path, depth + 1);
                }
            }
        }

        void append_json_string(std::string &target, std::string_view value)
        {
            constexpr char hex[] = "0123456789abcdef";
            target.push_back('"');
            for (const unsigned char character : value)
            {
                switch (character)
                {
                    case '"': target += "\\\""; break;
                    case '\\': target += "\\\\"; break;
                    case '\b': target += "\\b"; break;
                    case '\f': target += "\\f"; break;
                    case '\n': target += "\\n"; break;
                    case '\r': target += "\\r"; break;
                    case '\t': target += "\\t"; break;
                    default:
                        if (character < 0x20)
                        {
                            target += "\\u00";
                            target.push_back(hex[character >> 4U]);
                            target.push_back(hex[character & 0x0fU]);
                        }
                        else { target.push_back(static_cast<char>(character)); }
                }
            }
            target.push_back('"');
        }

        void append_diagnostic_json(std::string &target,
                                    const ValueView &value,
                                    DateTime evaluation_time,
                                    std::size_t depth,
                                    const std::vector<std::string> &source_path,
                                    GraphDiagnosticValue *capture,
                                    TargetResolver resolver,
                                    void *resolver_context);

        void append_reference_json(std::string &target,
                                   const TimeSeriesReference &reference,
                                   DateTime evaluation_time,
                                   std::size_t depth,
                                   const std::vector<std::string> &source_path,
                                   GraphDiagnosticValue *capture,
                                   TargetResolver resolver,
                                   void *resolver_context)
        {
            if (depth >= 32)
            {
                throw std::logic_error("reference rendering exceeded 32 levels");
            }
            if (reference.is_empty())
            {
                target += "null";
                return;
            }
            if (reference.is_peered())
            {
                TSOutputView output = reference.target_output().view(evaluation_time);
                record_target_tree(capture, resolver, resolver_context, output,
                                   source_path);
                if (output.valid())
                {
                    const ValueView value = output.value();
                    append_diagnostic_json(target, value, evaluation_time,
                                           depth + 1, source_path, capture, resolver,
                                           resolver_context);
                }
                else { target += "null"; }
                return;
            }

            const auto &items = reference.items();
            const TSValueTypeMetaData *schema = reference.target_schema();
            const bool named = schema != nullptr &&
                               schema->kind == TSTypeKind::TSB &&
                               schema->data.tsb.field_count == items.size();
            target.push_back(named ? '{' : '[');
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                if (index != 0) { target.push_back(','); }
                if (named)
                {
                    const char *name = schema->data.tsb.fields[index].name;
                    append_json_string(target, name == nullptr ? "" : name);
                    target.push_back(':');
                }
                auto child_path = source_path;
                if (named)
                {
                    const char *name = schema->data.tsb.fields[index].name;
                    std::string component;
                    append_json_string(component, name == nullptr ? "" : name);
                    child_path.push_back(std::move(component));
                }
                else { child_path.push_back(std::to_string(index)); }
                append_reference_json(target, items[index], evaluation_time,
                                      depth + 1, child_path, capture, resolver,
                                      resolver_context);
            }
            target.push_back(named ? '}' : ']');
        }

        template <typename IndexedView>
        void append_sequence_json(std::string &target,
                                  const IndexedView &view,
                                  DateTime evaluation_time,
                                  std::size_t depth,
                                  const std::vector<std::string> &source_path,
                                  GraphDiagnosticValue *capture,
                                  TargetResolver resolver,
                                  void *resolver_context)
        {
            target.push_back('[');
            for (std::size_t index = 0; index < view.size(); ++index)
            {
                if (index != 0) { target.push_back(','); }
                const ValueView child = view.at(index);
                auto child_path = source_path;
                child_path.push_back(std::to_string(index));
                append_diagnostic_json(target, child, evaluation_time,
                                       depth + 1, child_path, capture, resolver,
                                       resolver_context);
            }
            target.push_back(']');
        }

        void append_diagnostic_json(std::string &target,
                                    const ValueView &value,
                                    DateTime evaluation_time,
                                    std::size_t depth,
                                    const std::vector<std::string> &source_path,
                                    GraphDiagnosticValue *capture,
                                    TargetResolver resolver,
                                    void *resolver_context)
        {
            if (!value.valid())
            {
                target += "null";
                return;
            }
            if (depth >= 32)
            {
                throw std::logic_error("value rendering exceeded 32 levels");
            }

            const ValueView concrete = value.concrete();
            if (!concrete.valid())
            {
                target += "null";
                return;
            }
            visit(
                concrete,
                [&](AtomicView atomic) {
                    if (atomic.holds_alternative<TimeSeriesReference>())
                    {
                        append_reference_json(
                            target,
                            atomic.checked_as<TimeSeriesReference>(),
                            evaluation_time, depth + 1, source_path, capture, resolver,
                            resolver_context);
                    }
                    else if (atomic.holds_alternative<Frame>())
                    {
                        std::ostringstream rendered;
                        rendered << atomic.checked_as<Frame>();
                        append_json_string(target, rendered.str());
                    }
                    else if (atomic.holds_alternative<Series>())
                    {
                        std::ostringstream rendered;
                        rendered << atomic.checked_as<Series>();
                        append_json_string(target, rendered.str());
                    }
                    else { target += to_json_string(atomic); }
                },
                [&](TupleView tuple) {
                    append_sequence_json(target, tuple, evaluation_time,
                                         depth, source_path, capture, resolver,
                                         resolver_context);
                },
                [&](BundleView bundle) {
                    const bool named = bundle.schema()->is_named_bundle();
                    target.push_back(named ? '{' : '[');
                    bool first = true;
                    for (std::size_t index = 0; index < bundle.size(); ++index)
                    {
                        const ValueView child = bundle.at(index);
                        if (!std::exchange(first, false)) { target.push_back(','); }
                        if (named)
                        {
                            const char *name = bundle.schema()->fields[index].name;
                            append_json_string(target, name == nullptr ? "" : name);
                            target.push_back(':');
                        }
                        auto child_path = source_path;
                        if (named)
                        {
                            const char *name = bundle.schema()->fields[index].name;
                            std::string component;
                            append_json_string(component,
                                               name == nullptr ? "" : name);
                            child_path.push_back(std::move(component));
                        }
                        else { child_path.push_back(std::to_string(index)); }
                        append_diagnostic_json(target, child, evaluation_time,
                                               depth + 1, child_path, capture, resolver,
                                               resolver_context);
                    }
                    target.push_back(named ? '}' : ']');
                },
                [&](ListView list) {
                    append_sequence_json(target, list, evaluation_time,
                                         depth, source_path, capture, resolver,
                                         resolver_context);
                },
                [&](SetView set) {
                    target.push_back('[');
                    bool first = true;
                    for (const ValueView element : set)
                    {
                        if (!std::exchange(first, false)) { target.push_back(','); }
                        auto child_path = source_path;
                        child_path.push_back(to_json_string(element));
                        append_diagnostic_json(target, element,
                                               evaluation_time, depth + 1,
                                               child_path, capture, resolver,
                                               resolver_context);
                    }
                    target.push_back(']');
                },
                [&](MapView map) {
                    target.push_back('{');
                    bool first = true;
                    for (const auto [key, child] : map)
                    {
                        if (!std::exchange(first, false)) { target.push_back(','); }
                        const std::string key_text = to_json_string(key);
                        if (!key_text.empty() && key_text.front() == '"')
                        {
                            target += key_text;
                        }
                        else { append_json_string(target, key_text); }
                        target.push_back(':');
                        auto child_path = source_path;
                        if (!key_text.empty() && key_text.front() == '"')
                        {
                            child_path.push_back(key_text);
                        }
                        else
                        {
                            std::string component;
                            append_json_string(component, key_text);
                            child_path.push_back(std::move(component));
                        }
                        append_diagnostic_json(target, child,
                                               evaluation_time, depth + 1,
                                               child_path, capture, resolver,
                                               resolver_context);
                    }
                    target.push_back('}');
                },
                [&](CyclicBufferView buffer) {
                    append_sequence_json(target, buffer, evaluation_time,
                                         depth, source_path, capture, resolver,
                                         resolver_context);
                },
                [&](QueueView queue) {
                    append_sequence_json(target, queue, evaluation_time,
                                         depth, source_path, capture, resolver,
                                         resolver_context);
                });
        }

        [[nodiscard]] std::string diagnostic_json(const ValueView &value,
                                                   DateTime evaluation_time,
                                                   GraphDiagnosticValue *capture,
                                                   TargetResolver resolver,
                                                   void *resolver_context)
        {
            std::string target;
            const std::vector<std::string> source_path;
            append_diagnostic_json(target, value, evaluation_time, 0,
                                   source_path, capture, resolver,
                                   resolver_context);
            return target;
        }

        void capture_reference_targets(GraphDiagnosticValue &target,
                                       const TimeSeriesReference &reference,
                                       DateTime evaluation_time,
                                       TargetResolver resolver,
                                       void *resolver_context,
                                       std::size_t depth = 0,
                                       const std::vector<std::string> &source_path = {})
        {
            if (depth >= 32 || reference.is_empty()) { return; }
            if (reference.is_peered())
            {
                TSOutputView output = reference.target_output().view(evaluation_time);
                record_target_tree(&target, resolver, resolver_context, output,
                                   source_path);
                return;
            }
            const auto &items = reference.items();
            const TSValueTypeMetaData *schema = reference.target_schema();
            const bool named = schema != nullptr &&
                               schema->kind == TSTypeKind::TSB &&
                               schema->data.tsb.field_count == items.size();
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                auto child_path = source_path;
                if (named)
                {
                    const char *name = schema->data.tsb.fields[index].name;
                    std::string component;
                    append_json_string(component, name == nullptr ? "" : name);
                    child_path.push_back(std::move(component));
                }
                else { child_path.push_back(std::to_string(index)); }
                capture_reference_targets(target, items[index], evaluation_time,
                                          resolver, resolver_context,
                                          depth + 1, child_path);
            }
        }

        void capture_value(GraphDiagnosticValue &target, ValueView value,
                           bool valid, DateTime last_modified,
                           DateTime evaluation_time,
                           const TSValueTypeMetaData *ts_schema,
                           DiagnosticTableProjectionCache &table_projections,
                           TargetResolver resolver,
                           void *resolver_context)
        {
            target.valid = valid;
            target.last_modified = last_modified;
            target.error.clear();
            target.table_error.clear();
            target.frame = {};
            if (const std::string label = value_schema_label(value); !label.empty())
            {
                target.schema_label = label;
            }
            try
            {
                ValueView concrete = value.concrete();
                if (concrete.valid() && concrete.holds_alternative<Frame>())
                {
                    target.frame = concrete.checked_as<Frame>();
                }
                target.json = diagnostic_json(value, evaluation_time, &target,
                                              resolver, resolver_context);
                if (!target.frame.has_value() && ts_schema != nullptr)
                {
                    try
                    {
                        const auto &projection_result =
                            cached_diagnostic_table_projection(
                                table_projections, ts_schema);
                        if (projection_result.projection == nullptr)
                        {
                            throw std::logic_error(projection_result.error);
                        }
                        const auto &projection = *projection_result.projection;
                        Value materialized;
                        if (!target.json.empty() && target.json != "null")
                        {
                            materialized = from_json_string(
                                *projection.json_converter, target.json);
                        }
                        target.frame = diagnostic_table_frame(
                            projection, materialized.view());
                    }
                    catch (const std::exception &error)
                    {
                        // JSON remains the universally available owned view.
                        // A schema that has no released tabular form simply
                        // omits the optional Frame instead of making the row
                        // itself unavailable.
                        target.frame = {};
                        target.table_error = error.what();
                    }
                    catch (...)
                    {
                        target.frame = {};
                        target.table_error = "unknown conversion failure";
                    }
                }
            }
            catch (const std::exception &error)
            {
                target.json.clear();
                target.error = error.what();
            }
            catch (...)
            {
                target.json.clear();
                target.error = "value rendering failed";
            }
        }

        [[nodiscard]] std::vector<std::string> python_input_names(
            const NodeView &node)
        {
            std::vector<std::string> result;
            auto scalars = node.scalars().try_as_bundle();
            if (!scalars.has_value() || !scalars->has_field("config"))
            {
                return result;
            }
            const auto *config = scalars->at("config").try_as<Str>();
            if (config == nullptr) { return result; }
            const auto separator = config->find(';');
            if (separator == std::string::npos) { return result; }
            std::string_view names{*config};
            names.remove_prefix(separator + 1);
            while (!names.empty())
            {
                const auto comma = names.find(',');
                result.emplace_back(names.substr(0, comma));
                if (comma == std::string_view::npos) { break; }
                names.remove_prefix(comma + 1);
            }
            return result;
        }

        [[nodiscard]] bool capture_python_input(
            GraphDiagnosticValue &target,
            const TSInputView &input,
            const std::vector<std::string> &names,
            DateTime evaluation_time,
            DiagnosticTableProjectionCache &table_projections,
            TargetResolver resolver,
            void *resolver_context)
        {
            auto root = input.as_bundle();
            if (!root.has_field("args")) { return false; }
            TSInputView packed = root.at("args");
            auto args = packed.as_bundle();
            if (args.empty() && names.empty())
            {
                target.available = false;
                target.valid = false;
                target.json.clear();
                target.error.clear();
                target.table_error.clear();
                target.frame = {};
                return true;
            }
            if (names.empty()) { return false; }
            if (args.size() != names.size()) { return false; }

            target.valid = packed.valid();
            target.last_modified = packed.last_modified_time();
            target.error.clear();
            target.table_error.clear();
            target.frame = {};
            target.schema_label = std::string{packed.schema()->name()};
            target.json.clear();
            target.json.push_back('{');
            std::string internal_json{"{"};
            for (std::size_t index = 0; index < names.size(); ++index)
            {
                if (index != 0)
                {
                    target.json.push_back(',');
                    internal_json.push_back(',');
                }
                append_json_string(target.json, names[index]);
                target.json.push_back(':');
                const char *internal_name =
                    packed.schema()->fields()[index].name;
                append_json_string(
                    internal_json,
                    internal_name == nullptr ? "" : internal_name);
                internal_json.push_back(':');
                TSInputView child = args.at(index);
                std::vector<std::string> source_path;
                std::string path_component;
                append_json_string(path_component, names[index]);
                source_path.push_back(std::move(path_component));
                capture_bound_targets(
                    target.bound_targets, resolver, resolver_context, child,
                    source_path);
                capture_reference_targets(
                    target, child.reference(), evaluation_time, resolver,
                    resolver_context, 0, source_path);
                std::string child_json;
                append_diagnostic_json(
                    child_json, child.value(), evaluation_time, 0,
                    source_path, nullptr, nullptr, nullptr);
                target.json += child_json;
                internal_json += child_json;
            }
            target.json.push_back('}');
            internal_json.push_back('}');

            try
            {
                const auto &projection_result =
                    cached_diagnostic_table_projection(
                        table_projections, packed.schema());
                if (projection_result.projection == nullptr)
                {
                    throw std::logic_error(projection_result.error);
                }
                const auto &projection = *projection_result.projection;
                Value materialized = from_json_string(
                    *projection.json_converter, internal_json);
                target.frame = diagnostic_table_frame(
                    projection, materialized.view());
            }
            catch (const std::exception &error)
            {
                target.frame = {};
                target.table_error = error.what();
            }
            catch (...)
            {
                target.frame = {};
                target.table_error = "unknown conversion failure";
            }
            if (!target.frame.has_value()) { return true; }

            std::vector<std::string> columns =
                target.frame.table->ColumnNames();
            for (std::string &column : columns)
            {
                for (std::size_t index = 0; index < names.size(); ++index)
                {
                    const std::string internal = "_" + std::to_string(index);
                    if (column == internal)
                    {
                        column = names[index];
                        break;
                    }
                    const std::string prefix = internal + ".";
                    if (column.starts_with(prefix))
                    {
                        column = names[index] + column.substr(internal.size());
                        break;
                    }
                }
            }
            auto renamed = target.frame.table->RenameColumns(columns);
            if (renamed.ok())
            {
                target.frame = Frame{*renamed};
            }
            else
            {
                target.frame = {};
                target.table_error = renamed.status().ToString();
            }
            return true;
        }

        void capture_node_values(GraphDiagnosticEntry &entry,
                                 const NodeView &node,
                                 DiagnosticTableProjectionCache &table_projections,
                                 TargetResolver resolver,
                                 void *resolver_context)
        {
            const DateTime evaluation_time = node.graph().evaluation_time();
            if (entry.input.available)
            {
                try
                {
                    TSInputView input = node.input(evaluation_time);
                    entry.input.target_node_ids.clear();
                    entry.input.targets.clear();
                    entry.input.bound_targets.clear();
                    const bool projected =
                        entry.implementation_label.starts_with("hgraph.python.") &&
                        capture_python_input(
                            entry.input, input, python_input_names(node),
                            evaluation_time, table_projections, resolver,
                            resolver_context);
                    if (!projected)
                    {
                        capture_bound_targets(
                            entry.input.bound_targets, resolver,
                            resolver_context, input);
                        capture_reference_targets(
                            entry.input, input.reference(), evaluation_time,
                            resolver, resolver_context);
                        capture_value(
                            entry.input, input.value(), input.valid(),
                            input.last_modified_time(), evaluation_time,
                            input.schema(), table_projections, nullptr, nullptr);
                    }
                }
                catch (const std::exception &error)
                {
                    entry.input.error = error.what();
                }
                catch (...)
                {
                    entry.input.error = "input rendering failed";
                }
            }
            if (entry.output.available)
            {
                try
                {
                    TSOutputView output = node.output(evaluation_time);
                    entry.output.target_node_ids.clear();
                    entry.output.targets.clear();
                    entry.output.bound_targets.clear();
                    capture_value(entry.output, output.value(), output.valid(),
                                  output.last_modified_time(), evaluation_time,
                                  output.schema(),
                                  table_projections,
                                  resolver, resolver_context);
                }
                catch (const std::exception &error)
                {
                    entry.output.error = error.what();
                }
                catch (...)
                {
                    entry.output.error = "output rendering failed";
                }
            }
            if (entry.scalars.available)
            {
                try
                {
                    ValueView scalars = node.scalars();
                    entry.scalars.target_node_ids.clear();
                    entry.scalars.targets.clear();
                    entry.scalars.bound_targets.clear();
                    // Python node representations carry bridge callables and
                    // lifecycle configuration alongside the user-authored
                    // scalar arguments.  Released inspector SCALARS exposes
                    // only that nested user scalar bundle; bridge machinery
                    // is neither user API nor JSON-renderable graph data.
                    if (entry.implementation_label.starts_with("hgraph.python."))
                    {
                        auto bundle = scalars.try_as_bundle();
                        if (bundle.has_value() && bundle->has_field("scalars"))
                        {
                            ValueView user_scalars = bundle->at("scalars");
                            if (auto user_bundle = user_scalars.try_as_bundle();
                                user_bundle.has_value() && user_bundle->size() == 0)
                            {
                                entry.scalars.available = false;
                                entry.scalars.valid = false;
                                entry.scalars.json.clear();
                                entry.scalars.error.clear();
                                entry.scalars.table_error.clear();
                                entry.scalars.frame = {};
                                return;
                            }
                            const auto *scalar_schema = user_scalars.schema();
                            capture_value(
                                entry.scalars, std::move(user_scalars),
                                scalar_schema != nullptr, MIN_DT, evaluation_time,
                                scalar_schema != nullptr
                                    ? TypeRegistry::instance().ts(scalar_schema)
                                    : nullptr,
                                table_projections, resolver, resolver_context);
                            return;
                        }
                    }

                    const bool valid = scalars.valid();
                    const auto *scalar_schema = scalars.schema();
                    capture_value(
                        entry.scalars, std::move(scalars), valid, MIN_DT,
                        evaluation_time,
                        scalar_schema != nullptr
                            ? TypeRegistry::instance().ts(scalar_schema)
                            : nullptr,
                        table_projections, resolver, resolver_context);
                }
                catch (const std::exception &error)
                {
                    entry.scalars.error = error.what();
                }
                catch (...)
                {
                    entry.scalars.error = "scalar rendering failed";
                }
            }
        }

        void max_storage(NodeStorageMetrics &target, const NodeStorageMetrics &value) noexcept
        {
            target.static_bytes = std::max(target.static_bytes, value.static_bytes);
            target.nested_graph_count = std::max(target.nested_graph_count, value.nested_graph_count);
            target.nested_graph_capacity = std::max(target.nested_graph_capacity, value.nested_graph_capacity);
            target.nested_graph_blocks = std::max(target.nested_graph_blocks, value.nested_graph_blocks);
            target.dynamic_live_bytes = std::max(target.dynamic_live_bytes, value.dynamic_live_bytes);
            target.dynamic_reserved_bytes = std::max(target.dynamic_reserved_bytes, value.dynamic_reserved_bytes);
        }
    }  // namespace

    struct GraphDiagnostics::State
    {
        struct PhaseState
        {
            EvaluationProfilePhase snapshot{};
            std::vector<TimeDelta> recent{};
            std::size_t recent_cursor{0};
        };

        struct EntryState
        {
            GraphDiagnosticEntry snapshot{};
            PhaseState start{};
            PhaseState evaluation{};
            PhaseState stop{};
            std::array<std::optional<DiagnosticsTime>, 3> active{};
        };

        mutable std::mutex mutex{};
        std::map<std::uint64_t, EntryState> entries{};
        std::unordered_map<const void *, std::uint64_t> graph_entities{};
        std::unordered_map<const void *, std::uint64_t> node_entities{};
        DiagnosticTableProjectionCache table_projections{};
        std::uint64_t next_id{1};
        std::optional<DiagnosticsTime> wall_started{};
        std::optional<DiagnosticsTime> root_evaluation_started{};
        TimeDelta wall_time{0};
        std::uint64_t graph_cycles{0};
        TimeDelta root_evaluation_time{0};
        TimeDelta scheduling_lag_total{0};
        TimeDelta scheduling_lag_max{0};
        std::uint64_t scheduling_lag_samples{0};
        std::size_t planned_bytes{0};
        std::size_t dynamic_live_bytes{0};
        std::size_t dynamic_reserved_bytes{0};
        std::size_t peak_dynamic_live_bytes{0};
        std::size_t peak_dynamic_reserved_bytes{0};
    };

    namespace
    {
        [[nodiscard]] GraphDiagnostics::State::PhaseState &phase_state(
            GraphDiagnostics::State::EntryState &entry, DiagnosticsPhase phase) noexcept
        {
            switch (phase)
            {
                case DiagnosticsPhase::Start: return entry.start;
                case DiagnosticsPhase::Evaluation: return entry.evaluation;
                case DiagnosticsPhase::Stop: return entry.stop;
            }
            std::terminate();
        }

        void record_duration(GraphDiagnostics::State::PhaseState &phase,
                             TimeDelta duration, bool failed,
                             std::size_t recent_window)
        {
            duration = std::max(duration, TimeDelta{0});
            ++phase.snapshot.count;
            if (failed) { ++phase.snapshot.failures; }
            phase.snapshot.total_time += duration;
            phase.snapshot.max_time = std::max(phase.snapshot.max_time, duration);
            if (recent_window == 0)
            {
                phase.recent.clear();
                phase.recent_cursor = 0;
                phase.snapshot.recent_time = TimeDelta{0};
                return;
            }
            if (phase.recent.capacity() < recent_window) { phase.recent.reserve(recent_window); }
            if (phase.recent.size() < recent_window)
            {
                phase.recent.push_back(duration);
                phase.snapshot.recent_time += duration;
                return;
            }
            phase.snapshot.recent_time -= phase.recent[phase.recent_cursor];
            phase.recent[phase.recent_cursor] = duration;
            phase.snapshot.recent_time += duration;
            phase.recent_cursor = (phase.recent_cursor + 1) % recent_window;
        }

        void begin_phase(GraphDiagnostics::State::EntryState &entry,
                         DiagnosticsPhase phase) noexcept
        {
            entry.active[phase_index(phase)] = DiagnosticsClock::now();
        }

        void end_phase(GraphDiagnostics::State::EntryState &entry,
                       DiagnosticsPhase phase, bool failed,
                       std::size_t recent_window)
        {
            auto &started = entry.active[phase_index(phase)];
            if (!started.has_value()) { return; }
            record_duration(phase_state(entry, phase),
                            elapsed(*started, DiagnosticsClock::now()),
                            failed, recent_window);
            started.reset();
        }

        [[nodiscard]] GraphDiagnostics::State::EntryState *find_entry(
            GraphDiagnostics::State &state,
            const std::unordered_map<const void *, std::uint64_t> &entities,
            const void *identity) noexcept
        {
            const auto entity = entities.find(identity);
            if (entity == entities.end()) { return nullptr; }
            const auto entry = state.entries.find(entity->second);
            return entry == state.entries.end() ? nullptr : &entry->second;
        }

        [[nodiscard]] std::uint64_t parent_id_for_graph(
            GraphDiagnostics::State &state, const GraphView &graph) noexcept
        {
            if (!graph.is_nested()) { return 0; }
            NodeView parent = graph.as_nested().parent_node();
            const auto found = state.node_entities.find(parent.data());
            return found == state.node_entities.end() ? 0 : found->second;
        }

        [[nodiscard]] std::uint64_t parent_id_for_node(
            GraphDiagnostics::State &state, const NodeView &node) noexcept
        {
            GraphView graph = node.graph();
            const auto found = state.graph_entities.find(graph.data());
            return found == state.graph_entities.end() ? 0 : found->second;
        }

        [[nodiscard]] std::string child_path(const GraphDiagnostics::State &state,
                                             std::uint64_t parent_id,
                                             std::string_view suffix)
        {
            const auto parent = state.entries.find(parent_id);
            std::string result = parent == state.entries.end()
                                     ? std::string{"[]"}
                                     : parent->second.snapshot.path;
            result += suffix;
            return result;
        }

        void attach_to_parent(GraphDiagnostics::State &state, std::uint64_t parent_id,
                              std::uint64_t child_id)
        {
            if (parent_id == 0) { return; }
            const auto parent = state.entries.find(parent_id);
            if (parent != state.entries.end())
            {
                parent->second.snapshot.children.push_back(child_id);
            }
        }

        void update_totals(GraphDiagnostics::State &state,
                           GraphDiagnostics::State::EntryState &entry,
                           NodeStorageMetrics metrics)
        {
            state.dynamic_live_bytes -= entry.snapshot.storage.dynamic_live_bytes;
            state.dynamic_reserved_bytes -= entry.snapshot.storage.dynamic_reserved_bytes;
            state.dynamic_live_bytes += metrics.dynamic_live_bytes;
            state.dynamic_reserved_bytes += metrics.dynamic_reserved_bytes;
            state.peak_dynamic_live_bytes = std::max(
                state.peak_dynamic_live_bytes, state.dynamic_live_bytes);
            state.peak_dynamic_reserved_bytes = std::max(
                state.peak_dynamic_reserved_bytes, state.dynamic_reserved_bytes);
            entry.snapshot.storage = metrics;
            max_storage(entry.snapshot.peak_storage, metrics);
        }

        void clear_dynamic_totals(GraphDiagnostics::State &state,
                                  GraphDiagnostics::State::EntryState &entry) noexcept
        {
            state.dynamic_live_bytes -= entry.snapshot.storage.dynamic_live_bytes;
            state.dynamic_reserved_bytes -= entry.snapshot.storage.dynamic_reserved_bytes;
            entry.snapshot.storage.nested_graph_count = 0;
            entry.snapshot.storage.nested_graph_capacity = 0;
            entry.snapshot.storage.nested_graph_blocks = 0;
            entry.snapshot.storage.dynamic_live_bytes = 0;
            entry.snapshot.storage.dynamic_reserved_bytes = 0;
        }

        [[nodiscard]] NodeStorageMetrics graph_storage_metrics(
            const GraphView &graph) noexcept
        {
            NodeStorageMetrics result{};
            if (!graph.valid()) { return result; }
            result.static_bytes = graph.type().checked_plan().layout.size;
            if (graph.is_root())
            {
                const DynamicStorageMetrics pooled =
                    graph.compound_scalar_storage().metrics();
                result.dynamic_live_bytes = pooled.live_bytes;
                result.dynamic_reserved_bytes = pooled.reserved_bytes;
            }
            return result;
        }

        void refresh_graph(GraphDiagnostics::State &state, const GraphView &graph)
        {
            auto *entry = find_entry(state, state.graph_entities, graph.data());
            if (entry == nullptr) { return; }
            entry->snapshot.started = graph.started();
            entry->snapshot.evaluation_time = graph.evaluation_time();
            entry->snapshot.scheduled_time = graph.next_scheduled_time();
            update_totals(state, *entry, graph_storage_metrics(graph));

            for (std::size_t index = 0; index < graph.node_count(); ++index)
            {
                NodeView node = graph.node_at(index);
                auto *node_entry = find_entry(state, state.node_entities, node.data());
                if (node_entry == nullptr) { continue; }
                node_entry->snapshot.scheduled_time = graph.node_scheduled_time(index);
                update_totals(state, *node_entry, node.storage_metrics());
            }
        }

        [[nodiscard]] bool failed_node_is(const NodeView &node)
        {
            NodeView failed = node.graph().failed_node();
            return failed.valid() && failed.pointer() == node.pointer();
        }
    }  // namespace

    GraphDiagnostics::GraphDiagnostics(GraphDiagnosticsOptions options)
        : options_(options), state_(std::make_shared<State>())
    {
    }

    GraphDiagnostics::GraphDiagnostics(std::size_t recent_window)
        : GraphDiagnostics(GraphDiagnosticsOptions{.recent_window = recent_window})
    {
    }

    GraphDiagnosticsSnapshot GraphDiagnostics::snapshot() const
    {
        std::scoped_lock lock{state_->mutex};
        GraphDiagnosticsSnapshot result;
        result.graph_cycles = state_->graph_cycles;
        result.wall_time = state_->wall_started.has_value()
                               ? elapsed(*state_->wall_started, DiagnosticsClock::now())
                               : state_->wall_time;
        result.root_evaluation_time = state_->root_evaluation_time;
        result.scheduling_lag_total = state_->scheduling_lag_total;
        result.scheduling_lag_max = state_->scheduling_lag_max;
        result.scheduling_lag_samples = state_->scheduling_lag_samples;
        result.runtime_load = result.wall_time > TimeDelta{0}
                                  ? static_cast<double>(result.root_evaluation_time.count()) /
                                        static_cast<double>(result.wall_time.count())
                                  : 0.0;
        result.planned_bytes = state_->planned_bytes;
        result.dynamic_live_bytes = state_->dynamic_live_bytes;
        result.dynamic_reserved_bytes = state_->dynamic_reserved_bytes;
        result.peak_dynamic_live_bytes = state_->peak_dynamic_live_bytes;
        result.peak_dynamic_reserved_bytes = state_->peak_dynamic_reserved_bytes;
        result.entries.reserve(state_->entries.size());
        for (const auto &[id, entry] : state_->entries)
        {
            static_cast<void>(id);
            GraphDiagnosticEntry copy = entry.snapshot;
            copy.start = entry.start.snapshot;
            copy.evaluation = entry.evaluation.snapshot;
            copy.stop = entry.stop.snapshot;
            result.entries.push_back(std::move(copy));
        }
        return result;
    }

    void GraphDiagnostics::reset()
    {
        std::scoped_lock lock{state_->mutex};
        if (!state_->graph_entities.empty())
        {
            throw std::logic_error{"graph diagnostics cannot be reset while a graph is active"};
        }
        state_->entries.clear();
        state_->graph_entities.clear();
        state_->node_entities.clear();
        state_->table_projections.clear();
        state_->next_id = 1;
        state_->wall_started.reset();
        state_->root_evaluation_started.reset();
        state_->wall_time = TimeDelta{0};
        state_->graph_cycles = 0;
        state_->root_evaluation_time = TimeDelta{0};
        state_->scheduling_lag_total = TimeDelta{0};
        state_->scheduling_lag_max = TimeDelta{0};
        state_->scheduling_lag_samples = 0;
        state_->planned_bytes = 0;
        state_->dynamic_live_bytes = 0;
        state_->dynamic_reserved_bytes = 0;
        state_->peak_dynamic_live_bytes = 0;
        state_->peak_dynamic_reserved_bytes = 0;
    }

    void GraphDiagnostics::on_before_start_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (graph.is_root()) { state_->wall_started = DiagnosticsClock::now(); }
        const std::uint64_t id = state_->next_id++;
        const std::uint64_t parent_id = parent_id_for_graph(*state_, graph);
        GraphDiagnosticEntry snapshot{
            .id = id,
            .parent_id = parent_id,
            .path = graph.is_root()
                        ? std::string{"[]"}
                        : child_path(*state_, parent_id,
                                     "/" + diagnostic::graph_label(graph) +
                                         "#" + std::to_string(id)),
            .label = diagnostic::graph_label(graph),
            .schema_label = schema_label(graph.type().record()),
            .implementation_label = implementation_label(graph.type().record()),
            .kind = GraphDiagnosticEntityKind::Graph,
            .started = graph.started(),
            .evaluation_time = graph.evaluation_time(),
            .scheduled_time = graph.next_scheduled_time(),
        };
        auto [entry, inserted] = state_->entries.emplace(
            id, State::EntryState{.snapshot = std::move(snapshot)});
        static_cast<void>(inserted);
        update_totals(*state_, entry->second, graph_storage_metrics(graph));
        state_->graph_entities[graph.data()] = id;
        attach_to_parent(*state_, parent_id, id);
        if (graph.is_root())
        {
            state_->planned_bytes = std::max(
                state_->planned_bytes, graph.type().checked_plan().layout.size);
        }
        begin_phase(state_->entries.at(id), DiagnosticsPhase::Start);
    }

    void GraphDiagnostics::on_after_start_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, false, options_.recent_window);
            entry->snapshot.started = true;
            refresh_graph(*state_, graph);
        }
    }

    void GraphDiagnostics::on_start_graph_failed(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, true, options_.recent_window);
            entry->snapshot.stopped = true;
            entry->snapshot.started = false;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, DiagnosticsClock::now());
            state_->wall_started.reset();
        }
    }

    void GraphDiagnostics::on_before_start_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        const std::uint64_t id = state_->next_id++;
        const std::uint64_t parent_id = parent_id_for_node(*state_, node);
        const NodeStorageMetrics storage = node.storage_metrics();
        GraphDiagnosticEntry snapshot{
            .id = id,
            .parent_id = parent_id,
            .path = child_path(*state_, parent_id,
                               "." + diagnostic::node_label(node) +
                                   "<" + std::to_string(node.node_index()) + ">"),
            .label = diagnostic::node_label(node),
            .schema_label = schema_label(node.type().record()),
            .implementation_label = implementation_label(node.type().record()),
            .kind = GraphDiagnosticEntityKind::Node,
            .node_kind = node.node_kind(),
            .node_index = node.node_index(),
            .started = node.started(),
            .evaluation_time = node.graph().evaluation_time(),
            .scheduled_time = node.graph().node_scheduled_time(node.node_index()),
            .inspection = node.inspection_metrics(),
            .input = GraphDiagnosticValue{
                .available = node.has_input(),
                .schema_label = node.has_input()
                                    ? std::string{node.schema()->input_schema->name()}
                                    : std::string{},
            },
            .output = GraphDiagnosticValue{
                .available = node.has_output(),
                .schema_label = node.has_output()
                                    ? std::string{node.schema()->output_schema->name()}
                                    : std::string{},
            },
            .scalars = GraphDiagnosticValue{
                .available = node.has_scalars(),
                .schema_label = node.has_scalars()
                                    ? std::string{node.schema()->scalar_schema->name()}
                                    : std::string{},
            },
        };
        auto [entry, inserted] = state_->entries.emplace(
            id, State::EntryState{.snapshot = std::move(snapshot)});
        static_cast<void>(inserted);
        state_->node_entities[node.data()] = id;
        attach_to_parent(*state_, parent_id, id);
        update_totals(*state_, entry->second, storage);
        begin_phase(entry->second, DiagnosticsPhase::Start);
    }

    void GraphDiagnostics::on_after_start_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, false, options_.recent_window);
            entry->snapshot.started = true;
            entry->snapshot.inspection = node.inspection_metrics();
            update_totals(*state_, *entry, node.storage_metrics());
            if (options_.capture_values)
            {
                capture_node_values(entry->snapshot, node,
                                    state_->table_projections,
                                    resolve_target_node,
                                    &state_->node_entities);
            }
        }
    }

    void GraphDiagnostics::on_start_node_failed(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void GraphDiagnostics::on_before_graph_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (graph.is_root())
        {
            state_->root_evaluation_started = DiagnosticsClock::now();
            if (graph.executor().schema()->mode == GraphExecutorMode::RealTime)
            {
                const TimeDelta lag = std::max(
                    current_wall_time() - graph.evaluation_time(), TimeDelta{0});
                state_->scheduling_lag_total += lag;
                state_->scheduling_lag_max = std::max(state_->scheduling_lag_max, lag);
                ++state_->scheduling_lag_samples;
            }
        }
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            refresh_graph(*state_, graph);
            begin_phase(*entry, DiagnosticsPhase::Evaluation);
        }
    }

    void GraphDiagnostics::on_after_graph_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Evaluation,
                      graph.failed_node().valid(), options_.recent_window);
            refresh_graph(*state_, graph);
        }
        if (graph.is_root())
        {
            ++state_->graph_cycles;
            if (state_->root_evaluation_started.has_value())
            {
                state_->root_evaluation_time += elapsed(
                    *state_->root_evaluation_started, DiagnosticsClock::now());
                state_->root_evaluation_started.reset();
            }
        }
    }

    void GraphDiagnostics::on_before_node_evaluation(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            begin_phase(*entry, DiagnosticsPhase::Evaluation);
        }
    }

    void GraphDiagnostics::on_after_node_evaluation(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Evaluation, failed_node_is(node),
                      options_.recent_window);
            entry->snapshot.evaluation_time = node.graph().evaluation_time();
            entry->snapshot.scheduled_time =
                node.graph().node_scheduled_time(node.node_index());
            entry->snapshot.inspection = node.inspection_metrics();
            update_totals(*state_, *entry, node.storage_metrics());
            if (options_.capture_values)
            {
                capture_node_values(entry->snapshot, node,
                                    state_->table_projections,
                                    resolve_target_node,
                                    &state_->node_entities);
            }
        }
    }

    void GraphDiagnostics::on_after_graph_push_nodes_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        refresh_graph(*state_, graph);
    }

    void GraphDiagnostics::on_before_stop_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            entry->snapshot.inspection = node.inspection_metrics();
            update_totals(*state_, *entry, node.storage_metrics());
            if (options_.capture_values)
            {
                capture_node_values(entry->snapshot, node,
                                    state_->table_projections,
                                    resolve_target_node,
                                    &state_->node_entities);
            }
            begin_phase(*entry, DiagnosticsPhase::Stop);
        }
    }

    void GraphDiagnostics::on_after_stop_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, false, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            entry->snapshot.inspection = node.inspection_metrics();
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void GraphDiagnostics::on_stop_node_failed(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void GraphDiagnostics::on_before_stop_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            refresh_graph(*state_, graph);
            begin_phase(*entry, DiagnosticsPhase::Stop);
        }
    }

    void GraphDiagnostics::on_after_stop_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, false, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, DiagnosticsClock::now());
            state_->wall_started.reset();
        }
    }

    void GraphDiagnostics::on_stop_graph_failed(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, DiagnosticsClock::now());
            state_->wall_started.reset();
        }
    }
}  // namespace hgraph
