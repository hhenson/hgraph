#include <hgraph/runtime/distributed_boundary.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_hash.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace hgraph::distributed
{
    namespace
    {
        constexpr unsigned live_flag = 1;
        constexpr unsigned full_flag = 2;
        void byte(unsigned value, std::string &bytes) { bytes.push_back(static_cast<char>(value)); }
        unsigned byte(BinaryReader &reader) { return std::to_integer<unsigned>(*reader.take(1)); }
        void invalidate(const TSOutputView &out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.invalidate());
        }
        bool selected(std::size_t hash, std::size_t group, std::size_t groups)
        { return groups == 0 || hash % groups == group; }
    }

    struct BoundaryTransfer::Plan
    {
        using Capture = void (*)(const Plan &, const TSInputView &, bool, std::size_t,
                                  std::size_t, std::string &);
        // Decode a delta completely before publishing any of its mutations. Values
        // are decoded once; staging is proportional to the changed subtree.
        struct Decoded
        {
            unsigned flags{};
            std::size_t extent{};
            bool clear{};
            bool push{};
            Value value{};
            std::vector<Value> removed{};
            std::vector<Value> keys{};
            std::vector<std::size_t> indices{};
            std::vector<Decoded> children{};
            std::vector<DateTime> times{};
        };
        using Decode = void (*)(const Plan &, Decoded &, BinaryReader &, DateTime);
        using Apply = void (*)(const Plan &, const TSOutputView &, const Decoded &, bool);
        const TSValueTypeMetaData *schema{};
        BoundBinaryConverter value{};
        BoundBinaryConverter key{};
        BoundBinaryConverter time{};
        ValueTypeRef element_binding{};
        std::vector<std::unique_ptr<Plan>> children{};
        std::unordered_map<std::string_view, std::size_t> fields{};
        Capture capture{};
        Apply apply{};
        Decode decode{};
        bool full_when_unbound{false};

        explicit Plan(const TSValueTypeMetaData *type) : schema(type)
        {
            if (!schema) throw std::invalid_argument("distributed boundary requires a time-series schema");
            switch (schema->kind)
            {
                case TSTypeKind::TS:
                case TSTypeKind::SIGNAL:
                    value = bind_binary_converter(schema->delta_value_schema);
                    capture = &capture_atomic; apply = &apply_atomic; decode = &decode_atomic; break;
                case TSTypeKind::TSS:
                    full_when_unbound = true;
                    key = bind_binary_converter(schema->value_schema->element_type);
                    capture = &capture_set; apply = &apply_set; decode = &decode_set; break;
                case TSTypeKind::TSD:
                    full_when_unbound = true;
                    key = bind_binary_converter(schema->data.tsd.key_type);
                    children.push_back(std::make_unique<Plan>(schema->element_ts()));
                    capture = &capture_dict; apply = &apply_dict; decode = &decode_dict; break;
                case TSTypeKind::TSL:
                    full_when_unbound = true;
                    children.push_back(std::make_unique<Plan>(schema->element_ts()));
                    capture = &capture_list; apply = &apply_list; decode = &decode_list; break;
                case TSTypeKind::TSB:
                    for (std::size_t index = 0; index < schema->data.tsb.field_count; ++index)
                    {
                        const auto &field = schema->data.tsb.fields[index];
                        fields.emplace(field.name, index);
                        children.push_back(std::make_unique<Plan>(field.type));
                    }
                    capture = &capture_bundle; apply = &apply_bundle; decode = &decode_bundle; break;
                case TSTypeKind::TSW:
                    full_when_unbound = true;
                    value = bind_binary_converter(schema->value_type);
                    time = bind_binary_converter(TypeRegistry::instance().register_scalar<DateTime>("datetime"));
                    element_binding = value.binding();
                    capture = &capture_window; apply = &apply_window; decode = &decode_window; break;
                case TSTypeKind::REF:
                    throw std::invalid_argument("distributed boundaries require recursively materialized REF values");
            }
        }

        void write(const TSInputView &in, bool full, std::size_t group, std::size_t groups,
                   std::string &bytes) const
        {
            full = full || in.delta_is_sampled_rebind() ||
                   (full_when_unbound && !in.data_view().valid());
            byte((in.valid() ? live_flag : 0) | (full ? full_flag : 0), bytes);
            capture(*this, in, full, group, groups, bytes);
        }
        [[nodiscard]] Decoded read(BinaryReader &reader, DateTime evaluation_time) const
        {
            auto depth = reader.enter();
            Decoded decoded;
            decoded.flags = byte(reader);
            if (decoded.flags & ~(live_flag | full_flag))
                throw std::invalid_argument("invalid distributed boundary flags");
            decode(*this, decoded, reader, evaluation_time);
            return decoded;
        }
        static bool more(BinaryReader &reader)
        {
            const auto marker = byte(reader);
            if (marker > 1) throw std::invalid_argument("invalid distributed boundary marker");
            if (marker) reader.consume_work(1);
            return marker != 0;
        }
        static void decode_atomic(const Plan &plan, Decoded &decoded, BinaryReader &reader, DateTime)
        {
            if (decoded.flags & live_flag) decoded.value = plan.value.read(reader);
        }
        static void decode_set(const Plan &plan, Decoded &decoded, BinaryReader &reader, DateTime)
        {
            while (more(reader)) decoded.removed.push_back(plan.key.read(reader));
            while (more(reader)) decoded.keys.push_back(plan.key.read(reader));
        }
        static void decode_dict(const Plan &plan, Decoded &decoded, BinaryReader &reader, DateTime evaluation_time)
        {
            while (more(reader)) decoded.removed.push_back(plan.key.read(reader));
            std::unordered_set<Value, ValueHash, ValueEqual> updated;
            while (more(reader))
            {
                decoded.keys.push_back(plan.key.read(reader));
                if (!updated.insert(decoded.keys.back()).second)
                    throw std::invalid_argument("duplicate distributed dictionary update");
                decoded.children.push_back(plan.children.front()->read(reader, evaluation_time));
            }
        }
        static void decode_list(const Plan &plan, Decoded &decoded, BinaryReader &reader, DateTime evaluation_time)
        {
            const auto extent = read_varint(reader);
            // Sparse payloads may name a large extent with almost no bytes.
            // Charge the resulting endpoint count, before any resize/allocation.
            reader.consume_work(extent);
            decoded.extent = static_cast<std::size_t>(extent);
            if (!plan.schema->is_unbounded_tsl() && extent != plan.schema->fixed_size())
                throw std::invalid_argument("distributed fixed list size mismatch");
            std::unordered_set<std::size_t> updated;
            while (more(reader))
            {
                const auto index = read_varint(reader);
                if (!updated.insert(static_cast<std::size_t>(index)).second)
                    throw std::invalid_argument("duplicate distributed indexed update");
                if (index >= extent) throw std::invalid_argument("distributed list index exceeds length");
                decoded.indices.push_back(static_cast<std::size_t>(index));
                decoded.children.push_back(plan.children.front()->read(reader, evaluation_time));
            }
        }
        static void decode_bundle(const Plan &plan, Decoded &decoded, BinaryReader &reader, DateTime evaluation_time)
        {
            std::unordered_set<std::size_t> updated;
            while (more(reader))
            {
                const auto index = read_varint(reader);
                if (!updated.insert(static_cast<std::size_t>(index)).second)
                    throw std::invalid_argument("duplicate distributed indexed update");
                if (index >= plan.children.size()) throw std::invalid_argument("distributed bundle index exceeds shape");
                decoded.indices.push_back(static_cast<std::size_t>(index));
                decoded.children.push_back(plan.children[index]->read(reader, evaluation_time));
            }
        }
        static void decode_window(const Plan &plan, Decoded &decoded, BinaryReader &reader, DateTime evaluation_time)
        {
            if (decoded.flags & full_flag)
            {
                const auto count = read_varint(reader);
                reader.consume_work(count);
                if (count > reader.remaining()) throw std::invalid_argument("distributed window sample count exceeds frame");
                if (!plan.schema->is_duration_based() && count > plan.schema->period())
                    throw std::invalid_argument("distributed window sample count exceeds period");
                ListBuilder samples{plan.element_binding};
                decoded.times.reserve(static_cast<std::size_t>(count));
                for (std::uint64_t index = 0; index < count; ++index)
                {
                    const auto sample_time = plan.time.read(reader).view().checked_as<DateTime>();
                    if (sample_time <= MIN_DT || sample_time > evaluation_time ||
                        (!decoded.times.empty() && sample_time < decoded.times.back()))
                        throw std::invalid_argument("distributed window has invalid chronological samples");
                    if (plan.schema->is_duration_based() && !decoded.times.empty() &&
                        static_cast<std::uint64_t>(sample_time.time_since_epoch().count()) -
                            static_cast<std::uint64_t>(decoded.times.front().time_since_epoch().count()) >
                        static_cast<std::uint64_t>(plan.schema->time_range().count()))
                        throw std::invalid_argument("distributed window samples exceed time range");
                    decoded.times.push_back(sample_time);
                    const auto value = plan.value.read(reader);
                    samples.push_back_copy(value.view().data());
                }
                decoded.value = samples.build();
            }
            else
            {
                decoded.clear = more(reader);
                decoded.push = more(reader);
                if (decoded.push) decoded.value = plan.value.read(reader);
            }
        }

        static void capture_atomic(const Plan &plan, const TSInputView &in, bool,
                                   std::size_t, std::size_t, std::string &bytes)
        {
            if (!in.valid()) return;
            if (plan.schema->kind == TSTypeKind::SIGNAL)
                plan.value.write(Value{true}.view(), bytes);
            else plan.value.write(in.value(), bytes);
        }
        static void apply_atomic(const Plan &, const TSOutputView &out, const Decoded &decoded,
                                 bool)
        {
            const auto flags = decoded.flags;
            if (!(flags & live_flag)) { invalidate(out); return; }
            const auto &value = decoded.value;
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.copy_value_from(value.view()));
            mutation.mark_modified();
        }

        static void capture_set(const Plan &plan, const TSInputView &in, bool full,
                                std::size_t, std::size_t, std::string &bytes)
        {
            const auto set = in.as_set();
            if (full)
            {
                byte(0, bytes);
                for (const auto item : set.values()) { byte(1, bytes); plan.key.write(item, bytes); }
            }
            else
            {
                const auto data = in.data_view().as_set();
                for (auto slot = data.next_removed_slot(); slot != TS_DATA_NO_CHILD_ID;
                     slot = data.next_removed_slot(slot))
                { byte(1, bytes); plan.key.write(data.at_slot(slot), bytes); }
                byte(0, bytes);
                for (auto slot = data.next_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                     slot = data.next_added_slot(slot))
                { byte(1, bytes); plan.key.write(data.at_slot(slot), bytes); }
            }
            byte(0, bytes);
        }
        static void apply_set(const Plan &, const TSOutputView &out, const Decoded &decoded,
                              bool merge)
        {
            const auto flags = decoded.flags;
            const auto set = out.as_set();
            auto mutation = set.begin_mutation(out.evaluation_time());
            const bool full = (flags & full_flag) && !merge;
            std::unordered_set<Value, ValueHash, ValueEqual> present;
            for (const auto &key : decoded.removed) (void)mutation.remove(key.view());
            for (const auto &key : decoded.keys)
            {
                (void)mutation.add(key.view());
                if (full) present.insert(key);
            }
            if (full)
            {
                std::vector<Value> removed;
                for (const auto key : set.values())
                    if (!present.contains(key)) removed.emplace_back(key);
                for (const auto &key : removed) (void)mutation.remove(key.view());
            }
            if (flags & live_flag) mutation.touch();
            else if (!merge) invalidate(out);
        }

        static void capture_dict(const Plan &plan, const TSInputView &in, bool full,
                                 std::size_t group, std::size_t groups, std::string &bytes)
        {
            const auto dict = in.as_dict();
            const auto write_item = [&](const ValueView &key, const TSInputView &child, bool sample) {
                if (groups != 0 && !selected(plan.key.portable_hash(key), group, groups)) return;
                byte(1, bytes); plan.key.write(key, bytes);
                plan.children.front()->write(child, sample, 0, 0, bytes);
            };
            if (full)
            {
                byte(0, bytes); // no removals; receiver reconciles the full key set
                for (const auto &[key, child] : dict.items()) write_item(key, child, true);
            }
            else
            {
                // The dictionary value delta omits keys whose values have not
                // become valid. Membership belongs to its actual key-set
                // endpoint, independently of child publication/validity.
                const auto data = in.data_view().as_dict();
                const auto keys = data.key_set();
                const bool structure_changed = keys.modified(in.evaluation_time());
                if (structure_changed)
                    for (auto slot = data.next_membership_removed_slot(); slot != TS_DATA_NO_CHILD_ID;
                         slot = data.next_membership_removed_slot(slot))
                    {
                        const auto key = keys.at_slot(slot);
                        if (groups == 0 || selected(plan.key.portable_hash(key), group, groups))
                        { byte(1, bytes); plan.key.write(key, bytes); }
                    }
                byte(0, bytes);
                if (structure_changed)
                    for (auto slot = data.next_membership_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                         slot = data.next_membership_added_slot(slot))
                    {
                        const auto key = keys.at_slot(slot);
                        write_item(key, dict.at(key), true);
                    }
                for (auto slot = data.next_modified_slot(); slot != TS_DATA_NO_CHILD_ID;
                     slot = data.next_modified_slot(slot))
                    if (!structure_changed || !data.membership_slot_added(slot))
                    {
                        const auto key = data.key_at_slot(slot);
                        write_item(key, dict.at(key), false);
                    }
                // A child can lose validity while its key remains live. The
                // ordinary value delta reports that as a removed publication,
                // while the boundary must retain the key and invalidate only
                // the child.
                for (auto slot = data.next_removed_slot(); slot != TS_DATA_NO_CHILD_ID;
                     slot = data.next_removed_slot(slot))
                    if (data.slot_live(slot) && (!structure_changed || !data.membership_slot_added(slot)))
                    {
                        const auto key = data.key_at_slot(slot);
                        write_item(key, dict.at(key), false);
                    }
            }
            byte(0, bytes);
        }
        static void apply_dict(const Plan &plan, const TSOutputView &out, const Decoded &decoded,
                               bool merge)
        {
            const auto flags = decoded.flags;
            const auto dict = out.as_dict();
            auto mutation = dict.begin_mutation(out.evaluation_time());
            const bool full = (flags & full_flag) && !merge;
            std::unordered_set<Value, ValueHash, ValueEqual> present;
            for (const auto &key : decoded.removed) (void)mutation.erase(key.view());
            for (std::size_t index = 0; index < decoded.keys.size(); ++index)
            {
                const auto &key = decoded.keys[index];
                const auto child = mutation.at(key.view());
                const auto &child_plan = *plan.children.front();
                child_plan.apply(child_plan, TSOutputView{out.output(), child, out.evaluation_time()},
                                 decoded.children[index], false);
                if (full) present.insert(key);
            }
            if (full)
            {
                std::vector<Value> removed;
                for (const auto key : dict.keys())
                    if (!present.contains(key)) removed.emplace_back(key);
                for (const auto &key : removed) (void)mutation.erase(key.view());
            }
            if (flags & live_flag) mutation.touch();
            else if (!merge) invalidate(out);
        }

        static void capture_list(const Plan &plan, const TSInputView &in, bool full,
                                 std::size_t group, std::size_t groups, std::string &bytes)
        {
            const auto list = in.as_list();
            if (!in.data_view().valid())
            {
                write_varint(plan.schema->is_unbounded_tsl() ? 0 : plan.schema->fixed_size(), bytes);
                byte(0, bytes);
                return;
            }
            write_varint(list.size(), bytes);
            const auto write_item = [&](std::size_t index, const TSInputView &child) {
                if (!selected(index, group, groups)) return;
                byte(1, bytes); write_varint(index, bytes);
                plan.children.front()->write(child, full, 0, 0, bytes);
            };
            if (full)
                for (const auto &[index, child] : list.items()) write_item(index, child);
            else if (in.is_bindable() && in.data_view().valid())
            {
                // Owning list strategies provide a dense changed-index surface.
                // A non-peered prefix below uses input-local modification flags,
                // which additionally include independently sampled child links.
                const auto data = in.data_view().as_list();
                if (data.modified(in.evaluation_time()))
                    for (const auto index : data.modified_indices()) write_item(index, list.at(index));
            }
            else
                for (const auto &[index, child] : list.modified_items()) write_item(index, child);
            byte(0, bytes);
        }
        static void apply_list(const Plan &plan, const TSOutputView &out, const Decoded &decoded,
                               bool merge)
        {
            const auto flags = decoded.flags;
            const auto size = decoded.extent;
            auto list = out.as_list();
            if (plan.schema->is_unbounded_tsl())
            {
                if (!merge) list.resize(size);
                else if (!decoded.indices.empty())
                {
                    const auto required = *std::max_element(decoded.indices.begin(), decoded.indices.end()) + 1;
                    if (required > list.size()) list.resize(required);
                }
            }
            else if (size != list.size()) throw std::invalid_argument("distributed fixed list size mismatch");
            for (std::size_t item = 0; item < decoded.indices.size(); ++item)
            {
                const auto index = decoded.indices[item];
                const auto &child_plan = *plan.children.front();
                child_plan.apply(child_plan, list.at(index), decoded.children[item], false);
            }
            if (flags & live_flag)
            {
                auto mutation = out.begin_mutation(out.evaluation_time()); mutation.mark_modified();
            }
            else if (!merge) invalidate(out);
        }

        static void capture_bundle(const Plan &plan, const TSInputView &in, bool full,
                                   std::size_t, std::size_t, std::string &bytes)
        {
            const auto bundle = in.as_bundle();
            const auto items = full ? bundle.items() : bundle.modified_items();
            for (const auto &[name, child] : items)
            {
                const auto index = plan.fields.at(name);
                byte(1, bytes); write_varint(index, bytes);
                plan.children[index]->write(child, full, 0, 0, bytes);
            }
            byte(0, bytes);
        }
        static void apply_bundle(const Plan &plan, const TSOutputView &out, const Decoded &decoded,
                                 bool merge)
        {
            const auto flags = decoded.flags;
            for (std::size_t item = 0; item < decoded.indices.size(); ++item)
            {
                const auto index = decoded.indices[item];
                const auto &child_plan = *plan.children[index];
                child_plan.apply(child_plan, out.indexed_child_at(index), decoded.children[item], false);
            }
            if (flags & live_flag)
            {
                auto mutation = out.begin_mutation(out.evaluation_time()); mutation.mark_modified();
            }
            else if (!merge) invalidate(out);
        }

        static void capture_window(const Plan &plan, const TSInputView &in, bool full,
                                   std::size_t, std::size_t, std::string &bytes)
        {
            const auto window = in.as_window();
            if (full)
            {
                write_varint(window.size(), bytes);
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    plan.time.write(Value{window.time_at(index)}.view(), bytes);
                    plan.value.write(window.at(index), bytes);
                }
            }
            else
            {
                byte(window.data_view().cleared(in.evaluation_time()), bytes);
                const auto delta = in.delta_value();
                byte(delta.has_value(), bytes);
                if (delta.has_value()) plan.value.write(delta, bytes);
            }
        }
        static void apply_window(const Plan &, const TSOutputView &out, const Decoded &decoded,
                                 bool)
        {
            const auto flags = decoded.flags;
            auto window = out.as_window();
            auto mutation = window.begin_mutation(out.evaluation_time());
            if (flags & full_flag)
            {
                if (!decoded.times.empty() || (flags & live_flag))
                    mutation.replace_samples(decoded.value.view(), decoded.times);
                else if (!window.empty()) mutation.clear();
                if (!(flags & live_flag)) invalidate(out);
            }
            else
            {
                const bool clear = decoded.clear;
                const bool push = decoded.push;
                if (clear) mutation.clear();
                if (push) mutation.push(decoded.value.view());
                if (!clear && !push && !(flags & live_flag)) invalidate(out);
            }
        }
    };

    BoundaryTransfer::BoundaryTransfer(const TSValueTypeMetaData *schema, BinaryDecodeLimits limits)
        : plan_(std::make_shared<const Plan>(schema)), limits_(limits) {}
    const TSValueTypeMetaData *BoundaryTransfer::schema() const noexcept { return plan_->schema; }
    const ValueTypeMetaData *BoundaryTransfer::payload_schema()
    { return TypeRegistry::instance().register_scalar<std::string>("str"); }
    std::size_t BoundaryTransfer::key_hash(const ValueView &key) const
    { return plan_->key.portable_hash(key); }
    Value BoundaryTransfer::capture(const TSInputView &input, bool full,
                                    std::size_t group, std::size_t groups) const
    {
        if (input.schema() != schema()) throw std::invalid_argument("distributed capture schema mismatch");
        if (groups && group >= groups) throw std::invalid_argument("distributed capture group out of range");
        std::string bytes;
        plan_->write(input, full, group, groups, bytes);
        return Value{std::move(bytes)};
    }
    std::optional<std::size_t> BoundaryTransfer::root_list_size(const ValueView &payload) const
    {
        if (schema()->kind != TSTypeKind::TSL) return {};
        BinaryReader reader{payload.checked_as<std::string>(), 0, limits_};
        const auto flags = byte(reader);
        if (flags & ~(live_flag | full_flag)) throw std::invalid_argument("invalid distributed boundary flags");
        const auto extent = read_varint(reader);
        reader.consume_work(extent);
        return static_cast<std::size_t>(extent);
    }
    void BoundaryTransfer::apply(const TSOutputView &output, const ValueView &payload, bool merge) const
    {
        if (output.schema() != schema()) throw std::invalid_argument("distributed apply schema mismatch");
        BinaryReader reader{payload.checked_as<std::string>(), 0, limits_};
        const auto decoded = plan_->read(reader, output.evaluation_time());
        if (reader.remaining()) throw std::invalid_argument("trailing distributed boundary bytes");
        plan_->apply(*plan_, output, decoded, merge);
    }
}
