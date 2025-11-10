#ifndef TS_INDEXED_H
#define TS_INDEXED_H

#include <algorithm>
#include <hgraph/types/base_time_series.h>

namespace hgraph {
    struct TimeSeriesBundleInputBuilder;
    struct TimeSeriesListInputBuilder;

    template<typename T_TS>
        requires TimeSeriesT<T_TS>
    struct IndexedTimeSeries : T_TS {
        // Map concrete Base types back to interface types for collections
        using ts_type = std::conditional_t<
            std::is_base_of_v<TimeSeriesInput, T_TS>,
            TimeSeriesInput,
            TimeSeriesOutput
        >;
        using index_ts_type = IndexedTimeSeries<T_TS>;
        using ptr = nb::ref<IndexedTimeSeries<ts_type> >;
        using collection_type = std::vector<typename ts_type::ptr>;
        using enumerated_collection_type = std::vector<std::pair<size_t, typename ts_type::ptr> >;
        using index_collection_type = std::vector<size_t>;
        using value_iterator = typename collection_type::iterator;
        using value_const_iterator = typename collection_type::const_iterator;

        using T_TS::T_TS;  // Inherit constructors from actual base class
        using T_TS::valid;  // Inherit valid() from actual base class

        [[nodiscard]] bool all_valid() const override {
            if (empty()) { return true; }
            return valid() && std::ranges::all_of(_ts_values, [](const auto &ts) { return ts->valid(); });
        }

        [[nodiscard]] typename ts_type::ptr &operator[](size_t ndx) { return _ts_values.at(ndx); }

        [[nodiscard]] const typename ts_type::ptr &operator[](size_t ndx) const {
            return const_cast<index_ts_type *>(this)->operator[](ndx);
        }

        [[nodiscard]] collection_type values() { return _ts_values; }
        [[nodiscard]] collection_type values() const { return const_cast<index_ts_type *>(this)->values(); }

        [[nodiscard]] nb::iterator py_valid_values() const {
            // Create a Python list from the vector to avoid dangling references
            auto valid_ = valid_values();
            nb::list py_list;
            for (const auto &item: valid_) {
                py_list.append(item);
            }
            return nb::iter(py_list);
        }

        [[nodiscard]] collection_type valid_values() {
            return values_with_constraint([](const ts_type &ts) { return ts.valid(); });
        }

        [[nodiscard]] collection_type valid_values() const { return const_cast<index_ts_type *>(this)->valid_values(); }

        [[nodiscard]] nb::iterator py_modified_values() const {
            // Create a Python list from the vector to avoid dangling references
            auto modified_ = modified_values();
            nb::list py_list;
            for (const auto &item: modified_) {
                py_list.append(item);
            }
            return nb::iter(py_list);
        }

        [[nodiscard]] collection_type modified_values() {
            return values_with_constraint([](const ts_type &ts) { return ts.modified(); });
        }

        [[nodiscard]] collection_type modified_values() const {
            return const_cast<index_ts_type *>(this)->modified_values();
        }

        [[nodiscard]] size_t size() const { return _ts_values.size(); }

        [[nodiscard]] bool empty() const { return _ts_values.empty(); }

        [[nodiscard]] bool has_reference() const override {
            return std::ranges::any_of(_ts_values, [](const auto &ts) { return ts->has_reference(); });
        }

    protected:
        [[nodiscard]] collection_type &ts_values() { return _ts_values; }

        [[nodiscard]] const collection_type &ts_values() const { return _ts_values; }

        void set_ts_values(collection_type ts_values) { _ts_values = std::move(ts_values); }

        [[nodiscard]] index_collection_type index_with_constraint(
            const std::function<bool(const ts_type &)> &constraint) const {
            index_collection_type result;
            result.reserve(_ts_values.size());
            for (size_t i = 0; i < _ts_values.size(); ++i) {
                if (constraint(*_ts_values[i])) { result.push_back(i); }
            }
            return result;
        }

        [[nodiscard]] collection_type values_with_constraint(
            const std::function<bool(const ts_type &)> &constraint) const {
            collection_type result;
            result.reserve(_ts_values.size());
            for (const auto &value: _ts_values) {
                if (constraint(*value)) { result.push_back(value); }
            }
            return result;
        }

        [[nodiscard]] enumerated_collection_type
        items_with_constraint(const std::function<bool(const ts_type &)> &constraint) const {
            enumerated_collection_type result;
            result.reserve(_ts_values.size());
            for (size_t i = 0; i < _ts_values.size(); ++i) {
                if (constraint(*_ts_values[i])) { result.emplace_back(i, _ts_values[i]); }
            }
            return result;
        }

    private:
        friend TimeSeriesBundleInputBuilder;
        friend TimeSeriesListInputBuilder;
        collection_type _ts_values;
    };

    struct IndexedTimeSeriesOutput : IndexedTimeSeries<BaseTimeSeriesOutput> {
        using index_ts_type::IndexedTimeSeries;

        void invalidate() override;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        void clear() override;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct IndexedTimeSeriesInput : IndexedTimeSeries<BaseTimeSeriesInput> {
        using index_ts_type::IndexedTimeSeries;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] bool valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        [[nodiscard]] bool bound() const override;

        [[nodiscard]] bool active() const override;

        void make_active() override;

        void make_passive() override;

        [[nodiscard]] TimeSeriesInput *get_input(size_t index) override;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        bool do_bind_output(time_series_output_ptr &value) override;

        void do_un_bind_output(bool unbind_refs) override;
    };

    template<typename T_TS>
    concept IndexedTimeSeriesT = std::is_same_v<T_TS, IndexedTimeSeriesInput> || std::is_same_v<T_TS,
                                     IndexedTimeSeriesOutput>;
} // namespace hgraph

#endif  // TS_INDEXED_H