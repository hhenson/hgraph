//
// Created by Howard Henson on 02/05/2025.
//

#ifndef TSL_H
#define TSL_H


#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/time_series_visitor.h>

namespace hgraph {
    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    struct TimeSeriesList : T_TS {
        using list_type = TimeSeriesList<T_TS>;
        using ptr = nb::ref<list_type>;
        using typename T_TS::index_ts_type;
        using typename T_TS::ts_type;

        using value_iterator = typename T_TS::value_iterator;
        using value_const_iterator = typename T_TS::value_const_iterator;
        using collection_type = typename T_TS::collection_type;
        using enumerated_collection_type = typename T_TS::enumerated_collection_type;
        using index_collection_type = typename T_TS::index_collection_type;

        using index_ts_type::size;
        using T_TS::T_TS;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        value_iterator begin() { return ts_values().begin(); }
        value_const_iterator begin() const { return const_cast<list_type *>(this)->begin(); }
        value_iterator end() { return ts_values().end(); }
        value_const_iterator end() const { return const_cast<list_type *>(this)->end(); }

        // Retrieves valid keys
        [[nodiscard]] index_collection_type keys() const;

        [[nodiscard]] index_collection_type valid_keys() const;

        [[nodiscard]] index_collection_type modified_keys() const;

        // Retrieves valid items
        [[nodiscard]] enumerated_collection_type items();

        [[nodiscard]] enumerated_collection_type items() const;

        [[nodiscard]] enumerated_collection_type valid_items();

        [[nodiscard]] enumerated_collection_type valid_items() const;

        [[nodiscard]] enumerated_collection_type modified_items();

        [[nodiscard]] enumerated_collection_type modified_items() const;

        [[nodiscard]] bool has_reference() const override;

    protected:
        using T_TS::index_with_constraint;
        using T_TS::ts_values;
    };

    struct TimeSeriesListOutputBuilder;

    struct TimeSeriesListOutput : TimeSeriesList<IndexedTimeSeriesOutput> {
        using list_type::TimeSeriesList;

        void apply_result(nb::object value) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        void py_set_value(nb::object value) override;

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesListOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesListOutput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

        static void register_with_nanobind(nb::module_ &m);

    protected:
        friend TimeSeriesListOutputBuilder;
    };

    struct TimeSeriesListInputBuilder;

    struct TimeSeriesListInput : TimeSeriesList<IndexedTimeSeriesInput> {
        using list_type::TimeSeriesList;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesInputVisitor<TimeSeriesListInput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesInputVisitor<TimeSeriesListInput>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

        static void register_with_nanobind(nb::module_ &m);

    protected:
        friend TimeSeriesListInputBuilder;
    };
} // namespace hgraph
#endif  // TSL_H