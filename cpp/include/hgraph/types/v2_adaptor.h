#pragma once

#include <any>
#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/any_value.h>

namespace hgraph
{
    template <typename T>
        requires std::is_same_v<T, TimeSeriesInput> || std::is_same_v<T, TimeSeriesOutput>
    struct ParentAdapter
    {
        using ts_ptr = T *;

        explicit ParentAdapter(node_ptr parent) : _parent_ts_or_node(parent) {}
        explicit ParentAdapter(ts_ptr parent) : _parent_ts_or_node(parent) {}

        [[nodiscard]] bool has_parent_or_node() const { return has_value(); }

        [[nodiscard]] bool has_owning_node() const {
            if (!has_value()) { return false; }
            if (std::holds_alternative<node_ptr>(_parent_ts_or_node)) { return std::get<node_ptr>(_parent_ts_or_node) != nullptr; }
            if constexpr (std::is_same_v<T, TimeSeriesOutput>) {
                return std::get<time_series_output_ptr>(_parent_ts_or_node)->has_owning_node();
            }
            if constexpr (std::is_same_v<T, TimeSeriesInput>) {
                return std::get<time_series_input_ptr>(_parent_ts_or_node)->has_owning_node();
            }
            return false;
        }

        [[nodiscard]] node_ptr owning_node() {
            if (!has_value()) { return nullptr; }
            return std::visit(
                []<typename T_>(T_ &&value) -> Node * {
                    using T_T = std::decay_t<T_>;
                    if constexpr (std::is_same_v<T_T, time_series_output_ptr>) {
                        return value->owning_node();
                    } else if constexpr (std::is_same_v<T_T, time_series_input_ptr>) {
                        return value->owning_node();
                    } else if constexpr (std::is_same_v<T_T, node_ptr>) {
                        return value;
                    } else {
                        return nullptr;
                    }
                },
                _parent_ts_or_node);
        }

        [[nodiscard]] node_ptr  owning_node() const { return const_cast<ParentAdapter *>(this)->owning_node(); }
        [[nodiscard]] graph_ptr owning_graph() {
            auto on{owning_node()};
            return on != nullptr ? owning_node()->graph() : nullptr;
        }
        [[nodiscard]] graph_ptr owning_graph() const { return const_cast<ParentAdapter *>(this)->owning_graph(); }

        void reset_parent_or_node() { _parent_ts_or_node = std::monostate{}; }
        void re_parent(node_ptr parent) { _parent_ts_or_node = parent; }
        void re_parent(const time_series_type_ptr parent) { _parent_ts_or_node = static_cast<ts_ptr>(parent); }

        // If we are an output
        [[nodiscard]] time_series_output_ptr parent_output() const
            requires std::is_same_v<T, TimeSeriesOutput>
        {
            return const_cast<ParentAdapter *>(this)->parent_output();
        }

        [[nodiscard]] time_series_output_ptr parent_output()
            requires std::is_same_v<T, TimeSeriesOutput>
        {
            if (has_value() && std::holds_alternative<time_series_output_ptr>(_parent_ts_or_node)) {
                return std::get<time_series_output_ptr>(_parent_ts_or_node);
            }
            return nullptr;
        }

        [[nodiscard]] bool has_parent_output() const
            requires std::is_same_v<T, TimeSeriesOutput>
        {
            return has_value() && std::holds_alternative<time_series_output_ptr>(_parent_ts_or_node) &&
                   std::get<time_series_output_ptr>(_parent_ts_or_node) != nullptr;
        }

        [[nodiscard]] time_series_input_ptr parent_input() const
            requires std::is_same_v<T, TimeSeriesInput>
        {
            return const_cast<ParentAdapter *>(this)->parent_input();
        }

        [[nodiscard]] time_series_input_ptr parent_input()
            requires std::is_same_v<T, TimeSeriesInput>
        {
            if (std::holds_alternative<time_series_input_ptr>(_parent_ts_or_node)) {
                return std::get<time_series_input_ptr>(_parent_ts_or_node);
            }
            return nullptr;
        }

        [[nodiscard]] bool has_parent_input() const
            requires std::is_same_v<T, TimeSeriesInput>
        {
            return std::holds_alternative<time_series_input_ptr>(_parent_ts_or_node) &&
                   std::get<time_series_input_ptr>(_parent_ts_or_node) != nullptr;
        }

        void notify_modified(TimeSeriesInput *self, engine_time_t modified_time) {
            if (has_parent_input()) {
                parent_input()->notify_parent(self, modified_time);
            } else {
                owning_node()->notify(modified_time);
            }
        }

      protected:
        [[nodiscard]] bool has_value() const { return !std::holds_alternative<std::monostate>(_parent_ts_or_node); }

      private:
        std::variant<std::monostate, ts_ptr, node_ptr> _parent_ts_or_node{std::monostate{}};
    };

    inline nb::object any_to_py_object(const AnyValue<> &value) { return value.as_python(); }
}  // namespace hgraph
