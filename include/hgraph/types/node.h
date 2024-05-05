//
// Created by Howard Henson on 04/05/2024.
//

#ifndef NODE_H
#define NODE_H

#include <hgraph/python/pyb.h>
#include <hgraph/hgraph_export.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <sstream>

#include <hgraph/util/lifecycle.h>

#include "hgraph/util/date_time.h"

namespace hgraph {
    struct Graph;

    template<typename Enum>
    typename std::enable_if<std::is_enum<Enum>::value, Enum>::type
    operator|(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(
            static_cast<underlying>(lhs) | static_cast<underlying>(rhs)
        );
    }

    template<typename Enum>
    typename std::enable_if<std::is_enum<Enum>::value, Enum>::type
    operator&(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(
            static_cast<underlying>(lhs) & static_cast<underlying>(rhs)
        );
    }

    enum class NodeTypeEnum : char8_t {
        NONE = 0,
        SOURCE_NODE = 1,
        PUSH_SOURCE_NODE = SOURCE_NODE | (1 << 1),
        PULL_SOURCE_NODE = SOURCE_NODE | (1 << 2),
        COMPUTE_NODE = 1 << 3,
        SINK_NODE = 1 << 4
    };

    void node_type_enum_py_register(py::module_ &m);

    enum class InjectableTypesEnum : char8_t {
        NONE = 0,
        STATE = 1,
        SCHEDULER = 1 << 1,
        OUTPUT = 1 << 2,
        CLOCK = 1 << 3,
        ENGINE_API = 1 << 4,
        REPLAY_STATE = 1 << 5,
        LOGGER = 1 << 6
    };

    void injectable_type_enum(py::module_ &m);

    struct HGRAPH_EXPORT NodeSignature {
        std::string name{};
        NodeTypeEnum node_type{NodeTypeEnum::NONE};
        std::vector<std::string> args{};
        std::optional<std::unordered_map<std::string, py::object> > time_series_inputs{};
        std::optional<py::object> time_series_output{};
        std::optional<std::unordered_map<std::string, py::object> > scalars{};
        py::object src_location{py::none()};
        std::optional<std::unordered_set<std::string> > active_inputs{};
        std::optional<std::unordered_set<std::string> > valid_inputs{};
        std::optional<std::unordered_set<std::string> > all_valid_inputs{};
        std::optional<std::unordered_set<std::string> > context_inputs{};
        InjectableTypesEnum injectable_inputs{InjectableTypesEnum::NONE};
        std::string wiring_path_name{};
        std::optional<std::string> label{};
        std::optional<std::string> record_replay_id{};
        bool capture_values{false};
        bool capture_exception{false};
        char8_t trace_back_depth{1};

        [[nodiscard]] py::object get_arg_type(const std::string &arg) const;

        [[nodiscard]] std::string signature() const;

        [[nodiscard]] bool uses_scheduler() const;

        [[nodiscard]] bool uses_clock() const;

        [[nodiscard]] bool uses_engine() const;

        [[nodiscard]] bool uses_state() const;

        [[nodiscard]] bool uses_output_feedback() const;

        [[nodiscard]] bool uses_replay_state() const;

        [[nodiscard]] bool is_source_node() const;

        [[nodiscard]] bool is_push_source_node() const;

        [[nodiscard]] bool is_pull_source_node() const;

        [[nodiscard]] bool is_compute_node() const;

        [[nodiscard]] bool is_sink_node() const;

        [[nodiscard]] bool is_recordable() const;

        static void py_register(py::module_ &m);
    };

    struct TimeSeriesBundleInput;
    struct TimeSeriesOutput;

    struct NodeScheduler {
        [[nodiscard]] engine_time_t next_scheduled_time() const;
        [[nodiscard]] bool is_scheduled() const;
        [[nodiscard]] bool is_scheduled_node() const;
        [[nodiscard]] bool has_tag(const std::string& tag) const;
        engine_time_t pop_tag(const std::string& tag, std::optional<engine_time_t> default_time);
        void schedule(engine_time_t when, std::optional<std::string> tag);
        void schedule(engine_time_delta_t when, std::optional<std::string> tag);
        void un_schedule(std::optional<std::string> tag);
        void reset();
    };

    struct HGRAPH_EXPORT Node : ComponentLifeCycle {
        int64_t node_ndx;
        std::vector<int64_t> owning_graph_id;
        std::vector<int64_t> node_id;
        NodeSignature signature;
        std::unordered_map<std::string, py::object> scalars;
        Graph *graph;
        TimeSeriesBundleInput *input;
        TimeSeriesOutput* output;
        TimeSeriesOutput* error_output;
        std::optional<NodeScheduler> scheduler;
        virtual void eval() = 0;
        void notify();
        void notify_next_cycle();
    };

}

#endif //NODE_H
