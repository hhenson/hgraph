//
// Created by Howard Henson on 27/12/2024.
//

#ifndef ERROR_TYPE_H
#define ERROR_TYPE_H

#include <hgraph/hgraph_base.h>
#include <hgraph/types/scalar_types.h>

#include <exception>
#include <ostream>
#include <stdexcept>

namespace hgraph {
    struct HGRAPH_EXPORT BacktraceSignature : CompoundScalar {
        std::string name;
        std::vector<std::string> args;
        std::string wiring_path_name;
        std::string runtime_path_name;
        std::string node_id;

        BacktraceSignature(std::string name_, std::vector<std::string> args_, std::string wiring_path_name_,
                           std::string runtime_path_name_, std::string node_id_);

        BacktraceSignature(const BacktraceSignature &) = default;

        BacktraceSignature &operator=(const BacktraceSignature &) = default;

        BacktraceSignature(BacktraceSignature &&) = default;

        BacktraceSignature &operator=(BacktraceSignature &&) = default;

        // Override from AbstractSchema
        [[nodiscard]] const std::vector<std::string> &keys() const override;

        [[nodiscard]] nb::object get_value(const std::string &key) const override;

        // Override from CompoundScalar
        [[nodiscard]] nb::dict to_dict() const override;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT BackTrace {
        std::optional<BacktraceSignature> signature;
        std::unordered_map<std::string, BackTrace> active_inputs;
        std::unordered_map<std::string, std::string> input_short_values;
        std::unordered_map<std::string, std::string> input_delta_values;
        std::unordered_map<std::string, std::string> input_values;
        std::unordered_map<std::string, engine_time_t> input_last_modified_time;

        explicit BackTrace(
            std::optional<BacktraceSignature> signature_,
            std::unordered_map<std::string, BackTrace> active_inputs_,
            std::unordered_map<std::string, std::string> input_short_values_,
            std::unordered_map<std::string, std::string> input_delta_values_,
            std::unordered_map<std::string, std::string> input_values_,
            std::unordered_map<std::string, engine_time_t> input_last_modified_time_
        );

        BackTrace(const BackTrace &) = default;

        BackTrace &operator=(const BackTrace &) = default;

        BackTrace(BackTrace &&) = default;

        BackTrace &operator=(BackTrace &&) = default;

        [[nodiscard]] std::string arg_str(const std::string &arg_name) const;

        [[nodiscard]] std::string level_str(int64_t level = 0) const;

        [[nodiscard]] std::string to_string() const;

        static std::string runtime_path_name(const Node &node, bool use_label = true);

        static BackTrace capture_back_trace(const Node *node, bool capture_values = false, int64_t depth = 4);

        static void capture_input(std::unordered_map<std::string, BackTrace> &active_inputs,
                                  const TimeSeriesInput &input,
                                  const std::string &input_name, bool capture_values, int64_t depth);
    };

    struct HGRAPH_EXPORT NodeError : CompoundScalar {
        std::string signature_name;
        std::string label;
        std::string wiring_path;
        std::string error_msg;
        std::string stack_trace;
        std::string activation_back_trace;
        std::string additional_context;

        explicit NodeError(std::string signature_name_ = "", std::string label_ = "", std::string wiring_path_ = "",
                           std::string error_msg_ = "", std::string stack_trace_ = "",
                           std::string activation_back_trace_ = "",
                           std::string additional_context_ = "");

        // Override from AbstractSchema
        [[nodiscard]] const std::vector<std::string> &keys() const override;

        [[nodiscard]] nb::object get_value(const std::string &key) const override;

        // Override from CompoundScalar
        [[nodiscard]] nb::dict to_dict() const override;

        [[nodiscard]] std::string to_string() const override;

        friend std::ostream &operator<<(std::ostream &os, const NodeError &error);

        static NodeError capture_error(const std::exception &e, const Node &node, const std::string &msg = "");

        static NodeError capture_error(std::exception_ptr e, const Node &node, const std::string &msg = "");

        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT NodeException : NodeError, std::runtime_error {
        explicit NodeException(const NodeError &error);

        static NodeException capture_error(const std::exception &e, const Node &node, const std::string &msg = "");

        static NodeException capture_error(std::exception_ptr e, const Node &node, const std::string &msg = "");
    };
} // namespace hgraph

#endif  // ERROR_TYPE_H