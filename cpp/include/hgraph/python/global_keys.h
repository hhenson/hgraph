#ifndef HGRAPH_GLOBAL_KEYS_H
#define HGRAPH_GLOBAL_KEYS_H

#include <nanobind/nanobind.h>
#include <string>
#include <vector>

namespace nb = nanobind;

namespace hgraph {
    namespace keys {
        // Key helpers for internal C++ use
        inline std::string output_key(const std::string &path) { return path; }
        inline std::string output_subscriber_key(const std::string &path) { return path + "_subscriber"; }

        std::string context_output_key(const std::vector<int64_t> &owning_graph_id, const std::string &path);

        inline std::string component_key(const std::string &id_or_label) {
            return std::string("component::") + id_or_label;
        }

        inline std::string component_key(int64_t id_or_label) {
            return std::string("component::") + std::to_string(id_or_label);
        }
    } // namespace keys

    /**
 * OutputKeyBuilder exposed to Python to implement the hgraph OutputKeyBuilder protocol.
 * Provides the same key-building functions as the Python DefaultOutputKeyBuilder.
 */
    class OutputKeyBuilder {
    public:
        // Register with nanobind
        static void register_with_nanobind(nb::module_ &m);

        // Methods matching the Python protocol
        std::string output_key(const std::string &path) const { return keys::output_key(path); }
        std::string output_subscriber_key(const std::string &path) const { return keys::output_subscriber_key(path); }

        // Accept a Python tuple for owning_graph_id to match the Python API exactly
        std::string context_output_key(const nb::tuple &owning_graph_id, const std::string &path) const;

        // Accept int or str for component key
        std::string component_key(const nb::object &id_or_label) const;
    };
} // namespace hgraph

#endif // HGRAPH_GLOBAL_KEYS_H