#include <hgraph/api/python/py_value.h>
#include <hgraph/types/value/visitor.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/type_meta_bindings.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/python/chrono.h>

#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <cctype>
#include <memory>

namespace hgraph
{
    using namespace nanobind::literals;

    namespace
    {
        using namespace hgraph::value;

        template <typename TView> std::vector<View> collect_indexed_views(const TView &view)
        {
            std::vector<View> result;
            result.reserve(view.size());
            for (size_t i = 0; i < view.size(); ++i) { result.push_back(view.at(i)); }
            return result;
        }

        std::vector<View> collect_set_views(const SetView &view)
        {
            std::vector<View> result;
            result.reserve(view.size());
            for (auto elem : view.values()) { result.push_back(elem); }
            return result;
        }

        [[nodiscard]] Value materialize_python_value(const TypeMeta &schema, const nb::object &src)
        {
            Value value{schema};
            value.reset();
            value.from_python(src);
            return value;
        }

        struct KeySetView
        {
            explicit KeySetView(MapView map) : m_map(std::move(map))
            {
                nb::dict items = nb::cast<nb::dict>(m_map.to_python());
                m_keys.reserve(items.size());
                for (auto item : items) {
                    m_keys.push_back(materialize_python_value(*m_map.key_schema(), nb::borrow<nb::object>(item.first)));
                }
            }

            [[nodiscard]] size_t size() const { return m_map.size(); }
            [[nodiscard]] bool empty() const { return m_map.empty(); }
            [[nodiscard]] const TypeMeta *element_type() const { return m_map.key_schema(); }
            [[nodiscard]] const TypeMeta *element_schema() const { return m_map.key_schema(); }
            [[nodiscard]] bool contains(const View &key) const { return m_map.contains(key); }

            [[nodiscard]] std::vector<View> values() const
            {
                std::vector<View> result;
                result.reserve(m_keys.size());
                for (const Value &key : m_keys) {
                    result.push_back(key.view());
                }
                return result;
            }

            MapView            m_map;
            std::vector<Value> m_keys;
        };

        struct PathElement
        {
            enum class Kind
            {
                Field,
                Index,
                Value,
            };

            [[nodiscard]] static PathElement field(std::string name)
            {
                PathElement out;
                out.m_kind = Kind::Field;
                out.m_name = std::move(name);
                return out;
            }

            [[nodiscard]] static PathElement index(size_t index)
            {
                PathElement out;
                out.m_kind = Kind::Index;
                out.m_index = index;
                return out;
            }

            [[nodiscard]] static PathElement key(const View &key)
            {
                PathElement out;
                out.m_kind = Kind::Value;
                out.m_value = std::make_unique<Value>(key.clone());
                return out;
            }

            PathElement() = default;
            PathElement(const PathElement &other)
                : m_kind(other.m_kind), m_name(other.m_name), m_index(other.m_index)
            {
                if (other.m_value != nullptr) { m_value = std::make_unique<Value>(*other.m_value); }
            }

            PathElement &operator=(const PathElement &other)
            {
                if (this == &other) { return *this; }
                m_kind = other.m_kind;
                m_name = other.m_name;
                m_index = other.m_index;
                m_value.reset();
                if (other.m_value != nullptr) { m_value = std::make_unique<Value>(*other.m_value); }
                return *this;
            }

            PathElement(PathElement &&) noexcept = default;
            PathElement &operator=(PathElement &&) noexcept = default;

            [[nodiscard]] bool is_field() const noexcept { return m_kind == Kind::Field; }
            [[nodiscard]] bool is_index() const noexcept { return m_kind == Kind::Index; }
            [[nodiscard]] bool is_value() const noexcept { return m_kind == Kind::Value; }
            [[nodiscard]] std::string_view name() const noexcept { return m_name; }

            [[nodiscard]] size_t get_index() const
            {
                if (!is_index()) { throw std::runtime_error("PathElement does not hold an index"); }
                return m_index;
            }

            [[nodiscard]] View get_value() const
            {
                if (!is_value() || m_value == nullptr) { throw std::runtime_error("PathElement does not hold a value key"); }
                return m_value->view();
            }

          private:
            Kind                   m_kind{Kind::Field};
            std::string            m_name{};
            size_t                 m_index{0};
            std::unique_ptr<Value> m_value{};
        };

        [[nodiscard]] View navigate_impl(View current, const std::vector<PathElement> &path)
        {
            for (const PathElement &element : path) {
                const TypeMeta *schema = current.schema();
                if (schema == nullptr) { throw std::runtime_error("Cannot navigate an invalid view"); }

                if (element.is_field()) {
                    try {
                        if (schema->kind == TypeKind::Bundle) {
                            current = current.as_bundle().field(element.name());
                        } else if (schema->kind == TypeKind::Map) {
                            const TypeMeta *key_schema = current.as_map().key_schema();
                            if (key_schema != scalar_type_meta<std::string>()) {
                                throw std::runtime_error("Field-style access on maps requires a string key schema");
                            }
                            Value key{key_schema};
                            key.reset();
                            key.atomic_view().set(std::string(element.name()));
                            current = current.as_map().at(key.view());
                        } else {
                            throw std::runtime_error("Field access requires a bundle view");
                        }
                    } catch (const std::out_of_range &e) {
                        throw std::runtime_error(e.what());
                    }
                    continue;
                }

                if (element.is_value()) {
                    if (schema->kind != TypeKind::Map) { throw std::runtime_error("Value key access requires a map view"); }
                    try {
                        current = current.as_map().at(element.get_value());
                    } catch (const std::exception &e) {
                        throw std::runtime_error(e.what());
                    }
                    continue;
                }

                const size_t index = element.get_index();
                switch (schema->kind) {
                    case TypeKind::List:
                        current = current.as_list().at(index);
                        break;
                    case TypeKind::Tuple:
                        current = current.as_tuple().at(index);
                        break;
                    case TypeKind::Bundle:
                        current = current.as_bundle().at(index);
                        break;
                    case TypeKind::CyclicBuffer:
                        current = current.as_cyclic_buffer().at(index);
                        break;
                    case TypeKind::Map: {
                        const TypeMeta *key_schema = current.as_map().key_schema();
                        if (key_schema != scalar_type_meta<int64_t>()) {
                            throw std::runtime_error("Numeric bracket access on maps requires an int64 key schema");
                        }
                        Value key{key_schema};
                        key.reset();
                        key.atomic_view().set(static_cast<int64_t>(index));
                        try {
                            current = current.as_map().at(key.view());
                        } catch (const std::exception &e) {
                            throw std::runtime_error(e.what());
                        }
                        break;
                    }
                    default:
                        throw std::runtime_error("Index access is not supported for this view kind");
                }
            }

            return current;
        }

        [[nodiscard]] nb::object visit_python_value(View view, const nb::callable &fn)
        {
            return fn(view.to_python());
        }

        void visit_python_value_void(View view, const nb::callable &fn)
        {
            static_cast<void>(fn(view.to_python()));
        }

        void visit_python_value_mut(View view, const nb::callable &fn)
        {
            nb::object result = fn(view.to_python());
            if (!result.is_none()) { view.from_python(result); }
        }

        [[nodiscard]] nb::object match_python_value(View view, const nb::args &cases)
        {
            nb::object python_value = view.to_python();

            for (const nb::handle &case_handle : cases) {
                nb::tuple case_tuple = nb::cast<nb::tuple>(case_handle);
                if (case_tuple.size() != 2) { throw std::runtime_error("match expects (type, handler) pairs"); }

                nb::handle type_handle = case_tuple[0];
                nb::callable handler = nb::cast<nb::callable>(case_tuple[1]);

                if (type_handle.is_none()) { return handler(python_value); }
                if (nb::isinstance(python_value, type_handle)) { return handler(python_value); }
            }

            throw std::runtime_error("no handler matched");
        }

        using PythonPath = std::vector<nb::object>;

        /**
         * Convert the current traversal path into a plain Python list.
         *
         * The traversal API deliberately exposes paths as simple Python values
         * so callers can inspect, compare, serialize, and feed them back into
         * `navigate(...)` without learning a second custom path abstraction.
         */
        [[nodiscard]] nb::list python_path_list(const PythonPath &path)
        {
            nb::list out;
            for (const nb::object &part : path) { out.append(part); }
            return out;
        }

        /**
         * Recursively visit every scalar leaf reachable from the supplied
         * value-layer view.
         *
         * This utility exists for generic tooling and schema-agnostic
         * algorithms. It centralizes structural recursion over nested values so
         * callers do not need to hand-code traversal for bundles, lists, maps,
         * and sets. Cyclic buffers and queues are treated as leaf values: they
         * are stateful sequence objects rather than structural containers for
         * traversal purposes.
         */
        void deep_visit_impl(View current, PythonPath &path, const nb::callable &callback)
        {
            const TypeMeta *schema = current.schema();
            if (schema == nullptr || !current.has_value()) { return; }

            switch (schema->kind) {
                case TypeKind::Atomic:
                    callback(current, python_path_list(path));
                    return;
                case TypeKind::Tuple: {
                    TupleView tuple = current.as_tuple();
                    for (size_t i = 0; i < tuple.size(); ++i) {
                        path.push_back(nb::int_(i));
                        deep_visit_impl(tuple.at(i), path, callback);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::Bundle: {
                    BundleView bundle = current.as_bundle();
                    for (size_t i = 0; i < bundle.size(); ++i) {
                        path.push_back(nb::str(bundle.schema()->fields[i].name));
                        deep_visit_impl(bundle.at(i), path, callback);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::List: {
                    ListView list = current.as_list();
                    for (size_t i = 0; i < list.size(); ++i) {
                        path.push_back(nb::int_(i));
                        deep_visit_impl(list.at(i), path, callback);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::Set: {
                    for (View element : current.as_set().values()) {
                        path.push_back(element.to_python());
                        deep_visit_impl(element, path, callback);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::Map: {
                    MapView map = current.as_map();
                    nb::dict items = nb::cast<nb::dict>(map.to_python());
                    for (auto item : items) {
                        nb::object key_object = nb::borrow<nb::object>(item.first);
                        Value key = materialize_python_value(*map.key_schema(), key_object);
                        path.push_back(key_object);
                        deep_visit_impl(map.at(key.view()), path, callback);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::CyclicBuffer:
                case TypeKind::Queue:
                    callback(current, python_path_list(path));
                    return;
            }
        }

        [[nodiscard]] size_t count_leaves_impl(View current)
        {
            const TypeMeta *schema = current.schema();
            if (schema == nullptr || !current.has_value()) { return 0; }

            switch (schema->kind) {
                case TypeKind::Atomic:
                    return 1;
                case TypeKind::Tuple: {
                    size_t count = 0;
                    TupleView tuple = current.as_tuple();
                    for (size_t i = 0; i < tuple.size(); ++i) { count += count_leaves_impl(tuple.at(i)); }
                    return count;
                }
                case TypeKind::Bundle: {
                    size_t count = 0;
                    BundleView bundle = current.as_bundle();
                    for (size_t i = 0; i < bundle.size(); ++i) { count += count_leaves_impl(bundle.at(i)); }
                    return count;
                }
                case TypeKind::List: {
                    size_t count = 0;
                    ListView list = current.as_list();
                    for (size_t i = 0; i < list.size(); ++i) { count += count_leaves_impl(list.at(i)); }
                    return count;
                }
                case TypeKind::Set: {
                    size_t count = 0;
                    for (View element : current.as_set().values()) { count += count_leaves_impl(element); }
                    return count;
                }
                case TypeKind::Map: {
                    size_t count = 0;
                    MapView map = current.as_map();
                    nb::dict items = nb::cast<nb::dict>(map.to_python());
                    for (auto item : items) {
                        Value key = materialize_python_value(*map.key_schema(), nb::borrow<nb::object>(item.first));
                        count += count_leaves_impl(map.at(key.view()));
                    }
                    return count;
                }
                case TypeKind::CyclicBuffer:
                case TypeKind::Queue:
                    return 1;
            }
        }

        /**
         * Collect the logical path to every scalar leaf in a nested value.
         *
         * This complements `deep_visit(...)` by providing a lightweight way to
         * discover all reachable leaf paths without consuming the leaf values
         * themselves.
         */
        void collect_leaf_paths_impl(View current, PythonPath &path, nb::list &result)
        {
            const TypeMeta *schema = current.schema();
            if (schema == nullptr || !current.has_value()) { return; }

            switch (schema->kind) {
                case TypeKind::Atomic:
                    result.append(python_path_list(path));
                    return;
                case TypeKind::Tuple: {
                    TupleView tuple = current.as_tuple();
                    for (size_t i = 0; i < tuple.size(); ++i) {
                        path.push_back(nb::int_(i));
                        collect_leaf_paths_impl(tuple.at(i), path, result);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::Bundle: {
                    BundleView bundle = current.as_bundle();
                    for (size_t i = 0; i < bundle.size(); ++i) {
                        path.push_back(nb::str(bundle.schema()->fields[i].name));
                        collect_leaf_paths_impl(bundle.at(i), path, result);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::List: {
                    ListView list = current.as_list();
                    for (size_t i = 0; i < list.size(); ++i) {
                        path.push_back(nb::int_(i));
                        collect_leaf_paths_impl(list.at(i), path, result);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::Set: {
                    for (View element : current.as_set().values()) {
                        path.push_back(element.to_python());
                        collect_leaf_paths_impl(element, path, result);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::Map: {
                    MapView map = current.as_map();
                    nb::dict items = nb::cast<nb::dict>(map.to_python());
                    for (auto item : items) {
                        nb::object key_object = nb::borrow<nb::object>(item.first);
                        Value key = materialize_python_value(*map.key_schema(), key_object);
                        path.push_back(key_object);
                        collect_leaf_paths_impl(map.at(key.view()), path, result);
                        path.pop_back();
                    }
                    return;
                }
                case TypeKind::CyclicBuffer:
                case TypeKind::Queue:
                    result.append(python_path_list(path));
                    return;
            }
        }

        [[nodiscard]] nb::list collect_leaf_paths_impl(View current)
        {
            nb::list result;
            PythonPath path;
            collect_leaf_paths_impl(current, path, result);
            return result;
        }

        [[nodiscard]] std::vector<PathElement> parse_path_impl(const std::string &path)
        {
            std::vector<PathElement> out;
            if (path.empty()) { return out; }
            for (char c : path) {
                if (std::isspace(static_cast<unsigned char>(c))) {
                    throw std::runtime_error("Whitespace is not allowed in value paths");
                }
            }

            size_t i = 0;
            while (i < path.size()) {
                if (path[i] == '.') {
                    throw std::runtime_error("Invalid path syntax");
                }

                if (path[i] == '[') {
                    ++i;
                    if (i >= path.size()) { throw std::runtime_error("Unclosed bracket in path"); }

                    if (path[i] == '"' || path[i] == '\'') {
                        const char quote = path[i++];
                        const size_t start = i;
                        while (i < path.size() && path[i] != quote) { ++i; }
                        if (i >= path.size()) { throw std::runtime_error("Unclosed quoted key in path"); }
                        const std::string key = path.substr(start, i - start);
                        ++i;
                        if (i >= path.size() || path[i] != ']') { throw std::runtime_error("Expected closing bracket in path"); }
                        ++i;
                        out.push_back(PathElement::field(key));
                    } else {
                        const size_t start = i;
                        while (i < path.size() && path[i] != ']') { ++i; }
                        if (i >= path.size()) { throw std::runtime_error("Unclosed bracket in path"); }
                        const std::string token = path.substr(start, i - start);
                        if (token.empty()) { throw std::runtime_error("Empty bracket element in path"); }
                        if (token[0] == '-') { throw std::runtime_error("Negative indices are not supported"); }
                        for (char c : token) {
                            if (!std::isdigit(static_cast<unsigned char>(c))) {
                                throw std::runtime_error("Invalid bracket element in path");
                            }
                        }
                        ++i;
                        out.push_back(PathElement::index(static_cast<size_t>(std::stoull(token))));
                    }
                } else {
                    const size_t start = i;
                    while (i < path.size() && path[i] != '.' && path[i] != '[') { ++i; }
                    if (start == i) { throw std::runtime_error("Invalid path syntax"); }
                    const std::string token = path.substr(start, i - start);
                    if (token.find(']') != std::string::npos) { throw std::runtime_error("Invalid path syntax"); }
                    out.push_back(PathElement::field(token));
                }

                if (i < path.size() && path[i] == '.') {
                    ++i;
                    if (i >= path.size()) { throw std::runtime_error("Trailing dot in path"); }
                }
            }

            return out;
        }

        void register_path_support(nb::module_ &m)
        {
            nb::class_<PathElement>(m, "PathElement")
                .def_static("field", [](const std::string &name) { return PathElement::field(name); }, "name"_a)
                .def_static("index", [](size_t index) { return PathElement::index(index); }, "index"_a)
                .def_static("key", [](const View &key) { return PathElement::key(key); }, "key"_a)
                .def_prop_ro("name", [](const PathElement &self) { return std::string(self.name()); })
                .def("get_index", &PathElement::get_index)
                .def("get_value", &PathElement::get_value)
                .def("is_field", &PathElement::is_field)
                .def("is_index", &PathElement::is_index)
                .def("is_value", &PathElement::is_value);

            m.def("parse_path", &parse_path_impl, "path"_a);
            m.def("navigate",
                  [](View view, const std::vector<PathElement> &path) { return navigate_impl(view, path); },
                  "view"_a,
                  "path"_a);
            m.def("navigate",
                  [](View view, const std::string &path) { return navigate_impl(view, parse_path_impl(path)); },
                  "view"_a,
                  "path"_a);
            m.def("try_navigate",
                  [](View view, const std::vector<PathElement> &path) -> nb::object {
                      try {
                          return nb::cast(navigate_impl(view, path));
                      } catch (...) {
                          return nb::none();
                      }
                  },
                  "view"_a,
                  "path"_a);
            m.def("try_navigate",
                  [](View view, const std::string &path) -> nb::object {
                      try {
                          return nb::cast(navigate_impl(view, parse_path_impl(path)));
                      } catch (...) {
                          return nb::none();
                      }
                  },
                  "view"_a,
                  "path"_a);
            m.def("deep_visit",
                  [](View view, const nb::callable &callback) {
                      PythonPath path;
                      deep_visit_impl(view, path, callback);
                  },
                  "view"_a,
                  "callback"_a,
                  "Recursively visit every scalar leaf in a nested value. "
                  "The callback receives `(leaf_view, path)` where `path` is a "
                  "Python list of field names, indices, or logical keys.");
            m.def("count_leaves",
                  [](View view) { return count_leaves_impl(view); },
                  "view"_a,
                  "Return the number of scalar leaves reachable from a nested value.");
            m.def("collect_leaf_paths",
                  [](View view) { return collect_leaf_paths_impl(view); },
                  "view"_a,
                  "Return the logical path to every scalar leaf in a nested value.");
        }

        void register_type_kind(nb::module_ &m)
        {
            nb::enum_<TypeKind>(m, "TypeKind")
                .value("Atomic", TypeKind::Atomic)
                .value("Tuple", TypeKind::Tuple)
                .value("Bundle", TypeKind::Bundle)
                .value("List", TypeKind::List)
                .value("Set", TypeKind::Set)
                .value("Map", TypeKind::Map)
                .value("CyclicBuffer", TypeKind::CyclicBuffer)
                .value("Queue", TypeKind::Queue);
        }

        void register_bundle_field_info(nb::module_ &m)
        {
            nb::class_<BundleFieldInfo>(m, "BundleFieldInfo")
                .def_prop_ro("name", [](const BundleFieldInfo &self) { return self.name ? nb::str(self.name) : nb::none(); })
                .def_ro("index", &BundleFieldInfo::index)
                .def_ro("offset", &BundleFieldInfo::offset)
                .def_prop_ro("type", [](const BundleFieldInfo &self) { return self.type; }, nb::rv_policy::reference);
        }

        void register_type_meta(nb::module_ &m)
        {
            nb::class_<TypeMeta>(m, "TypeMeta")
                .def_ro("size", &TypeMeta::size)
                .def_ro("alignment", &TypeMeta::alignment)
                .def_ro("kind", &TypeMeta::kind)
                .def_ro("field_count", &TypeMeta::field_count)
                .def_ro("fixed_size", &TypeMeta::fixed_size)
                .def_prop_ro("name", [](const TypeMeta &self) { return self.name ? nb::str(self.name) : nb::none(); })
                .def_prop_ro("element_type", [](const TypeMeta &self) { return self.element_type; }, nb::rv_policy::reference)
                .def_prop_ro("key_type", [](const TypeMeta &self) { return self.key_type; }, nb::rv_policy::reference)
                .def_prop_ro("fields",
                             [](const TypeMeta &self) {
                                 std::vector<const BundleFieldInfo *> result;
                                 for (size_t i = 0; i < self.field_count; ++i) { result.push_back(&self.fields[i]); }
                                 return result;
                             },
                             nb::rv_policy::reference_internal)
                .def("is_fixed_size", &TypeMeta::is_fixed_size)
                .def("is_hashable", &TypeMeta::is_hashable)
                .def("is_comparable", &TypeMeta::is_comparable)
                .def("is_equatable", &TypeMeta::is_equatable)
                .def("is_trivially_copyable", &TypeMeta::is_trivially_copyable)
                .def_static("get", [](const std::string &name) { return TypeMeta::get(name); }, "name"_a, nb::rv_policy::reference);
        }

        void register_scalar_type_meta_functions(nb::module_ &m)
        {
            m.def("scalar_type_meta_int64", []() { return scalar_type_meta<int64_t>(); }, nb::rv_policy::reference);
            m.def("scalar_type_meta_double", []() { return scalar_type_meta<double>(); }, nb::rv_policy::reference);
            m.def("scalar_type_meta_bool", []() { return scalar_type_meta<bool>(); }, nb::rv_policy::reference);
            m.def("scalar_type_meta_string", []() { return scalar_type_meta<std::string>(); }, nb::rv_policy::reference);
            m.def("scalar_type_meta_date", []() { return scalar_type_meta<engine_date_t>(); }, nb::rv_policy::reference);
            m.def("scalar_type_meta_datetime", []() { return scalar_type_meta<engine_time_t>(); }, nb::rv_policy::reference);
            m.def("scalar_type_meta_timedelta",
                  []() { return scalar_type_meta<engine_time_delta_t>(); },
                  nb::rv_policy::reference);
        }

        void register_type_builders(nb::module_ &m)
        {
            nb::class_<TupleBuilder>(m, "TupleBuilder")
                .def(nb::init<>())
                .def("add_element", &TupleBuilder::add_element, "type"_a, nb::rv_policy::reference)
                .def("element", &TupleBuilder::element, "type"_a, nb::rv_policy::reference)
                .def("build", &TupleBuilder::build, nb::rv_policy::reference);

            nb::class_<BundleBuilder>(m, "BundleBuilder")
                .def(nb::init<>())
                .def("set_name", &BundleBuilder::set_name, "name"_a, nb::rv_policy::reference)
                .def("add_field", &BundleBuilder::add_field, "name"_a, "type"_a, nb::rv_policy::reference)
                .def("field", &BundleBuilder::field, "name"_a, "type"_a, nb::rv_policy::reference)
                .def("build", &BundleBuilder::build, nb::rv_policy::reference);

            nb::class_<ListBuilder>(m, "ListBuilder")
                .def(nb::init<>())
                .def("set_element_type", &ListBuilder::set_element_type, "type"_a, nb::rv_policy::reference)
                .def("set_size", &ListBuilder::set_size, "size"_a, nb::rv_policy::reference)
                .def("build", &ListBuilder::build, nb::rv_policy::reference);

            nb::class_<SetBuilder>(m, "SetBuilder")
                .def(nb::init<>())
                .def("set_element_type", &SetBuilder::set_element_type, "type"_a, nb::rv_policy::reference)
                .def("build", &SetBuilder::build, nb::rv_policy::reference);

            nb::class_<MapBuilder>(m, "MapBuilder")
                .def(nb::init<>())
                .def("set_key_type", &MapBuilder::set_key_type, "type"_a, nb::rv_policy::reference)
                .def("set_value_type", &MapBuilder::set_value_type, "type"_a, nb::rv_policy::reference)
                .def("build", &MapBuilder::build, nb::rv_policy::reference);

            nb::class_<CyclicBufferBuilder>(m, "CyclicBufferBuilder")
                .def(nb::init<>())
                .def("set_element_type", &CyclicBufferBuilder::set_element_type, "type"_a, nb::rv_policy::reference)
                .def("set_capacity", &CyclicBufferBuilder::set_capacity, "capacity"_a, nb::rv_policy::reference)
                .def("build", &CyclicBufferBuilder::build, nb::rv_policy::reference);

            nb::class_<QueueBuilder>(m, "QueueBuilder")
                .def(nb::init<>())
                .def("set_element_type", &QueueBuilder::set_element_type, "type"_a, nb::rv_policy::reference)
                .def("max_capacity", &QueueBuilder::max_capacity, "max"_a, nb::rv_policy::reference)
                .def("build", &QueueBuilder::build, nb::rv_policy::reference);
        }

        void register_type_registry(nb::module_ &m)
        {
            nb::class_<TypeRegistry>(m, "TypeRegistry")
                .def_static("instance", &TypeRegistry::instance, nb::rv_policy::reference)
                .def("get_bundle_by_name", &TypeRegistry::get_bundle_by_name, "name"_a, nb::rv_policy::reference)
                .def("has_bundle", &TypeRegistry::has_bundle, "name"_a)
                .def("tuple", &TypeRegistry::tuple)
                .def("bundle", static_cast<BundleBuilder (TypeRegistry::*)()>(&TypeRegistry::bundle))
                .def("bundle", static_cast<BundleBuilder (TypeRegistry::*)(const std::string &)>(&TypeRegistry::bundle), "name"_a)
                .def("list", &TypeRegistry::list, "element_type"_a)
                .def("fixed_list", &TypeRegistry::fixed_list, "element_type"_a, "size"_a)
                .def("set", &TypeRegistry::set, "element_type"_a)
                .def("map", &TypeRegistry::map, "key_type"_a, "value_type"_a)
                .def("cyclic_buffer", &TypeRegistry::cyclic_buffer, "element_type"_a, "capacity"_a)
                .def("queue", &TypeRegistry::queue, "element_type"_a);
        }

        void register_view(nb::module_ &m)
        {
            nb::class_<View>(m, "_View")
                .def("has_value", &View::has_value)
                .def("__bool__", &View::has_value)
                .def_prop_ro("schema", &View::schema, nb::rv_policy::reference)
                .def("is_type", &View::is_type, "schema"_a)
                .def("is_scalar", [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::Atomic; })
                .def("is_tuple", [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::Tuple; })
                .def("is_bundle", [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::Bundle; })
                .def("is_list", [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::List; })
                .def("is_fixed_list",
                     [](const View &self) {
                         return self.schema() != nullptr && self.schema()->kind == TypeKind::List && self.schema()->is_fixed_size();
                     })
                .def("is_set", [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::Set; })
                .def("is_map", [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::Map; })
                .def("is_cyclic_buffer",
                     [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::CyclicBuffer; })
                .def("is_queue", [](const View &self) { return self.schema() != nullptr && self.schema()->kind == TypeKind::Queue; })
                .def("to_string", &View::to_string)
                .def("to_python", &View::to_python)
                .def("from_python",
                     [](View &self, nb::handle src) { self.from_python(nb::borrow<nb::object>(src)); },
                     "src"_a)
                .def("hash", &View::hash)
                .def("equals", &View::equals, "other"_a)
                .def("clone", &View::clone)
                .def("copy_from", &View::copy_from, "other"_a)
                .def("visit", [](View self, const nb::callable &fn) { return visit_python_value(self, fn); }, "fn"_a)
                .def("visit_void",
                     [](View self, const nb::callable &fn) { visit_python_value_void(self, fn); },
                     "fn"_a)
                .def("visit_mut",
                     [](View self, const nb::callable &fn) { visit_python_value_mut(self, fn); },
                     "fn"_a)
                .def("match", [](View self, const nb::args &cases) { return match_python_value(self, cases); })
                .def("as_int", [](const View &self) { return self.checked_as<int64_t>(); })
                .def("as_double", [](const View &self) { return self.checked_as<double>(); })
                .def("as_bool", [](const View &self) { return self.checked_as<bool>(); })
                .def("as_string", [](const View &self) { return self.checked_as<std::string>(); })
                .def("set_int", [](View &self, int64_t value) { self.as_atomic().set(value); }, "value"_a)
                .def("set_double", [](View &self, double value) { self.as_atomic().set(value); }, "value"_a)
                .def("set_bool", [](View &self, bool value) { self.as_atomic().set(value); }, "value"_a)
                .def("set_string", [](View &self, const std::string &value) { self.as_atomic().set(value); }, "value"_a)
                .def("try_as_int",
                     [](const View &self) -> std::optional<int64_t> {
                         const auto *ptr = self.try_as<int64_t>();
                         if (ptr != nullptr) { return *ptr; }
                         return std::nullopt;
                     })
                .def("try_as_double",
                     [](const View &self) -> std::optional<double> {
                         const auto *ptr = self.try_as<double>();
                         if (ptr != nullptr) { return *ptr; }
                         return std::nullopt;
                     })
                .def("try_as_bool",
                     [](const View &self) -> std::optional<bool> {
                         const auto *ptr = self.try_as<bool>();
                         if (ptr != nullptr) { return *ptr; }
                         return std::nullopt;
                     })
                .def("try_as_string",
                     [](const View &self) -> std::optional<std::string> {
                         const auto *ptr = self.try_as<std::string>();
                         if (ptr != nullptr) { return *ptr; }
                         return std::nullopt;
                     })
                .def("as_atomic", [](View &self) { return self.as_atomic(); })
                .def("as_tuple", [](View &self) { return self.as_tuple(); })
                .def("as_bundle", [](View &self) { return self.as_bundle(); })
                .def("as_list", [](View &self) { return self.as_list(); })
                .def("as_set", [](View &self) { return self.as_set(); })
                .def("as_map", [](View &self) { return self.as_map(); })
                .def("as_cyclic_buffer", [](View &self) { return self.as_cyclic_buffer(); })
                .def("as_queue", [](View &self) { return self.as_queue(); })
                .def("try_as_tuple",
                     [](View &self) -> nb::object {
                         const auto *schema = self.schema();
                         if (schema == nullptr) { return nb::none(); }
                         if (schema->kind != TypeKind::Tuple && schema->kind != TypeKind::Bundle) { return nb::none(); }
                         return nb::cast(self.as_tuple());
                     })
                .def("try_as_bundle",
                     [](View &self) -> nb::object {
                         const auto *schema = self.schema();
                         if (schema == nullptr || schema->kind != TypeKind::Bundle) { return nb::none(); }
                         return nb::cast(self.as_bundle());
                     })
                .def("try_as_list",
                     [](View &self) -> nb::object {
                         const auto *schema = self.schema();
                         if (schema == nullptr || schema->kind != TypeKind::List) { return nb::none(); }
                         return nb::cast(self.as_list());
                     })
                .def("try_as_set",
                     [](View &self) -> nb::object {
                         const auto *schema = self.schema();
                         if (schema == nullptr || schema->kind != TypeKind::Set) { return nb::none(); }
                         return nb::cast(self.as_set());
                     })
                .def("try_as_map",
                     [](View &self) -> nb::object {
                         const auto *schema = self.schema();
                         if (schema == nullptr || schema->kind != TypeKind::Map) { return nb::none(); }
                         return nb::cast(self.as_map());
                     })
                .def("try_as_cyclic_buffer",
                     [](View &self) -> nb::object {
                         const auto *schema = self.schema();
                         if (schema == nullptr || schema->kind != TypeKind::CyclicBuffer) { return nb::none(); }
                         return nb::cast(self.as_cyclic_buffer());
                     })
                .def("try_as_queue",
                     [](View &self) -> nb::object {
                         const auto *schema = self.schema();
                         if (schema == nullptr || schema->kind != TypeKind::Queue) { return nb::none(); }
                         return nb::cast(self.as_queue());
                     })
                .def("navigate", [](View self, const std::string &path) { return navigate_impl(self, parse_path_impl(path)); }, "path"_a)
                .def("navigate",
                     [](View self, const std::vector<PathElement> &path) { return navigate_impl(self, path); },
                     "path"_a)
                .def("navigate_mut",
                     [](View self, const std::string &path) { return navigate_impl(self, parse_path_impl(path)); },
                     "path"_a)
                .def("navigate_mut",
                     [](View self, const std::vector<PathElement> &path) { return navigate_impl(self, path); },
                     "path"_a)
                .def("try_navigate",
                     [](View self, const std::string &path) -> nb::object {
                         try {
                             return nb::cast(navigate_impl(self, parse_path_impl(path)));
                         } catch (...) {
                             return nb::none();
                         }
                     },
                     "path"_a)
                .def("try_navigate",
                     [](View self, const std::vector<PathElement> &path) -> nb::object {
                         try {
                             return nb::cast(navigate_impl(self, path));
                         } catch (...) {
                             return nb::none();
                         }
                     },
                     "path"_a)
                .def("__repr__", [](const View &self) {
                    return self.has_value() ? std::string("View(") + self.to_string() + ")" : std::string("View(invalid)");
                });
        }

        void register_atomic_view(nb::module_ &m)
        {
            nb::class_<AtomicView, View>(m, "AtomicView")
                .def("set_int", [](AtomicView &self, int64_t value) { self.set(value); })
                .def("set_double", [](AtomicView &self, double value) { self.set(value); })
                .def("set_bool", [](AtomicView &self, bool value) { self.set(value); })
                .def("set_string", [](AtomicView &self, const std::string &value) { self.set(value); });
        }

        void register_tuple_view(nb::module_ &m)
        {
            nb::class_<TupleView, View>(m, "TupleView")
                .def("size", &TupleView::size)
                .def("__len__", &TupleView::size)
                .def("empty", &TupleView::empty)
                .def("element_type", [](const TupleView &self, size_t index) { return self.at(index).schema(); }, "index"_a, nb::rv_policy::reference)
                .def("at", static_cast<View (TupleView::*)(size_t)>(&TupleView::at), "index"_a)
                .def("__getitem__", [](TupleView &self, size_t index) { return self.at(index); })
                .def("set",
                     [](TupleView &self, size_t index, const Value &value) {
                         auto mutation = self.begin_mutation();
                         mutation.set(index, value.view());
                     },
                     "index"_a,
                     "value"_a)
                .def("set",
                     [](TupleView &self, size_t index, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.at(index).schema(), value);
                         mutation.set(index, wrapped.view());
                     },
                     "index"_a,
                     "value"_a)
                .def("__iter__", [](const TupleView &self) { return nb::iter(nb::cast(collect_indexed_views(self))); });
        }

        void register_bundle_view(nb::module_ &m)
        {
            nb::class_<BundleView, TupleView>(m, "BundleView")
                .def("has_field", &BundleView::has_field, "name"_a)
                .def("is_field", &BundleView::has_field, "name"_a)
                .def("at_name",
                     [](BundleView &self, std::string_view name) {
                         try {
                             return self.field(name);
                         } catch (const std::out_of_range &e) {
                             throw std::runtime_error(e.what());
                         }
                     },
                     "name"_a)
                .def("at_name_mut",
                     [](BundleView &self, std::string_view name) {
                         try {
                             return self.field(name);
                         } catch (const std::out_of_range &e) {
                             throw std::runtime_error(e.what());
                         }
                     },
                     "name"_a)
                .def("field",
                     [](BundleView &self, std::string_view name) {
                         try {
                             return self.field(name);
                         } catch (const std::out_of_range &e) {
                             throw std::runtime_error(e.what());
                         }
                     },
                     "name"_a)
                .def("field_index",
                     [](const BundleView &self, std::string_view name) {
                         const TypeMeta *schema = self.schema();
                         if (schema == nullptr || schema->fields == nullptr) {
                             throw std::runtime_error("BundleView::field_index on invalid view");
                         }
                         for (size_t i = 0; i < schema->field_count; ++i) {
                             if (schema->fields[i].name != nullptr && name == schema->fields[i].name) { return i; }
                         }
                         throw std::out_of_range("BundleView::field_index unknown name");
                     },
                     "name"_a)
                .def("set",
                     [](BundleView &self, std::string_view name, const Value &value) {
                         auto mutation = self.begin_mutation();
                         mutation.set_field(name, value.view());
                     },
                     "name"_a,
                     "value"_a)
                .def("set",
                     [](BundleView &self, std::string_view name, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.field(name).schema(), value);
                         mutation.set_field(name, wrapped.view());
                     },
                     "name"_a,
                     "value"_a)
                .def("__getitem__", [](BundleView &self, const std::string &name) {
                    try {
                        return self.field(name);
                    } catch (const std::out_of_range &e) {
                        throw std::runtime_error(e.what());
                    }
                });
        }

        void register_list_view(nb::module_ &m)
        {
            nb::class_<ListView, View>(m, "ListView")
                .def("size", &ListView::size)
                .def("__len__", &ListView::size)
                .def("empty", &ListView::empty)
                .def("is_fixed", &ListView::is_fixed)
                .def("is_fixed_list", &ListView::is_fixed)
                .def("element_schema", &ListView::element_schema, nb::rv_policy::reference)
                .def("element_type", &ListView::element_schema, nb::rv_policy::reference)
                .def("is_buffer_compatible",
                     [](const ListView &self) {
                         const TypeMeta *elem = self.element_schema();
                         return elem != nullptr && elem->kind == TypeKind::Atomic && elem->is_buffer_compatible();
                     })
                .def("at", static_cast<View (ListView::*)(size_t)>(&ListView::at), "index"_a)
                .def("__getitem__", [](ListView &self, size_t index) { return self.at(index); })
                .def("front", static_cast<View (ListView::*)()>(&ListView::front))
                .def("back", static_cast<View (ListView::*)()>(&ListView::back))
                .def("__iter__", [](const ListView &self) { return nb::iter(nb::cast(collect_indexed_views(self))); })
                .def("to_numpy", [](ListView &self) -> nb::object {
                    const TypeMeta *elem = self.element_schema();
                    if (elem == nullptr || elem->kind != TypeKind::Atomic || !elem->is_buffer_compatible()) {
                        throw std::runtime_error("List element type not buffer compatible for numpy");
                    }

                    nb::module_ np = nb::module_::import_("numpy");
                    const size_t n = self.size();
                    if (n == 0) {
                        const char *dtype = "int64";
                        if (elem == scalar_type_meta<double>()) { dtype = "float64"; }
                        else if (elem == scalar_type_meta<bool>()) { dtype = "bool"; }
                        return np.attr("array")(nb::list(), "dtype"_a = dtype);
                    }

                    nb::module_ ctypes = nb::module_::import_("ctypes");
                    if (elem == scalar_type_meta<int64_t>()) {
                        auto &first = self.at(0).checked_as<int64_t>();
                        nb::object array_type = ctypes.attr("c_int64").attr("__mul__")(n);
                        nb::object ptr = ctypes.attr("cast")(
                            ctypes.attr("c_void_p")(reinterpret_cast<uintptr_t>(std::addressof(first))),
                            ctypes.attr("POINTER")(array_type));
                        return np.attr("ctypeslib").attr("as_array")(ptr.attr("contents"));
                    }
                    if (elem == scalar_type_meta<double>()) {
                        auto &first = self.at(0).checked_as<double>();
                        nb::object array_type = ctypes.attr("c_double").attr("__mul__")(n);
                        nb::object ptr = ctypes.attr("cast")(
                            ctypes.attr("c_void_p")(reinterpret_cast<uintptr_t>(std::addressof(first))),
                            ctypes.attr("POINTER")(array_type));
                        return np.attr("ctypeslib").attr("as_array")(ptr.attr("contents"));
                    }
                    if (elem == scalar_type_meta<bool>()) {
                        auto &first = self.at(0).checked_as<bool>();
                        nb::object array_type = ctypes.attr("c_bool").attr("__mul__")(n);
                        nb::object ptr = ctypes.attr("cast")(
                            ctypes.attr("c_void_p")(reinterpret_cast<uintptr_t>(std::addressof(first))),
                            ctypes.attr("POINTER")(array_type));
                        return np.attr("ctypeslib").attr("as_array")(ptr.attr("contents"));
                    }
                    throw std::runtime_error("Unsupported list element type for numpy conversion");
                })
                .def("set",
                     [](ListView &self, size_t index, const View &value) {
                         auto mutation = self.begin_mutation();
                         mutation.set(index, value);
                     },
                     "index"_a,
                     "value"_a)
                .def("set",
                     [](ListView &self, size_t index, const Value &value) {
                         auto mutation = self.begin_mutation();
                         mutation.set(index, value.view());
                     },
                     "index"_a,
                     "value"_a)
                .def("set",
                     [](ListView &self, size_t index, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         mutation.set(index, wrapped.view());
                     },
                     "index"_a,
                     "value"_a)
                .def("push_back",
                     [](ListView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         mutation.push_back(value);
                     },
                     "value"_a)
                .def("push_back",
                     [](ListView &self, const Value &value) {
                         auto mutation = self.begin_mutation();
                         mutation.push_back(value.view());
                     },
                     "value"_a)
                .def("push_back",
                     [](ListView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         mutation.push_back(wrapped.view());
                     },
                     "value"_a)
                .def("append",
                     [](ListView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         mutation.push_back(value);
                     },
                     "value"_a)
                .def("append",
                     [](ListView &self, const Value &value) {
                         auto mutation = self.begin_mutation();
                         mutation.push_back(value.view());
                     },
                     "value"_a)
                .def("append",
                     [](ListView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         mutation.push_back(wrapped.view());
                     },
                     "value"_a)
                .def("pop_back", [](ListView &self) {
                    if (self.is_fixed()) { throw std::runtime_error("ListView::pop_back on fixed-size list"); }
                    if (self.empty()) { throw std::out_of_range("ListView::pop_back on empty list"); }
                    auto mutation = self.begin_mutation();
                    mutation.resize(self.size() - 1);
                })
                .def("resize",
                     [](ListView &self, size_t new_size) {
                         auto mutation = self.begin_mutation();
                         mutation.resize(new_size);
                     },
                     "new_size"_a)
                .def("reserve",
                     [](ListView &self, size_t capacity) {
                         auto mutation = self.begin_mutation();
                         mutation.reserve(capacity);
                     },
                     "capacity"_a)
                .def("clear", [](ListView &self) {
                    auto mutation = self.begin_mutation();
                    mutation.clear();
                })
                .def("reset",
                     [](ListView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         for (size_t i = 0; i < self.size(); ++i) { mutation.set(i, value); }
                     },
                     "value"_a);
        }

        void register_set_view(nb::module_ &m)
        {
            nb::class_<SetView, View>(m, "SetView")
                .def("size", &SetView::size)
                .def("__len__", &SetView::size)
                .def("empty", &SetView::empty)
                .def("element_schema", &SetView::element_schema, nb::rv_policy::reference)
                .def("element_type", &SetView::element_schema, nb::rv_policy::reference)
                .def("contains", &SetView::contains, "value"_a)
                .def("__contains__", &SetView::contains, "value"_a)
                .def("__iter__", [](const SetView &self) { return nb::iter(nb::cast(collect_set_views(self))); })
                .def("add",
                     [](SetView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         return mutation.add(value);
                     },
                     "value"_a)
                .def("add",
                     [](SetView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         return mutation.add(wrapped.view());
                     },
                     "value"_a)
                .def("insert",
                     [](SetView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         return mutation.add(value);
                     },
                     "value"_a)
                .def("insert",
                     [](SetView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         return mutation.add(wrapped.view());
                     },
                     "value"_a)
                .def("remove",
                     [](SetView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         return mutation.remove(value);
                     },
                     "value"_a)
                .def("remove",
                     [](SetView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         return mutation.remove(wrapped.view());
                     },
                     "value"_a)
                .def("erase",
                     [](SetView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         return mutation.remove(value);
                     },
                     "value"_a)
                .def("erase",
                     [](SetView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         return mutation.remove(wrapped.view());
                     },
                     "value"_a)
                .def("reserve",
                     [](SetView &self, size_t capacity) {
                         auto mutation = self.begin_mutation();
                         mutation.reserve(capacity);
                     },
                     "capacity"_a)
                .def("clear", [](SetView &self) {
                    auto mutation = self.begin_mutation();
                    mutation.clear();
                });
        }

        void register_key_set_view(nb::module_ &m)
        {
            nb::class_<KeySetView>(m, "KeySetView")
                .def("size", &KeySetView::size)
                .def("__len__", &KeySetView::size)
                .def("empty", &KeySetView::empty)
                .def("contains", &KeySetView::contains, "value"_a)
                .def("__contains__", &KeySetView::contains, "value"_a)
                .def("element_type", &KeySetView::element_type, nb::rv_policy::reference)
                .def("element_schema", &KeySetView::element_schema, nb::rv_policy::reference)
                .def("__iter__", [](const KeySetView &self) { return nb::iter(nb::cast(self.values())); });
        }

        void register_map_view(nb::module_ &m)
        {
            nb::class_<MapView, View>(m, "MapView")
                .def("size", &MapView::size)
                .def("__len__", &MapView::size)
                .def("empty", &MapView::empty)
                .def("key_schema", &MapView::key_schema, nb::rv_policy::reference)
                .def("value_schema", &MapView::value_schema, nb::rv_policy::reference)
                .def("key_type", &MapView::key_schema, nb::rv_policy::reference)
                .def("value_type", &MapView::value_schema, nb::rv_policy::reference)
                .def("contains", &MapView::contains, "key"_a)
                .def("__contains__", &MapView::contains, "key"_a)
                .def("at", static_cast<View (MapView::*)(const View &)>(&MapView::at), "key"_a)
                .def("__getitem__", [](MapView &self, const View &key) { return self.at(key); })
                .def("keys", [](MapView &self) { return KeySetView(self); })
                .def("set",
                     [](MapView &self, const View &key, const View &value) {
                         auto mutation = self.begin_mutation();
                         static_cast<void>(mutation.set(key, value));
                     },
                     "key"_a,
                     "value"_a)
                .def("set",
                     [](MapView &self, const nb::object &key, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped_key = materialize_python_value(*self.key_schema(), key);
                         Value wrapped_value = materialize_python_value(*self.value_schema(), value);
                         static_cast<void>(mutation.set(wrapped_key.view(), wrapped_value.view()));
                     },
                     "key"_a,
                     "value"_a)
                .def("add",
                     [](MapView &self, const View &key, const View &value) {
                         const bool existed = self.contains(key);
                         if (!existed) {
                             auto mutation = self.begin_mutation();
                             mutation.set(key, value);
                         }
                         return !existed;
                     },
                     "key"_a,
                     "value"_a)
                .def("add",
                     [](MapView &self, const nb::object &key, const nb::object &value) {
                         Value wrapped_key = materialize_python_value(*self.key_schema(), key);
                         const bool existed = self.contains(wrapped_key.view());
                         if (!existed) {
                             auto mutation = self.begin_mutation();
                             Value wrapped_value = materialize_python_value(*self.value_schema(), value);
                             mutation.set(wrapped_key.view(), wrapped_value.view());
                         }
                         return !existed;
                     },
                     "key"_a,
                     "value"_a)
                .def("remove",
                     [](MapView &self, const View &key) {
                         auto mutation = self.begin_mutation();
                         return mutation.remove(key);
                     },
                     "key"_a)
                .def("remove",
                     [](MapView &self, const nb::object &key) {
                         auto mutation = self.begin_mutation();
                         Value wrapped_key = materialize_python_value(*self.key_schema(), key);
                         return mutation.remove(wrapped_key.view());
                     },
                     "key"_a)
                .def("erase",
                     [](MapView &self, const View &key) {
                         auto mutation = self.begin_mutation();
                         return mutation.remove(key);
                     },
                     "key"_a)
                .def("erase",
                     [](MapView &self, const nb::object &key) {
                         auto mutation = self.begin_mutation();
                         Value wrapped_key = materialize_python_value(*self.key_schema(), key);
                         return mutation.remove(wrapped_key.view());
                     },
                     "key"_a)
                .def("reserve",
                     [](MapView &self, size_t capacity) {
                         auto mutation = self.begin_mutation();
                         mutation.reserve(capacity);
                     },
                     "capacity"_a)
                .def("clear", [](MapView &self) {
                    auto mutation = self.begin_mutation();
                    mutation.clear();
                })
                .def("items", [](MapView &self) {
                    nb::dict d = nb::cast<nb::dict>(self.to_python());
                    std::vector<std::pair<nb::object, nb::object>> result;
                    result.reserve(d.size());
                    for (auto item : d) { result.emplace_back(nb::borrow(item.first), nb::borrow(item.second)); }
                    return result;
                });
        }

        void register_sequence_views(nb::module_ &m)
        {
            nb::class_<CyclicBufferView, View>(m, "CyclicBufferView")
                .def("size", &CyclicBufferView::size)
                .def("__len__", &CyclicBufferView::size)
                .def("empty", &CyclicBufferView::empty)
                .def("element_schema", &CyclicBufferView::element_schema, nb::rv_policy::reference)
                .def("element_type", &CyclicBufferView::element_schema, nb::rv_policy::reference)
                .def("is_buffer_compatible",
                     [](const CyclicBufferView &self) {
                         const TypeMeta *elem = self.element_schema();
                         return elem != nullptr && elem->kind == TypeKind::Atomic && elem->is_buffer_compatible();
                     })
                .def("front", static_cast<View (CyclicBufferView::*)()>(&CyclicBufferView::front))
                .def("back", static_cast<View (CyclicBufferView::*)()>(&CyclicBufferView::back))
                .def("capacity", &CyclicBufferView::capacity)
                .def("full", &CyclicBufferView::full)
                .def("at", static_cast<View (CyclicBufferView::*)(size_t)>(&CyclicBufferView::at), "index"_a)
                .def("__getitem__", [](CyclicBufferView &self, size_t index) { return self.at(index); })
                .def("to_numpy", [](const CyclicBufferView &self) -> nb::object {
                    const TypeMeta *elem = self.element_schema();
                    if (elem == nullptr || elem->kind != TypeKind::Atomic || !elem->is_buffer_compatible()) {
                        throw std::runtime_error("CyclicBuffer element type not buffer compatible for numpy");
                    }

                    nb::module_ np = nb::module_::import_("numpy");
                    const size_t n = self.size();
                    if (elem == scalar_type_meta<int64_t>()) {
                        auto arr = np.attr("empty")(n, "dtype"_a = "int64");
                        auto *ptr =
                            reinterpret_cast<int64_t *>(nb::cast<intptr_t>(arr.attr("ctypes").attr("data")));
                        for (size_t i = 0; i < n; ++i) { ptr[i] = self.at(i).checked_as<int64_t>(); }
                        return arr;
                    }
                    if (elem == scalar_type_meta<double>()) {
                        auto arr = np.attr("empty")(n, "dtype"_a = "float64");
                        auto *ptr =
                            reinterpret_cast<double *>(nb::cast<intptr_t>(arr.attr("ctypes").attr("data")));
                        for (size_t i = 0; i < n; ++i) { ptr[i] = self.at(i).checked_as<double>(); }
                        return arr;
                    }
                    if (elem == scalar_type_meta<bool>()) {
                        auto arr = np.attr("empty")(n, "dtype"_a = "bool");
                        auto *ptr = reinterpret_cast<bool *>(nb::cast<intptr_t>(arr.attr("ctypes").attr("data")));
                        for (size_t i = 0; i < n; ++i) { ptr[i] = self.at(i).checked_as<bool>(); }
                        return arr;
                    }
                    throw std::runtime_error("Unsupported cyclic buffer element type for numpy conversion");
                })
                .def("push",
                     [](CyclicBufferView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         mutation.push(value);
                     },
                     "value"_a)
                .def("push",
                     [](CyclicBufferView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         mutation.push(wrapped.view());
                     },
                     "value"_a)
                .def("clear", [](CyclicBufferView &self) {
                    auto mutation = self.begin_mutation();
                    mutation.clear();
                });

            nb::class_<QueueView, View>(m, "QueueView")
                .def("size", &QueueView::size)
                .def("__len__", &QueueView::size)
                .def("empty", &QueueView::empty)
                .def("element_schema", &QueueView::element_schema, nb::rv_policy::reference)
                .def("element_type", &QueueView::element_schema, nb::rv_policy::reference)
                .def("front", static_cast<View (QueueView::*)()>(&QueueView::front))
                .def("back", static_cast<View (QueueView::*)()>(&QueueView::back))
                .def("__iter__", [](QueueView &self) { return nb::iter(self.to_python()); })
                .def("max_capacity", &QueueView::max_capacity)
                .def("has_max_capacity", &QueueView::has_max_capacity)
                .def("push",
                     [](QueueView &self, const View &value) {
                         auto mutation = self.begin_mutation();
                         mutation.push(value);
                     },
                     "value"_a)
                .def("push",
                     [](QueueView &self, const nb::object &value) {
                         auto mutation = self.begin_mutation();
                         Value wrapped = materialize_python_value(*self.element_schema(), value);
                         mutation.push(wrapped.view());
                     },
                     "value"_a)
                .def("pop", [](QueueView &self) {
                    auto mutation = self.begin_mutation();
                    mutation.pop();
                })
                .def("reserve",
                     [](QueueView &self, size_t capacity) {
                         auto mutation = self.begin_mutation();
                         mutation.reserve(capacity);
                     },
                     "capacity"_a)
                .def("clear", [](QueueView &self) {
                    auto mutation = self.begin_mutation();
                    mutation.clear();
                });
        }

        void register_value(nb::module_ &m)
        {
            nb::class_<Value>(m, "Value")
                .def(nb::init<const TypeMeta *>(), "schema"_a)
                .def("__init__",
                     [](Value *self, bool value) { new (self) Value(hgraph::value_for<bool>(std::move(value))); },
                     "value"_a)
                .def("__init__",
                     [](Value *self, int64_t value) { new (self) Value(hgraph::value_for<int64_t>(std::move(value))); },
                     "value"_a)
                .def("__init__",
                     [](Value *self, double value) { new (self) Value(hgraph::value_for<double>(std::move(value))); },
                     "value"_a)
                .def("__init__",
                     [](Value *self, std::string value) { new (self) Value(hgraph::value_for<std::string>(std::move(value))); },
                     "value"_a)
                .def("has_value", &Value::has_value)
                .def("__bool__", &Value::has_value)
                .def_prop_ro("schema", &Value::schema, nb::rv_policy::reference)
                .def("view", static_cast<View (Value::*)()>(&Value::view))
                .def("as_int", [](const Value &self) { return self.view().checked_as<int64_t>(); })
                .def("as_double", [](const Value &self) { return self.view().checked_as<double>(); })
                .def("as_bool", [](const Value &self) { return self.view().checked_as<bool>(); })
                .def("as_string", [](const Value &self) { return self.view().checked_as<std::string>(); })
                .def("set_int", [](Value &self, int64_t value) { self.atomic_view().set(value); }, "value"_a)
                .def("set_double", [](Value &self, double value) { self.atomic_view().set(value); }, "value"_a)
                .def("set_bool", [](Value &self, bool value) { self.atomic_view().set(value); }, "value"_a)
                .def("set_string", [](Value &self, const std::string &value) { self.atomic_view().set(value); }, "value"_a)
                .def("tuple_view", static_cast<TupleView (Value::*)()>(&Value::tuple_view))
                .def("bundle_view", static_cast<BundleView (Value::*)()>(&Value::bundle_view))
                .def("list_view", static_cast<ListView (Value::*)()>(&Value::list_view))
                .def("set_view", static_cast<SetView (Value::*)()>(&Value::set_view))
                .def("map_view", static_cast<MapView (Value::*)()>(&Value::map_view))
                .def("cyclic_buffer_view", static_cast<CyclicBufferView (Value::*)()>(&Value::cyclic_buffer_view))
                .def("queue_view", static_cast<QueueView (Value::*)()>(&Value::queue_view))
                .def("to_python", &Value::to_python)
                .def("from_python",
                     [](Value &self, nb::handle src) { self.from_python(nb::borrow<nb::object>(src)); },
                     "src"_a)
                .def("hash", &Value::hash)
                .def("equals", static_cast<bool (Value::*)(const Value &) const>(&Value::equals), "other"_a)
                .def("equals", static_cast<bool (Value::*)(const View &) const>(&Value::equals), "other"_a)
                .def("to_string", &Value::to_string)
                .def("reset", &Value::reset)
                .def("navigate", [](Value &self, const std::string &path) { return navigate_impl(self.view(), parse_path_impl(path)); }, "path"_a)
                .def("navigate",
                     [](Value &self, const std::vector<PathElement> &path) { return navigate_impl(self.view(), path); },
                     "path"_a)
                .def("navigate_mut",
                     [](Value &self, const std::string &path) { return navigate_impl(self.view(), parse_path_impl(path)); },
                     "path"_a)
                .def("navigate_mut",
                     [](Value &self, const std::vector<PathElement> &path) { return navigate_impl(self.view(), path); },
                     "path"_a)
                .def("try_navigate",
                     [](Value &self, const std::string &path) -> nb::object {
                         try {
                             return nb::cast(navigate_impl(self.view(), parse_path_impl(path)));
                         } catch (...) {
                             return nb::none();
                         }
                     },
                     "path"_a)
                .def("try_navigate",
                     [](Value &self, const std::vector<PathElement> &path) -> nb::object {
                         try {
                             return nb::cast(navigate_impl(self.view(), path));
                         } catch (...) {
                             return nb::none();
                         }
                     },
                     "path"_a)
                .def("__repr__", [](const Value &self) {
                    return self.has_value() ? std::string("Value(") + self.to_string() + ")" : std::string("Value(invalid)");
                });
        }
    }  // namespace

    void value_register_with_nanobind(nb::module_ &m)
    {
        nb::module_ value_mod = m.def_submodule("value");

        register_type_kind(value_mod);
        register_bundle_field_info(value_mod);
        register_type_meta(value_mod);
        register_type_meta_bindings(value_mod);
        register_scalar_type_meta_functions(value_mod);
        register_type_builders(value_mod);
        register_type_registry(value_mod);
        register_view(value_mod);
        register_atomic_view(value_mod);
        register_tuple_view(value_mod);
        register_bundle_view(value_mod);
        register_list_view(value_mod);
        register_set_view(value_mod);
        register_key_set_view(value_mod);
        register_map_view(value_mod);
        register_sequence_views(value_mod);
        register_value(value_mod);
        register_path_support(value_mod);

        m.attr("TypeKind") = value_mod.attr("TypeKind");
        m.attr("TypeMeta") = value_mod.attr("TypeMeta");
        m.attr("TypeRegistry") = value_mod.attr("TypeRegistry");
        m.attr("Value") = value_mod.attr("Value");
        m.attr("View") = value_mod.attr("_View");
        m.attr("ValueView") = value_mod.attr("_View");
        m.attr("AtomicView") = value_mod.attr("AtomicView");
        m.attr("TupleView") = value_mod.attr("TupleView");
        m.attr("BundleView") = value_mod.attr("BundleView");
        m.attr("ListView") = value_mod.attr("ListView");
        m.attr("SetView") = value_mod.attr("SetView");
        m.attr("MapView") = value_mod.attr("MapView");
        m.attr("CyclicBufferView") = value_mod.attr("CyclicBufferView");
        m.attr("QueueView") = value_mod.attr("QueueView");
    }
}  // namespace hgraph
