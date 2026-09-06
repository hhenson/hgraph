#include "descriptor/module_descriptor_reader.h"

#include <hgl/native_module_abi.h>

#include "syntax/temporal.h"

#include <simdjson.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgl::descriptor
{
    namespace
    {
        using Element = simdjson::dom::element;

        struct ObjectFields
        {
            std::vector<std::pair<std::string, Element>> values{};

            [[nodiscard]] const Element *find(std::string_view name) const noexcept {
                const auto found = std::ranges::find(values, name, &std::pair<std::string, Element>::first);
                return found == values.end() ? nullptr : &found->second;
            }
        };

        [[nodiscard]] std::string member_path(std::string_view path, std::string_view name) {
            std::string result{path};
            result += '.';
            result += name;
            return result;
        }

        [[nodiscard]] std::string index_path(std::string_view path, std::size_t index) {
            std::string result{path};
            result += '[';
            result += std::to_string(index);
            result += ']';
            return result;
        }

        [[nodiscard]] bool known_scalar_name(std::string_view name) noexcept {
            using ir::hir::ScalarType;
            for (std::uint8_t value = static_cast<std::uint8_t>(ScalarType::Bool);
                 value <= static_cast<std::uint8_t>(ScalarType::TimeZone); ++value) {
                if (ir::hir::scalar_type_name(static_cast<ScalarType>(value)) == name) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool known_unary_operator(std::string_view spelling) noexcept { return spelling == "-" || spelling == "!"; }

        [[nodiscard]] bool known_binary_operator(std::string_view spelling) noexcept {
            using ir::hir::BinaryOp;
            for (std::uint8_t value = static_cast<std::uint8_t>(BinaryOp::Mul); value <= static_cast<std::uint8_t>(BinaryOp::Or);
                 ++value) {
                if (ir::hir::binary_op_spelling(static_cast<BinaryOp>(value)) == spelling) { return true; }
            }
            return false;
        }

        [[nodiscard]] constexpr bool cpp_identifier_start(char value) noexcept {
            return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || value == '_';
        }

        [[nodiscard]] constexpr bool cpp_identifier_continue(char value) noexcept {
            return cpp_identifier_start(value) || (value >= '0' && value <= '9');
        }

        /// Native symbols become tokens in generated C++, so descriptors may
        /// name one exact qualified identifier but may not carry expressions,
        /// calls, templates, operators, whitespace, or preprocessor text.
        [[nodiscard]] bool exact_cpp_symbol(std::string_view value) noexcept {
            if (value.starts_with("::")) { value.remove_prefix(2U); }
            if (value.empty()) { return false; }
            while (true) {
                if (!cpp_identifier_start(value.front())) { return false; }
                std::size_t length = 1U;
                while (length < value.size() && cpp_identifier_continue(value[length])) { ++length; }
                value.remove_prefix(length);
                if (value.empty()) { return true; }
                if (!value.starts_with("::")) { return false; }
                value.remove_prefix(2U);
                if (value.empty()) { return false; }
            }
        }

        [[nodiscard]] std::optional<ReadError> find_duplicate_member(Element value, std::string_view path) {
            if (value.is_object()) {
                simdjson::dom::object object_value;
                if (value.get(object_value)) { return ReadError{std::string{path}, "expected object"}; }
                std::vector<std::string> names;
                for (const simdjson::dom::key_value_pair field : object_value) {
                    const std::string name{field.key};
                    const std::string field_path = member_path(path, name);
                    if (std::ranges::find(names, name) != names.end()) { return ReadError{field_path, "duplicate member"}; }
                    names.push_back(name);
                    if (std::optional<ReadError> nested = find_duplicate_member(field.value, field_path)) { return nested; }
                }
            } else if (value.is_array()) {
                simdjson::dom::array array;
                if (value.get(array)) { return ReadError{std::string{path}, "expected array"}; }
                std::size_t index = 0;
                for (Element item : array) {
                    if (std::optional<ReadError> nested = find_duplicate_member(item, index_path(path, index))) { return nested; }
                    ++index;
                }
            }
            return std::nullopt;
        }

        class Decoder
        {
          public:
            [[nodiscard]] ReadResult run(Element root) {
                ModuleDescriptor descriptor;
                ObjectFields     document;
                if (!object(root, "$", document)) { return failure(); }

                std::string format;
                if (!required_string(document, "format", "$", format)) { return failure(); }
                if (format != "hgl.module") {
                    fail("$.format", "expected 'hgl.module'");
                    return failure();
                }
                if (!required_u32(document, "format_version", "$", descriptor.format_version)) { return failure(); }
                if (descriptor.format_version != module_descriptor_format_version) {
                    fail("$.format_version", "unsupported descriptor format version " + std::to_string(descriptor.format_version));
                    return failure();
                }

                const Element *module = required(document, "module", "$");
                if (module == nullptr || !read_module(*module, descriptor)) { return failure(); }
                const Element *interface = required(document, "interface", "$");
                if (interface == nullptr || !read_interface(*interface, descriptor.interface)) { return failure(); }
                const Element *provider = required(document, "provider", "$");
                if (provider == nullptr || !read_provider(*provider, descriptor)) { return failure(); }
                const Element *schema = required(document, "schema", "$");
                if (schema == nullptr || !read_schema(*schema, descriptor)) { return failure(); }
                if (const Element *native = document.find("native"); native != nullptr && !read_native(*native, descriptor)) {
                    return failure();
                }
                const Element *build = required(document, "build", "$");
                if (build == nullptr || !read_build(*build, descriptor.build)) { return failure(); }

                if (std::optional<ReadError> invalid = validate(descriptor)) {
                    error_ = std::move(invalid);
                    return failure();
                }
                return ReadResult{.value = std::move(descriptor)};
            }

          private:
            [[nodiscard]] ReadResult failure() { return ReadResult{.error = std::move(error_)}; }

            bool fail(std::string path, std::string message) {
                if (!error_) { error_ = ReadError{std::move(path), std::move(message)}; }
                return false;
            }

            bool object(Element value, std::string_view path, ObjectFields &out) {
                simdjson::dom::object object_value;
                if (value.get(object_value)) { return fail(std::string{path}, "expected object"); }
                for (const simdjson::dom::key_value_pair field : object_value) {
                    const std::string key{field.key};
                    if (out.find(key) != nullptr) { return fail(member_path(path, key), "duplicate member"); }
                    out.values.emplace_back(key, field.value);
                }
                return true;
            }

            [[nodiscard]] const Element *required(const ObjectFields &fields, std::string_view name, std::string_view path) {
                const Element *value = fields.find(name);
                if (value == nullptr) { fail(member_path(path, name), "missing required member"); }
                return value;
            }

            bool string(Element value, std::string_view path, std::string &out) {
                std::string_view text;
                if (value.get(text)) { return fail(std::string{path}, "expected string"); }
                out.assign(text);
                return true;
            }

            bool boolean(Element value, std::string_view path, bool &out) {
                if (value.get(out)) { return fail(std::string{path}, "expected boolean"); }
                return true;
            }

            bool u32(Element value, std::string_view path, std::uint32_t &out) {
                std::uint64_t number{};
                if (value.get(number) || number > std::numeric_limits<std::uint32_t>::max()) {
                    return fail(std::string{path}, "expected unsigned 32-bit integer");
                }
                out = static_cast<std::uint32_t>(number);
                return true;
            }

            bool reference(Element value, std::string_view path, SchemaId &out) {
                if (value.is_null()) {
                    out = no_schema_id;
                    return true;
                }
                return u32(value, path, out);
            }

            bool required_string(const ObjectFields &fields, std::string_view name, std::string_view path, std::string &out) {
                const Element *value = required(fields, name, path);
                return value != nullptr && string(*value, member_path(path, name), out);
            }

            bool optional_string(const ObjectFields &fields, std::string_view name, std::string_view path, std::string &out) {
                const Element *value = fields.find(name);
                return value == nullptr || string(*value, member_path(path, name), out);
            }

            bool nullable_string(Element value, std::string_view path, std::string &out) {
                if (value.is_null()) {
                    out.clear();
                    return true;
                }
                return string(value, path, out);
            }

            bool required_bool(const ObjectFields &fields, std::string_view name, std::string_view path, bool &out) {
                const Element *value = required(fields, name, path);
                return value != nullptr && boolean(*value, member_path(path, name), out);
            }

            bool optional_bool(const ObjectFields &fields, std::string_view name, std::string_view path, bool &out) {
                const Element *value = fields.find(name);
                return value == nullptr || boolean(*value, member_path(path, name), out);
            }

            bool required_u32(const ObjectFields &fields, std::string_view name, std::string_view path, std::uint32_t &out) {
                const Element *value = required(fields, name, path);
                return value != nullptr && u32(*value, member_path(path, name), out);
            }

            bool required_reference(const ObjectFields &fields, std::string_view name, std::string_view path, SchemaId &out) {
                const Element *value = required(fields, name, path);
                return value != nullptr && reference(*value, member_path(path, name), out);
            }

            bool optional_reference(const ObjectFields &fields, std::string_view name, std::string_view path, SchemaId &out) {
                const Element *value = fields.find(name);
                return value == nullptr || reference(*value, member_path(path, name), out);
            }

            template <typename Enum>
            bool enum_value(Element value, std::string_view path, std::initializer_list<std::pair<std::string_view, Enum>> choices,
                            Enum &out) {
                std::string text;
                if (!string(value, path, text)) { return false; }
                for (const auto &[name, choice] : choices) {
                    if (text == name) {
                        out = choice;
                        return true;
                    }
                }
                return fail(std::string{path}, "unknown value '" + text + "'");
            }

            template <typename Enum>
            bool enum_array(Element value, std::string_view path, std::initializer_list<std::pair<std::string_view, Enum>> choices,
                            std::vector<Enum> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    Enum decoded{};
                    if (!enum_value(item, index_path(path, index), choices, decoded)) { return false; }
                    out.push_back(decoded);
                    ++index;
                }
                return true;
            }

            bool string_array(Element value, std::string_view path, std::vector<std::string> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    std::string decoded;
                    if (!string(item, index_path(path, index), decoded)) { return false; }
                    out.push_back(std::move(decoded));
                    ++index;
                }
                return true;
            }

            bool reference_array(Element value, std::string_view path, std::vector<SchemaId> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    SchemaId decoded{};
                    if (!reference(item, index_path(path, index), decoded)) { return false; }
                    out.push_back(decoded);
                    ++index;
                }
                return true;
            }

            bool required_string_array(const ObjectFields &fields, std::string_view name, std::string_view path,
                                       std::vector<std::string> &out) {
                const Element *value = required(fields, name, path);
                return value != nullptr && string_array(*value, member_path(path, name), out);
            }

            bool read_module(Element value, ModuleDescriptor &out) {
                ObjectFields fields;
                return object(value, "$.module", fields) && required_string(fields, "identity", "$.module", out.module_identity) &&
                       required_string(fields, "language_version", "$.module", out.language_version) &&
                       optional_string(fields, "descriptor_fingerprint", "$.module", out.descriptor_fingerprint);
            }

            bool read_generic_parameters(Element value, std::string_view path, std::vector<GenericParameter> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path(path, index);
                    ObjectFields      fields;
                    GenericParameter  parameter;
                    std::string       kind;
                    if (!object(item, item_path, fields) || !required_string(fields, "name", item_path, parameter.name) ||
                        !required_string(fields, "kind", item_path, kind) ||
                        !required_string(fields, "binding", item_path, parameter.binding_identity) ||
                        !required_reference(fields, "type", item_path, parameter.type)) {
                        return false;
                    }
                    if (kind == "const") {
                        parameter.is_const = true;
                    } else if (kind != "type") {
                        return fail(member_path(item_path, "kind"), "unknown value '" + kind + "'");
                    }
                    out.push_back(std::move(parameter));
                    ++index;
                }
                return true;
            }

            bool read_parameters(Element value, std::string_view path, std::vector<Parameter> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path(path, index);
                    ObjectFields      fields;
                    Parameter         parameter;
                    std::string       kind;
                    if (!object(item, item_path, fields) || !required_string(fields, "name", item_path, parameter.name) ||
                        !required_string(fields, "kind", item_path, kind) ||
                        !required_string(fields, "binding", item_path, parameter.binding_identity) ||
                        !required_reference(fields, "type", item_path, parameter.type) ||
                        !required_reference(fields, "default", item_path, parameter.default_value)) {
                        return false;
                    }
                    if (kind == "const") {
                        parameter.is_const = true;
                    } else if (kind != "signal") {
                        return fail(member_path(item_path, "kind"), "unknown value '" + kind + "'");
                    }
                    out.push_back(std::move(parameter));
                    ++index;
                }
                return true;
            }

            bool read_signature(Element value, std::string_view path, Signature &out) {
                ObjectFields fields;
                if (!object(value, path, fields)) { return false; }
                const Element *generics   = required(fields, "generic_parameters", path);
                const Element *parameters = required(fields, "parameters", path);
                return generics != nullptr && parameters != nullptr &&
                       read_generic_parameters(*generics, member_path(path, "generic_parameters"), out.generics) &&
                       read_parameters(*parameters, member_path(path, "parameters"), out.parameters) &&
                       required_reference(fields, "result", path, out.result) &&
                       required_reference(fields, "requires", path, out.requirements);
            }

            bool read_fields(Element value, std::string_view path, std::vector<StructField> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path(path, index);
                    ObjectFields      fields;
                    StructField       field;
                    if (!object(item, item_path, fields) || !required_string(fields, "name", item_path, field.name) ||
                        !required_reference(fields, "type", item_path, field.type) ||
                        !required_bool(fields, "optional", item_path, field.optional) ||
                        !required_reference(fields, "default", item_path, field.default_value) ||
                        !required_string(fields, "origin", item_path, field.origin_identity)) {
                        return false;
                    }
                    out.push_back(std::move(field));
                    ++index;
                }
                return true;
            }

            bool read_interface(Element value, std::vector<InterfaceDeclaration> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail("$.interface", "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string    item_path = index_path("$.interface", index);
                    ObjectFields         fields;
                    InterfaceDeclaration declaration;
                    const Element       *category{};
                    if (!object(item, item_path, fields) || (category = required(fields, "category", item_path)) == nullptr ||
                        !enum_value(*category, member_path(item_path, "category"),
                                    {{"structure", DeclarationCategory::Structure},
                                     {"operator", DeclarationCategory::Operator},
                                     {"function", DeclarationCategory::Function}},
                                    declaration.category) ||
                        !required_string(fields, "identity", item_path, declaration.identity) ||
                        !optional_string(fields, "registry_name", item_path, declaration.registry_name)) {
                        return false;
                    }
                    if (const Element *execution = fields.find("execution")) {
                        if (!enum_value(*execution, member_path(item_path, "execution"),
                                        {{"none", ExecutionKind::None},
                                         {"composition", ExecutionKind::Composition},
                                         {"runtime-node", ExecutionKind::RuntimeNode}},
                                        declaration.execution)) {
                            return false;
                        }
                    }
                    if (declaration.category == DeclarationCategory::Structure) {
                        const Element *generics = required(fields, "generic_parameters", item_path);
                        const Element *parents  = required(fields, "parents", item_path);
                        const Element *members  = required(fields, "fields", item_path);
                        if (generics == nullptr || parents == nullptr || members == nullptr ||
                            !required_bool(fields, "abstract", item_path, declaration.abstract) ||
                            !read_generic_parameters(*generics, member_path(item_path, "generic_parameters"),
                                                     declaration.signature.generics) ||
                            !reference_array(*parents, member_path(item_path, "parents"), declaration.parents) ||
                            !required_reference(fields, "requires", item_path, declaration.signature.requirements) ||
                            !read_fields(*members, member_path(item_path, "fields"), declaration.fields)) {
                            return false;
                        }
                    } else {
                        const Element *signature = required(fields, "signature", item_path);
                        if (signature == nullptr ||
                            !read_signature(*signature, member_path(item_path, "signature"), declaration.signature)) {
                            return false;
                        }
                    }
                    out.push_back(std::move(declaration));
                    ++index;
                }
                return true;
            }

            bool read_implementations(Element value, std::string_view path, std::vector<Implementation> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path(path, index);
                    ObjectFields      fields;
                    Implementation    implementation;
                    const Element    *execution{};
                    const Element    *signature{};
                    if (!object(item, item_path, fields) ||
                        !required_string(fields, "identity", item_path, implementation.identity) ||
                        !required_string(fields, "operator", item_path, implementation.operator_identity) ||
                        !required_string(fields, "registry_name", item_path, implementation.operator_registry_name) ||
                        (execution = required(fields, "execution", item_path)) == nullptr ||
                        !enum_value(*execution, member_path(item_path, "execution"),
                                    {{"none", ExecutionKind::None},
                                     {"composition", ExecutionKind::Composition},
                                     {"runtime-node", ExecutionKind::RuntimeNode}},
                                    implementation.execution) ||
                        (signature = required(fields, "signature", item_path)) == nullptr ||
                        !read_signature(*signature, member_path(item_path, "signature"), implementation.signature)) {
                        return false;
                    }
                    out.push_back(std::move(implementation));
                    ++index;
                }
                return true;
            }

            bool read_provider(Element value, ModuleDescriptor &out) {
                ObjectFields fields;
                if (!object(value, "$.provider", fields) ||
                    !required_string(fields, "identity", "$.provider", out.provider_identity)) {
                    return false;
                }
                const Element *implementations = required(fields, "implementations", "$.provider");
                const Element *requirements    = required(fields, "requires", "$.provider");
                return implementations != nullptr && requirements != nullptr &&
                       read_implementations(*implementations, "$.provider.implementations", out.implementations) &&
                       string_array(*requirements, "$.provider.requires", out.provider_requirements);
            }

            bool read_type_arguments(Element value, std::string_view path, std::vector<TypeArgument> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path(path, index);
                    ObjectFields      fields;
                    TypeArgument      argument;
                    const Element    *kind{};
                    if (!object(item, item_path, fields) || (kind = required(fields, "kind", item_path)) == nullptr ||
                        !enum_value(*kind, member_path(item_path, "kind"),
                                    {{"type", TypeArgumentCategory::Type}, {"constant", TypeArgumentCategory::Constant}},
                                    argument.category) ||
                        !required_reference(fields, "reference", item_path, argument.reference)) {
                        return false;
                    }
                    out.push_back(argument);
                    ++index;
                }
                return true;
            }

            bool read_types(Element value, std::vector<TypeRecord> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail("$.schema.types", "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path("$.schema.types", index);
                    ObjectFields      fields;
                    TypeRecord        record;
                    std::uint32_t     id{};
                    const Element    *kind{};
                    if (!object(item, item_path, fields) || !required_u32(fields, "id", item_path, id) ||
                        static_cast<std::size_t>(id) != index || (kind = required(fields, "kind", item_path)) == nullptr ||
                        !enum_value(*kind, member_path(item_path, "kind"),
                                    {{"void", TypeCategory::Void},
                                     {"scalar", TypeCategory::Scalar},
                                     {"symbol", TypeCategory::Symbol},
                                     {"tuple", TypeCategory::Tuple},
                                     {"list", TypeCategory::List},
                                     {"set", TypeCategory::Set},
                                     {"map", TypeCategory::Map},
                                     {"rolling", TypeCategory::Rolling},
                                     {"atomic", TypeCategory::Atomic},
                                     {"ref", TypeCategory::Reference},
                                     {"iterator", TypeCategory::Iterator},
                                     {"callable", TypeCategory::Callable},
                                     {"capability", TypeCategory::Capability},
                                     {"harness-sequence", TypeCategory::HarnessSequence},
                                     {"deferred", TypeCategory::Deferred}},
                                    record.category) ||
                        !optional_string(fields, "name", item_path, record.scalar_name) ||
                        !optional_string(fields, "identity", item_path, record.nominal_identity) ||
                        !optional_string(fields, "binding", item_path, record.binding_identity) ||
                        !optional_reference(fields, "size", item_path, record.size) ||
                        !optional_reference(fields, "min_size", item_path, record.min_size) ||
                        !optional_bool(fields, "unbounded", item_path, record.unbounded)) {
                        if (!error_ && static_cast<std::size_t>(id) != index) {
                            fail(member_path(item_path, "id"), "record id does not match array index");
                        }
                        return false;
                    }
                    if (const Element *children = fields.find("children")) {
                        if (!reference_array(*children, member_path(item_path, "children"), record.children)) { return false; }
                    }
                    if (const Element *arguments = fields.find("arguments")) {
                        if (!read_type_arguments(*arguments, member_path(item_path, "arguments"), record.arguments)) {
                            return false;
                        }
                    }
                    out.push_back(std::move(record));
                    ++index;
                }
                return true;
            }

            bool read_literal(Element value, std::string_view path, ir::hir::Constant &out) {
                ObjectFields fields;
                std::string  kind;
                if (!object(value, path, fields) || !required_string(fields, "kind", path, kind)) { return false; }
                const Element *payload = fields.find("value");
                if (kind == "null") {
                    out = ir::hir::NullValue{};
                    return true;
                }
                if (kind == "placeholder") {
                    out = ir::hir::PlaceholderValue{};
                    return true;
                }
                if (payload == nullptr) { return fail(member_path(path, "value"), "missing required member"); }
                if (kind == "bool") {
                    bool decoded{};
                    if (!boolean(*payload, member_path(path, "value"), decoded)) { return false; }
                    out = decoded;
                    return true;
                }
                std::string text;
                if (!string(*payload, member_path(path, "value"), text)) { return false; }
                if (kind == "str") {
                    out = std::move(text);
                    return true;
                }
                if (kind == "i64") {
                    std::int64_t decoded{};
                    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), decoded);
                    if (error != std::errc{} || end != text.data() + text.size()) {
                        return fail(member_path(path, "value"), "invalid i64 literal");
                    }
                    out = decoded;
                    return true;
                }
                if (kind == "f64") {
                    double decoded{};
                    if (text == "nan") {
                        decoded = std::numeric_limits<double>::quiet_NaN();
                    } else if (text == "inf") {
                        decoded = std::numeric_limits<double>::infinity();
                    } else if (text == "-inf") {
                        decoded = -std::numeric_limits<double>::infinity();
                    } else {
                        const auto [end, error] =
                            std::from_chars(text.data(), text.data() + text.size(), decoded, std::chars_format::general);
                        if (error != std::errc{} || end != text.data() + text.size() || !std::isfinite(decoded)) {
                            return fail(member_path(path, "value"), "invalid f64 literal");
                        }
                    }
                    out = decoded;
                    return true;
                }
                syntax::TemporalParseResult temporal = syntax::parse_temporal_literal(text);
                if (!temporal.value || syntax::temporal_kind_name(temporal.value->kind) != kind) {
                    return fail(member_path(path, "kind"), "unknown or mismatched literal kind '" + kind + "'");
                }
                out = std::move(*temporal.value);
                return true;
            }

            bool read_constant_elements(Element value, std::string_view path, std::vector<ConstantElement> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path(path, index);
                    ObjectFields      fields;
                    ConstantElement   element;
                    if (!object(item, item_path, fields) || !optional_reference(fields, "key", item_path, element.key) ||
                        !required_reference(fields, "value", item_path, element.value)) {
                        return false;
                    }
                    out.push_back(element);
                    ++index;
                }
                return true;
            }

            bool read_constant_arguments(Element value, std::string_view path, std::vector<ConstantArgument> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path(path, index);
                    ObjectFields      fields;
                    ConstantArgument  argument;
                    if (!object(item, item_path, fields) || !required_string(fields, "name", item_path, argument.name) ||
                        !required_reference(fields, "value", item_path, argument.value)) {
                        return false;
                    }
                    out.push_back(std::move(argument));
                    ++index;
                }
                return true;
            }

            bool read_constants(Element value, std::vector<ConstantExpressionRecord> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail("$.schema.constant_expressions", "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string        item_path = index_path("$.schema.constant_expressions", index);
                    ObjectFields             fields;
                    ConstantExpressionRecord record;
                    std::uint32_t            id{};
                    const Element           *kind{};
                    if (!object(item, item_path, fields) || !required_u32(fields, "id", item_path, id) ||
                        static_cast<std::size_t>(id) != index || (kind = required(fields, "kind", item_path)) == nullptr ||
                        !enum_value(*kind, member_path(item_path, "kind"),
                                    {{"literal", ConstantExpressionCategory::Literal},
                                     {"parameter", ConstantExpressionCategory::Parameter},
                                     {"unary", ConstantExpressionCategory::Unary},
                                     {"binary", ConstantExpressionCategory::Binary},
                                     {"index", ConstantExpressionCategory::Index},
                                     {"field", ConstantExpressionCategory::Field},
                                     {"sequence", ConstantExpressionCategory::Sequence},
                                     {"tuple", ConstantExpressionCategory::Tuple},
                                     {"construct", ConstantExpressionCategory::Construct}},
                                    record.category) ||
                        !optional_string(fields, "parameter", item_path, record.parameter_identity) ||
                        !optional_string(fields, "operator", item_path, record.operator_spelling) ||
                        !optional_reference(fields, "lhs", item_path, record.lhs) ||
                        !optional_reference(fields, "rhs", item_path, record.rhs) ||
                        !optional_string(fields, "member", item_path, record.member) ||
                        !optional_reference(fields, "type", item_path, record.constructed_type) ||
                        !optional_bool(fields, "delta", item_path, record.delta)) {
                        if (!error_ && static_cast<std::size_t>(id) != index) {
                            fail(member_path(item_path, "id"), "record id does not match array index");
                        }
                        return false;
                    }
                    if (const Element *literal = fields.find("literal")) {
                        ir::hir::Constant decoded;
                        if (!read_literal(*literal, member_path(item_path, "literal"), decoded)) { return false; }
                        record.literal = std::move(decoded);
                    }
                    if (const Element *elements = fields.find("elements")) {
                        if (!read_constant_elements(*elements, member_path(item_path, "elements"), record.elements)) {
                            return false;
                        }
                    }
                    if (const Element *items = fields.find("items")) {
                        if (!reference_array(*items, member_path(item_path, "items"), record.items)) { return false; }
                    }
                    if (const Element *arguments = fields.find("arguments")) {
                        if (!read_constant_arguments(*arguments, member_path(item_path, "arguments"), record.arguments)) {
                            return false;
                        }
                    }
                    out.push_back(std::move(record));
                    ++index;
                }
                return true;
            }

            bool read_constraints(Element value, std::vector<ConstraintRecord> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail("$.schema.constraints", "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path("$.schema.constraints", index);
                    ObjectFields      fields;
                    ConstraintRecord  record;
                    std::uint32_t     id{};
                    const Element    *kind{};
                    if (!object(item, item_path, fields) || !required_u32(fields, "id", item_path, id) ||
                        static_cast<std::size_t>(id) != index || (kind = required(fields, "kind", item_path)) == nullptr ||
                        !enum_value(*kind, member_path(item_path, "kind"),
                                    {{"symbol", ConstraintCategory::Symbol},
                                     {"type", ConstraintCategory::Type},
                                     {"value", ConstraintCategory::Value},
                                     {"set", ConstraintCategory::Set},
                                     {"call", ConstraintCategory::Call},
                                     {"operator", ConstraintCategory::Operator},
                                     {"relation", ConstraintCategory::Relation},
                                     {"not", ConstraintCategory::Not},
                                     {"logic", ConstraintCategory::Logic}},
                                    record.category) ||
                        !optional_string(fields, "identity", item_path, record.identity) ||
                        !optional_string(fields, "registry_name", item_path, record.registry_name) ||
                        !optional_string(fields, "operator", item_path, record.operator_spelling) ||
                        !optional_string(fields, "category", item_path, record.relation_category) ||
                        !optional_reference(fields, "type", item_path, record.type) ||
                        !optional_reference(fields, "value", item_path, record.value) ||
                        !optional_reference(fields, "lhs", item_path, record.lhs) ||
                        !optional_reference(fields, "rhs", item_path, record.rhs) ||
                        !optional_reference(fields, "operand", item_path, record.operand) ||
                        !optional_reference(fields, "result", item_path, record.result)) {
                        if (!error_ && static_cast<std::size_t>(id) != index) {
                            fail(member_path(item_path, "id"), "record id does not match array index");
                        }
                        return false;
                    }
                    if (const Element *elements = fields.find("elements")) {
                        if (!reference_array(*elements, member_path(item_path, "elements"), record.elements)) { return false; }
                    }
                    if (const Element *arguments = fields.find("arguments")) {
                        if (!reference_array(*arguments, member_path(item_path, "arguments"), record.arguments)) { return false; }
                    }
                    out.push_back(std::move(record));
                    ++index;
                }
                return true;
            }

            bool read_schema(Element value, ModuleDescriptor &out) {
                ObjectFields fields;
                if (!object(value, "$.schema", fields)) { return false; }
                const Element *types       = required(fields, "types", "$.schema");
                const Element *constants   = required(fields, "constant_expressions", "$.schema");
                const Element *constraints = required(fields, "constraints", "$.schema");
                return types != nullptr && constants != nullptr && constraints != nullptr && read_types(*types, out.types) &&
                       read_constants(*constants, out.constant_expressions) && read_constraints(*constraints, out.constraints);
            }

            bool read_native_value_policy(Element value, std::string_view path, NativeValuePolicy &out) {
                ObjectFields   fields;
                const Element *ownership{};
                const Element *dependent_on{};
                return object(value, path, fields) && (ownership = required(fields, "ownership", path)) != nullptr &&
                       enum_value(*ownership, member_path(path, "ownership"),
                                  {{"value", NativeOwnership::Value},
                                   {"owned", NativeOwnership::Owned},
                                   {"shared", NativeOwnership::Shared},
                                   {"borrowed", NativeOwnership::Borrowed}},
                                  out.ownership) &&
                       (dependent_on = required(fields, "dependent_on", path)) != nullptr &&
                       nullable_string(*dependent_on, member_path(path, "dependent_on"), out.dependent_on) &&
                       required_bool(fields, "mutable", path, out.mutable_value);
            }

            bool read_native_types(Element value, std::vector<NativeTypeDeclaration> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail("$.native.types", "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string     item_path = index_path("$.native.types", index);
                    ObjectFields          fields;
                    NativeTypeDeclaration declaration;
                    const Element        *category{};
                    if (!object(item, item_path, fields) || (category = required(fields, "category", item_path)) == nullptr ||
                        !enum_value(
                            *category, member_path(item_path, "category"),
                            {{"opaque-state", NativeTypeCategory::OpaqueState}, {"atomic-value", NativeTypeCategory::AtomicValue}},
                            declaration.category) ||
                        !required_string(fields, "identity", item_path, declaration.identity) ||
                        !required_string(fields, "cpp_type", item_path, declaration.cpp_type) ||
                        !required_string(fields, "public_header", item_path, declaration.public_header)) {
                        return false;
                    }
                    out.push_back(std::move(declaration));
                    ++index;
                }
                return true;
            }

            bool read_native_parameter_policies(Element value, std::string_view path, std::vector<NativeParameterPolicy> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail(std::string{path}, "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string     item_path = index_path(path, index);
                    ObjectFields          fields;
                    NativeParameterPolicy parameter;
                    const Element        *policy{};
                    if (!object(item, item_path, fields) || !required_string(fields, "name", item_path, parameter.name) ||
                        (policy = required(fields, "value", item_path)) == nullptr ||
                        !read_native_value_policy(*policy, member_path(item_path, "value"), parameter.value)) {
                        return false;
                    }
                    out.push_back(std::move(parameter));
                    ++index;
                }
                return true;
            }

            bool read_native_declarations(Element value, std::vector<NativeDeclaration> &out) {
                simdjson::dom::array array;
                if (value.get(array)) { return fail("$.native.declarations", "expected array"); }
                std::size_t index = 0;
                for (Element item : array) {
                    const std::string item_path = index_path("$.native.declarations", index);
                    ObjectFields      fields;
                    NativeDeclaration declaration;
                    const Element    *category{};
                    const Element    *signature_value{};
                    const Element    *phases{};
                    const Element    *effects{};
                    const Element    *parameters{};
                    const Element    *result{};
                    const Element    *exception{};
                    const Element    *thread_safety{};
                    if (!object(item, item_path, fields) || (category = required(fields, "category", item_path)) == nullptr ||
                        !enum_value(*category, member_path(item_path, "category"),
                                    {{"function", NativeDeclarationCategory::Function},
                                     {"constructor", NativeDeclarationCategory::Constructor},
                                     {"lifecycle", NativeDeclarationCategory::Lifecycle}},
                                    declaration.category) ||
                        !required_string(fields, "identity", item_path, declaration.identity) ||
                        !required_string(fields, "cpp_symbol", item_path, declaration.cpp_symbol) ||
                        (signature_value = required(fields, "signature", item_path)) == nullptr ||
                        !read_signature(*signature_value, member_path(item_path, "signature"), declaration.signature) ||
                        (phases = required(fields, "phases", item_path)) == nullptr ||
                        !enum_array(*phases, member_path(item_path, "phases"),
                                    {{"wiring", NativePhase::Wiring},
                                     {"start", NativePhase::Start},
                                     {"evaluation", NativePhase::Evaluation},
                                     {"stop", NativePhase::Stop}},
                                    declaration.phases) ||
                        (effects = required(fields, "effects", item_path)) == nullptr ||
                        !enum_array(*effects, member_path(item_path, "effects"),
                                    {{"mutation", NativeEffect::Mutation},
                                     {"io", NativeEffect::InputOutput},
                                     {"blocking", NativeEffect::Blocking},
                                     {"allocation", NativeEffect::Allocation}},
                                    declaration.effects) ||
                        (parameters = required(fields, "parameters", item_path)) == nullptr ||
                        !read_native_parameter_policies(*parameters, member_path(item_path, "parameters"),
                                                        declaration.parameters) ||
                        (result = required(fields, "result", item_path)) == nullptr ||
                        !read_native_value_policy(*result, member_path(item_path, "result"), declaration.result) ||
                        (exception = required(fields, "exception", item_path)) == nullptr ||
                        !enum_value(
                            *exception, member_path(item_path, "exception"),
                            {{"noexcept", NativeExceptionPolicy::NoThrow}, {"translated", NativeExceptionPolicy::Translated}},
                            declaration.exception_policy) ||
                        (thread_safety = required(fields, "thread_safety", item_path)) == nullptr ||
                        !enum_value(*thread_safety, member_path(item_path, "thread_safety"),
                                    {{"node-local", NativeThreadSafety::NodeLocal},
                                     {"thread-safe", NativeThreadSafety::ThreadSafe},
                                     {"serialized", NativeThreadSafety::Serialized}},
                                    declaration.thread_safety)) {
                        return false;
                    }
                    out.push_back(std::move(declaration));
                    ++index;
                }
                return true;
            }

            bool read_native(Element value, ModuleDescriptor &out) {
                ObjectFields fields;
                if (!object(value, "$.native", fields)) { return false; }
                const Element *types        = required(fields, "types", "$.native");
                const Element *declarations = required(fields, "declarations", "$.native");
                return types != nullptr && declarations != nullptr && read_native_types(*types, out.native_types) &&
                       read_native_declarations(*declarations, out.native_declarations);
            }

            bool read_build(Element value, BuildMetadata &out) {
                ObjectFields fields;
                if (!object(value, "$.build", fields) ||
                    !required_string_array(fields, "public_headers", "$.build", out.public_headers) ||
                    !required_string_array(fields, "cmake_packages", "$.build", out.cmake_packages) ||
                    !required_string_array(fields, "imported_targets", "$.build", out.imported_targets)) {
                    return false;
                }
                if (const Element *runtime_images = fields.find("runtime_images");
                    runtime_images != nullptr && !string_array(*runtime_images, "$.build.runtime_images", out.runtime_images)) {
                    return false;
                }
                const Element *registration = required(fields, "registration", "$.build");
                if (registration == nullptr) { return false; }
                ObjectFields registration_fields;
                std::string  kind;
                if (!object(*registration, "$.build.registration", registration_fields) ||
                    !required_string(registration_fields, "kind", "$.build.registration", kind) ||
                    !required_string(registration_fields, "symbol", "$.build.registration", out.registration_symbol)) {
                    return false;
                }
                if (kind != "cpp") { return fail("$.build.registration.kind", "unknown value '" + kind + "'"); }
                if (const Element *lifecycle = fields.find("lifecycle")) {
                    ObjectFields lifecycle_fields;
                    if (!object(*lifecycle, "$.build.lifecycle", lifecycle_fields) ||
                        !required_u32(lifecycle_fields, "abi_version", "$.build.lifecycle", out.lifecycle.abi_version) ||
                        !required_string(lifecycle_fields, "query_symbol", "$.build.lifecycle", out.lifecycle.query_symbol)) {
                        return false;
                    }
                }
                return true;
            }

            std::optional<ReadError> error_{};
        };

        class Validator
        {
          public:
            explicit Validator(const ModuleDescriptor &descriptor) : descriptor_{descriptor} {}

            [[nodiscard]] std::optional<ReadError> run() {
                if (descriptor_.format_version != module_descriptor_format_version) {
                    return ReadError{"$.format_version",
                                     "unsupported descriptor format version " + std::to_string(descriptor_.format_version)};
                }
                if (descriptor_.module_identity.empty()) {
                    return ReadError{"$.module.identity", "module identity must not be empty"};
                }
                if (!descriptor_.descriptor_fingerprint.empty() && descriptor_.descriptor_fingerprint != fingerprint(descriptor_)) {
                    return ReadError{"$.module.descriptor_fingerprint", "descriptor fingerprint does not match canonical contents"};
                }
                if (descriptor_.provider_identity.empty()) {
                    return ReadError{"$.provider.identity", "provider identity must not be empty"};
                }
                for (std::size_t index = 0; index < descriptor_.types.size(); ++index) {
                    if (!type_record(descriptor_.types[index], index_path("$.schema.types", index))) { return error_; }
                }
                for (std::size_t index = 0; index < descriptor_.constant_expressions.size(); ++index) {
                    if (!constant_record(descriptor_.constant_expressions[index],
                                         index_path("$.schema.constant_expressions", index))) {
                        return error_;
                    }
                }
                for (std::size_t index = 0; index < descriptor_.constraints.size(); ++index) {
                    if (!constraint_record(descriptor_.constraints[index], index_path("$.schema.constraints", index))) {
                        return error_;
                    }
                }
                for (std::size_t index = 0; index < descriptor_.interface.size(); ++index) {
                    const InterfaceDeclaration &declaration = descriptor_.interface[index];
                    const std::string           path        = index_path("$.interface", index);
                    if (!unique_identity(declaration.identity, path, interface_identities_)) { return error_; }
                    if (declaration.category == DeclarationCategory::Structure) {
                        if (!generic_parameters(declaration.signature.generics, member_path(path, "generic_parameters")) ||
                            !constraint_ref(declaration.signature.requirements, member_path(path, "requires"), true)) {
                            return error_;
                        }
                    } else if (!signature(declaration.signature, member_path(path, "signature"))) {
                        return error_;
                    }
                    for (std::size_t parent = 0; parent < declaration.parents.size(); ++parent) {
                        if (!type_ref(declaration.parents[parent], index_path(member_path(path, "parents"), parent))) {
                            return error_;
                        }
                    }
                    for (std::size_t field = 0; field < declaration.fields.size(); ++field) {
                        const StructField &member     = declaration.fields[field];
                        const std::string  field_path = index_path(member_path(path, "fields"), field);
                        if (!type_ref(member.type, member_path(field_path, "type")) ||
                            !constant_ref(member.default_value, member_path(field_path, "default"), true)) {
                            return error_;
                        }
                    }
                }
                for (std::size_t index = 0; index < descriptor_.implementations.size(); ++index) {
                    const Implementation &implementation = descriptor_.implementations[index];
                    const std::string     path           = index_path("$.provider.implementations", index);
                    if (!unique_identity(implementation.identity, path, implementation_identities_) ||
                        !signature(implementation.signature, member_path(path, "signature"))) {
                        return error_;
                    }
                }
                for (std::size_t index = 0; index < descriptor_.native_types.size(); ++index) {
                    if (!native_type(descriptor_.native_types[index], index_path("$.native.types", index))) { return error_; }
                }
                for (std::size_t index = 0; index < descriptor_.native_declarations.size(); ++index) {
                    if (!native_declaration(descriptor_.native_declarations[index], index_path("$.native.declarations", index))) {
                        return error_;
                    }
                }
                if (!lifecycle()) { return error_; }
                return std::nullopt;
            }

          private:
            bool fail(std::string path, std::string message) {
                error_ = ReadError{std::move(path), std::move(message)};
                return false;
            }

            bool unique_identity(const std::string &identity, std::string_view path, std::vector<std::string> &seen) {
                if (identity.empty()) { return fail(member_path(path, "identity"), "identity must not be empty"); }
                if (std::ranges::find(seen, identity) != seen.end()) {
                    return fail(member_path(path, "identity"), "duplicate declaration identity '" + identity + "'");
                }
                seen.push_back(identity);
                return true;
            }

            bool arena_ref(SchemaId id, std::size_t size, std::string_view arena, std::string_view path, bool optional = false) {
                if (id == no_schema_id) {
                    return optional || fail(std::string{path}, "missing required " + std::string{arena} + " reference");
                }
                if (id >= size) {
                    return fail(std::string{path}, "dangling " + std::string{arena} + " reference " + std::to_string(id));
                }
                return true;
            }

            bool type_ref(SchemaId id, std::string_view path, bool optional = false) {
                return arena_ref(id, descriptor_.types.size(), "type", path, optional);
            }

            bool constant_ref(SchemaId id, std::string_view path, bool optional = false) {
                return arena_ref(id, descriptor_.constant_expressions.size(), "constant-expression", path, optional);
            }

            bool constraint_ref(SchemaId id, std::string_view path, bool optional = false) {
                return arena_ref(id, descriptor_.constraints.size(), "constraint", path, optional);
            }

            bool generic_parameters(const std::vector<GenericParameter> &parameters, std::string_view path) {
                for (std::size_t index = 0; index < parameters.size(); ++index) {
                    const GenericParameter &parameter = parameters[index];
                    if (!type_ref(parameter.type, member_path(index_path(path, index), "type"), !parameter.is_const)) {
                        return false;
                    }
                }
                return true;
            }

            bool signature(const Signature &value, std::string_view path) {
                if (!generic_parameters(value.generics, member_path(path, "generic_parameters"))) { return false; }
                for (std::size_t index = 0; index < value.parameters.size(); ++index) {
                    const Parameter  &parameter      = value.parameters[index];
                    const std::string parameter_path = index_path(member_path(path, "parameters"), index);
                    if (!type_ref(parameter.type, member_path(parameter_path, "type")) ||
                        !constant_ref(parameter.default_value, member_path(parameter_path, "default"), true)) {
                        return false;
                    }
                }
                return type_ref(value.result, member_path(path, "result"), true) &&
                       constraint_ref(value.requirements, member_path(path, "requires"), true);
            }

            template <typename Enum>
            bool unique_enum_values(const std::vector<Enum> &values, std::string_view path, std::string_view role) {
                for (std::size_t index = 0; index < values.size(); ++index) {
                    if (std::ranges::find(values.begin(), values.begin() + static_cast<std::ptrdiff_t>(index), values[index]) !=
                        values.begin() + static_cast<std::ptrdiff_t>(index)) {
                        return fail(index_path(path, index), "duplicate native " + std::string{role});
                    }
                }
                return true;
            }

            bool native_type(const NativeTypeDeclaration &type, std::string_view path) {
                if (!unique_identity(type.identity, path, native_type_identities_)) { return false; }
                const bool known_identity = std::ranges::any_of(descriptor_.types, [&](const TypeRecord &record) {
                    return record.category == TypeCategory::Symbol && record.nominal_identity == type.identity;
                });
                if (!known_identity) {
                    return fail(member_path(path, "identity"), "native type does not name a nominal descriptor type");
                }
                if (type.cpp_type.empty()) { return fail(member_path(path, "cpp_type"), "C++ type must not be empty"); }
                if (type.public_header.empty()) {
                    return fail(member_path(path, "public_header"), "public header must not be empty");
                }
                if (std::ranges::find(descriptor_.build.public_headers, type.public_header) ==
                    descriptor_.build.public_headers.end()) {
                    return fail(member_path(path, "public_header"), "native type header is not present in build.public_headers");
                }
                return true;
            }

            const NativeParameterPolicy *find_parameter(const NativeDeclaration &declaration, std::string_view name) const {
                const auto found = std::ranges::find(declaration.parameters, name, &NativeParameterPolicy::name);
                return found == declaration.parameters.end() ? nullptr : &*found;
            }

            bool native_value_policy(const NativeValuePolicy &policy, std::string_view path, const NativeDeclaration &declaration,
                                     bool result) {
                if (policy.ownership == NativeOwnership::Shared) {
                    return fail(member_path(path, "ownership"), "shared native ownership is not supported in this ABI version");
                }
                if (policy.ownership == NativeOwnership::Borrowed) {
                    if (result && policy.dependent_on.empty()) {
                        return fail(member_path(path, "dependent_on"), "a borrowed native result must name its lifetime parameter");
                    }
                    if (!policy.dependent_on.empty()) {
                        const NativeParameterPolicy *source = find_parameter(declaration, policy.dependent_on);
                        if (source == nullptr) {
                            return fail(member_path(path, "dependent_on"),
                                        "unknown lifetime parameter '" + policy.dependent_on + "'");
                        }
                        if (source->value.ownership != NativeOwnership::Borrowed) {
                            return fail(member_path(path, "dependent_on"), "a dependent lifetime must name a borrowed parameter");
                        }
                    }
                } else if (!policy.dependent_on.empty()) {
                    return fail(member_path(path, "dependent_on"), "only borrowed native values have a dependent lifetime");
                }
                if (result && policy.mutable_value) {
                    return fail(member_path(path, "mutable"), "a native result cannot be a mutable argument");
                }
                if (policy.mutable_value && policy.ownership != NativeOwnership::Borrowed) {
                    return fail(member_path(path, "mutable"), "a mutable native argument must be borrowed");
                }
                return true;
            }

            bool native_value_type(SchemaId id, std::string_view path, bool optional = false) {
                if (id == no_schema_id) { return optional || fail(std::string{path}, "missing required native value type"); }
                const TypeRecord &type = descriptor_.types[id];
                if (type.category == TypeCategory::Scalar) { return true; }
                if (type.category == TypeCategory::Symbol &&
                    std::ranges::any_of(descriptor_.native_types, [&](const NativeTypeDeclaration &declaration) {
                        return declaration.identity == type.nominal_identity;
                    })) {
                    return true;
                }
                return fail(std::string{path}, "native signature type is outside the initial scalar and declared-native envelope");
            }

            bool native_signature(const Signature &signature, std::string_view path) {
                if (!signature.generics.empty()) {
                    return fail(member_path(path, "generic_parameters"),
                                "native declarations require one complete exact signature");
                }
                if (signature.requirements != no_schema_id) {
                    return fail(member_path(path, "requires"), "native declarations cannot carry unresolved constraints");
                }
                for (std::size_t index = 0; index < signature.parameters.size(); ++index) {
                    if (!native_value_type(signature.parameters[index].type,
                                           member_path(index_path(member_path(path, "parameters"), index), "type"))) {
                        return false;
                    }
                }
                return native_value_type(signature.result, member_path(path, "result"), true);
            }

            bool native_declaration(const NativeDeclaration &declaration, std::string_view path) {
                if (!unique_identity(declaration.identity, path, native_declaration_identities_) ||
                    !signature(declaration.signature, member_path(path, "signature")) ||
                    !native_signature(declaration.signature, member_path(path, "signature"))) {
                    return false;
                }
                if (!exact_cpp_symbol(declaration.cpp_symbol)) {
                    return fail(member_path(path, "cpp_symbol"), "C++ symbol must be one exact qualified identifier");
                }
                if (declaration.phases.empty()) {
                    return fail(member_path(path, "phases"), "native declaration needs at least one permitted phase");
                }
                if (!unique_enum_values(declaration.phases, member_path(path, "phases"), "phase") ||
                    !unique_enum_values(declaration.effects, member_path(path, "effects"), "effect")) {
                    return false;
                }
                if (declaration.parameters.size() != declaration.signature.parameters.size()) {
                    return fail(member_path(path, "parameters"), "native parameter policy count does not match the signature");
                }
                std::size_t mutable_parameters = 0;
                for (std::size_t index = 0; index < declaration.parameters.size(); ++index) {
                    const NativeParameterPolicy &parameter      = declaration.parameters[index];
                    const std::string            parameter_path = index_path(member_path(path, "parameters"), index);
                    if (parameter.name != declaration.signature.parameters[index].name) {
                        return fail(member_path(parameter_path, "name"),
                                    "native parameter policy does not match signature parameter '" +
                                        declaration.signature.parameters[index].name + "'");
                    }
                    if (!native_value_policy(parameter.value, member_path(parameter_path, "value"), declaration, false)) {
                        return false;
                    }
                    if (parameter.value.mutable_value) { ++mutable_parameters; }
                }
                if (!native_value_policy(declaration.result, member_path(path, "result"), declaration, true)) { return false; }
                const bool mutates = std::ranges::find(declaration.effects, NativeEffect::Mutation) != declaration.effects.end();
                if (mutable_parameters != (mutates ? 1U : 0U)) {
                    return fail(member_path(path, "effects"), "mutation requires exactly one explicitly mutable borrowed argument");
                }
                const bool evaluation = std::ranges::find(declaration.phases, NativePhase::Evaluation) != declaration.phases.end();
                if (evaluation && declaration.exception_policy != NativeExceptionPolicy::NoThrow) {
                    return fail(member_path(path, "exception"), "evaluation native functions must be noexcept");
                }
                if (evaluation && std::ranges::find(declaration.effects, NativeEffect::Blocking) != declaration.effects.end()) {
                    return fail(member_path(path, "effects"), "evaluation native functions must be non-blocking");
                }
                if (declaration.category == NativeDeclarationCategory::Constructor &&
                    declaration.result.ownership != NativeOwnership::Owned) {
                    return fail(member_path(path, "result.ownership"), "a native constructor must return owned state");
                }
                return true;
            }

            bool lifecycle() {
                const LifecycleMetadata &lifecycle = descriptor_.build.lifecycle;
                if (lifecycle.abi_version == 0U) {
                    return lifecycle.query_symbol.empty() ||
                           fail("$.build.lifecycle.query_symbol", "a descriptor without a lifecycle ABI has no query symbol");
                }
                if (lifecycle.abi_version != HGL_NATIVE_MODULE_ABI_V1) {
                    return fail("$.build.lifecycle.abi_version",
                                "unsupported native module ABI version " + std::to_string(lifecycle.abi_version));
                }
                if (lifecycle.query_symbol != HGL_NATIVE_MODULE_QUERY_SYMBOL_V1) {
                    return fail("$.build.lifecycle.query_symbol", "query symbol does not match native module ABI version 1");
                }
                return !descriptor_.build.runtime_images.empty() ||
                       fail("$.build.runtime_images", "a native lifecycle requires at least one runtime image");
            }

            bool type_record(const TypeRecord &record, std::string_view path) {
                if (record.category == TypeCategory::Scalar) {
                    if (record.scalar_name.empty()) { return fail(member_path(path, "name"), "scalar type is missing its name"); }
                    if (!known_scalar_name(record.scalar_name)) {
                        return fail(member_path(path, "name"), "unknown scalar type '" + record.scalar_name + "'");
                    }
                }
                if (record.category == TypeCategory::Symbol && record.nominal_identity.empty()) {
                    return fail(member_path(path, "identity"), "symbol type is missing its identity");
                }
                std::optional<std::size_t> required_children;
                switch (record.category) {
                    case TypeCategory::List:
                    case TypeCategory::Set:
                    case TypeCategory::Rolling:
                    case TypeCategory::Atomic:
                    case TypeCategory::Reference:
                    case TypeCategory::HarnessSequence: required_children = 1U; break;
                    case TypeCategory::Map: required_children = 2U; break;
                    case TypeCategory::Void:
                    case TypeCategory::Scalar:
                    case TypeCategory::Symbol:
                    case TypeCategory::Capability:
                    case TypeCategory::Deferred: required_children = 0U; break;
                    case TypeCategory::Tuple:
                    case TypeCategory::Iterator:
                    case TypeCategory::Callable: break;
                }
                if (required_children && record.children.size() != *required_children) {
                    return fail(member_path(path, "children"), "type requires exactly " + std::to_string(*required_children) +
                                                                   " child" + (*required_children == 1U ? "" : "ren"));
                }
                for (std::size_t index = 0; index < record.children.size(); ++index) {
                    if (!type_ref(record.children[index], index_path(member_path(path, "children"), index))) { return false; }
                }
                for (std::size_t index = 0; index < record.arguments.size(); ++index) {
                    const TypeArgument &argument      = record.arguments[index];
                    const std::string   argument_path = member_path(index_path(member_path(path, "arguments"), index), "reference");
                    if (argument.category == TypeArgumentCategory::Type) {
                        if (!type_ref(argument.reference, argument_path)) { return false; }
                    } else if (!constant_ref(argument.reference, argument_path)) {
                        return false;
                    }
                }
                return constant_ref(record.size, member_path(path, "size"), true) &&
                       constant_ref(record.min_size, member_path(path, "min_size"), true);
            }

            bool constant_record(const ConstantExpressionRecord &record, std::string_view path) {
                switch (record.category) {
                    case ConstantExpressionCategory::Literal:
                        if (!record.literal) { return fail(member_path(path, "literal"), "missing literal payload"); }
                        break;
                    case ConstantExpressionCategory::Parameter:
                        if (record.parameter_identity.empty()) {
                            return fail(member_path(path, "parameter"), "missing parameter identity");
                        }
                        break;
                    case ConstantExpressionCategory::Unary:
                        if (!known_unary_operator(record.operator_spelling)) {
                            return fail(member_path(path, "operator"),
                                        record.operator_spelling.empty()
                                            ? "unary expression is missing its operator"
                                            : "unknown unary operator '" + record.operator_spelling + "'");
                        }
                        if (!constant_ref(record.lhs, member_path(path, "lhs"))) { return false; }
                        break;
                    case ConstantExpressionCategory::Binary:
                        if (!known_binary_operator(record.operator_spelling)) {
                            return fail(member_path(path, "operator"),
                                        record.operator_spelling.empty()
                                            ? "binary expression is missing its operator"
                                            : "unknown binary operator '" + record.operator_spelling + "'");
                        }
                        if (!constant_ref(record.lhs, member_path(path, "lhs")) ||
                            !constant_ref(record.rhs, member_path(path, "rhs"))) {
                            return false;
                        }
                        break;
                    case ConstantExpressionCategory::Index:
                        if (!constant_ref(record.lhs, member_path(path, "lhs")) ||
                            !constant_ref(record.rhs, member_path(path, "rhs"))) {
                            return false;
                        }
                        break;
                    case ConstantExpressionCategory::Field:
                        if (record.member.empty()) {
                            return fail(member_path(path, "member"), "field expression is missing its member");
                        }
                        if (!constant_ref(record.lhs, member_path(path, "lhs"))) { return false; }
                        break;
                    case ConstantExpressionCategory::Sequence:
                    case ConstantExpressionCategory::Tuple: break;
                    case ConstantExpressionCategory::Construct:
                        if (!type_ref(record.constructed_type, member_path(path, "type"))) { return false; }
                        break;
                }
                for (std::size_t index = 0; index < record.elements.size(); ++index) {
                    const ConstantElement &element      = record.elements[index];
                    const std::string      element_path = index_path(member_path(path, "elements"), index);
                    if (!constant_ref(element.key, member_path(element_path, "key"), true) ||
                        !constant_ref(element.value, member_path(element_path, "value"))) {
                        return false;
                    }
                }
                for (std::size_t index = 0; index < record.items.size(); ++index) {
                    if (!constant_ref(record.items[index], index_path(member_path(path, "items"), index))) { return false; }
                }
                for (std::size_t index = 0; index < record.arguments.size(); ++index) {
                    if (!constant_ref(record.arguments[index].value,
                                      member_path(index_path(member_path(path, "arguments"), index), "value"))) {
                        return false;
                    }
                }
                return true;
            }

            bool constraint_record(const ConstraintRecord &record, std::string_view path) {
                switch (record.category) {
                    case ConstraintCategory::Symbol:
                        return !record.identity.empty() ||
                               fail(member_path(path, "identity"), "constraint symbol is missing its identity");
                    case ConstraintCategory::Type: return type_ref(record.type, member_path(path, "type"));
                    case ConstraintCategory::Value: return constant_ref(record.value, member_path(path, "value"));
                    case ConstraintCategory::Set: return constraint_refs(record.elements, member_path(path, "elements"));
                    case ConstraintCategory::Call:
                        return (!record.identity.empty() ||
                                fail(member_path(path, "identity"), "constraint call is missing its identity")) &&
                               constraint_refs(record.arguments, member_path(path, "arguments"));
                    case ConstraintCategory::Operator:
                        return (!record.identity.empty() ||
                                fail(member_path(path, "identity"), "operator requirement is missing its identity")) &&
                               constraint_refs(record.arguments, member_path(path, "arguments")) &&
                               type_ref(record.result, member_path(path, "result"), true);
                    case ConstraintCategory::Relation:
                    case ConstraintCategory::Logic:
                        return (!record.operator_spelling.empty() ||
                                fail(member_path(path, "operator"), "constraint expression is missing its operator")) &&
                               constraint_ref(record.lhs, member_path(path, "lhs")) &&
                               constraint_ref(record.rhs, member_path(path, "rhs"));
                    case ConstraintCategory::Not: return constraint_ref(record.operand, member_path(path, "operand"));
                }
                std::unreachable();
            }

            bool constraint_refs(const std::vector<SchemaId> &values, std::string_view path) {
                for (std::size_t index = 0; index < values.size(); ++index) {
                    if (!constraint_ref(values[index], index_path(path, index))) { return false; }
                }
                return true;
            }

            const ModuleDescriptor  &descriptor_;
            std::optional<ReadError> error_{};
            std::vector<std::string> interface_identities_{};
            std::vector<std::string> implementation_identities_{};
            std::vector<std::string> native_type_identities_{};
            std::vector<std::string> native_declaration_identities_{};
        };
    }  // namespace

    ReadResult read_json(std::string_view json) {
        simdjson::dom::parser   parser;
        simdjson::padded_string padded{json};
        Element                 root;
        if (const simdjson::error_code error = parser.parse(padded).get(root)) {
            return ReadResult{.error = ReadError{"$", "invalid JSON: " + std::string{simdjson::error_message(error)}}};
        }
        if (std::optional<ReadError> duplicate = find_duplicate_member(root, "$")) {
            return ReadResult{.error = std::move(duplicate)};
        }
        return Decoder{}.run(root);
    }

    std::optional<ReadError> validate(const ModuleDescriptor &descriptor) { return Validator{descriptor}.run(); }
}  // namespace hgl::descriptor
