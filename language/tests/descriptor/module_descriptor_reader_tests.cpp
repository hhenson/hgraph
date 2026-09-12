#include "descriptor/import_catalog.h"
#include "descriptor/module_descriptor_reader.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace descriptor = hgl::descriptor;

namespace
{
    descriptor::ModuleDescriptor minimal_descriptor() {
        descriptor::ModuleDescriptor result;
        result.module_identity   = "checks.reader";
        result.language_version  = "0.1-test";
        result.provider_identity = "checks.reader";
        return result;
    }

    descriptor::ModuleDescriptor rich_descriptor() {
        descriptor::ModuleDescriptor result = minimal_descriptor();

        result.types = {
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
            descriptor::TypeRecord{
                .category         = descriptor::TypeCategory::Symbol,
                .nominal_identity = "T",
                .binding_identity = "checks.reader.map::T",
                .arguments = {{descriptor::TypeArgumentCategory::Type, 0U}, {descriptor::TypeArgumentCategory::Constant, 0U}}},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Rolling, .children = {1U}, .size = 0U},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Reference, .children = {0U}},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Symbol, .nominal_identity = "checks.reader.State"},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal},
        };

        descriptor::ConstantExpressionRecord integer;
        integer.literal = hgl::ir::hir::Constant{std::numeric_limits<std::int64_t>::max()};
        descriptor::ConstantExpressionRecord floating;
        floating.literal = hgl::ir::hir::Constant{std::numeric_limits<double>::infinity()};
        descriptor::ConstantExpressionRecord text;
        text.literal                = hgl::ir::hir::Constant{std::string{"reader\nvalue"}};
        result.constant_expressions = {
            std::move(integer),
            std::move(floating),
            std::move(text),
            descriptor::ConstantExpressionRecord{
                .category = descriptor::ConstantExpressionCategory::Unary, .operator_spelling = "-", .lhs = 0U},
            descriptor::ConstantExpressionRecord{
                .category = descriptor::ConstantExpressionCategory::Binary, .operator_spelling = "+", .lhs = 0U, .rhs = 0U},
            descriptor::ConstantExpressionRecord{.category = descriptor::ConstantExpressionCategory::Sequence,
                                                 .elements = {{descriptor::no_schema_id, 0U}, {0U, 1U}}},
            descriptor::ConstantExpressionRecord{.category = descriptor::ConstantExpressionCategory::Index, .lhs = 5U, .rhs = 0U},
            descriptor::ConstantExpressionRecord{
                .category = descriptor::ConstantExpressionCategory::Field, .lhs = 5U, .member = "value"},
            descriptor::ConstantExpressionRecord{.category = descriptor::ConstantExpressionCategory::Tuple, .items = {0U, 1U, 2U}},
            descriptor::ConstantExpressionRecord{.category         = descriptor::ConstantExpressionCategory::Construct,
                                                 .constructed_type = 0U,
                                                 .arguments        = {{"value", 0U}},
                                                 .delta            = true},
            descriptor::ConstantExpressionRecord{.category           = descriptor::ConstantExpressionCategory::Parameter,
                                                 .parameter_identity = "checks.reader.map::max_size"},
        };

        result.constraints = {
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Symbol, .identity = "T"},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Type, .type = 0U},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Value, .value = 0U},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Set, .elements = {1U}},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Call, .identity = "schema", .arguments = {0U}},
            descriptor::ConstraintRecord{.category      = descriptor::ConstraintCategory::Operator,
                                         .identity      = "hgraph.std.add",
                                         .registry_name = "add_",
                                         .result        = 0U,
                                         .arguments     = {0U}},
            descriptor::ConstraintRecord{.category          = descriptor::ConstraintCategory::Relation,
                                         .operator_spelling = "in",
                                         .relation_category = "admission",
                                         .lhs               = 0U,
                                         .rhs               = 3U},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Not, .operand = 6U},
            descriptor::ConstraintRecord{
                .category = descriptor::ConstraintCategory::Logic, .operator_spelling = "and", .lhs = 7U, .rhs = 2U},
            descriptor::ConstraintRecord{
                .category = descriptor::ConstraintCategory::Each, .identity = "checks.reader.map::Item", .source = 4U, .body = 5U},
        };

        descriptor::InterfaceDeclaration structure;
        structure.category               = descriptor::DeclarationCategory::Structure;
        structure.identity               = "checks.reader.Record";
        structure.abstract               = true;
        structure.signature.generics     = {{"T", "checks.reader.Record::T", false, descriptor::no_schema_id}};
        structure.signature.requirements = 6U;
        structure.parents                = {1U};
        structure.fields                 = {{"value", 0U, 0U, "checks.reader.Record", true}};

        descriptor::InterfaceDeclaration operation;
        operation.category               = descriptor::DeclarationCategory::Operator;
        operation.identity               = "checks.reader.map";
        operation.registry_name          = "checks.reader.map";
        operation.signature.generics     = {{"T", "checks.reader.map::T", false, descriptor::no_schema_id},
                                            {"max_size", "checks.reader.map::max_size", true, 0U}};
        operation.signature.parameters   = {{"window", "checks.reader.map::window", false, 2U, 0U}};
        operation.signature.result       = 0U;
        operation.signature.requirements = 8U;

        descriptor::InterfaceDeclaration function;
        function.category             = descriptor::DeclarationCategory::Function;
        function.identity             = "checks.reader.public_value";
        function.execution            = descriptor::ExecutionKind::Composition;
        function.signature.parameters = {{"value", "checks.reader.public_value::value", false, 0U, descriptor::no_schema_id}};
        function.signature.result     = 0U;
        result.interface              = {std::move(structure), std::move(operation), std::move(function)};

        descriptor::Implementation implementation;
        implementation.identity               = "checks.reader.map#1";
        implementation.operator_identity      = "checks.reader.map";
        implementation.operator_registry_name = "checks.reader.map";
        implementation.execution              = descriptor::ExecutionKind::RuntimeNode;
        implementation.signature.parameters   = {{"window", "checks.reader.map#1::window", false, 2U, 0U}};
        implementation.signature.result       = 0U;
        result.implementations                = {std::move(implementation)};

        result.native_types = {
            descriptor::NativeTypeDeclaration{.category      = descriptor::NativeTypeCategory::OpaqueState,
                                              .identity      = "checks.reader.State",
                                              .cpp_type      = "checks::reader::State",
                                              .public_header = "checks_reader.h"},
        };
        descriptor::NativeDeclaration update;
        update.identity             = "checks.reader.update";
        update.cpp_symbol           = "checks::reader::update";
        update.signature.parameters = {{"state", "checks.reader.update::state", false, 4U, descriptor::no_schema_id},
                                       {"value", "checks.reader.update::value", false, 0U, descriptor::no_schema_id}};
        update.signature.result     = 0U;
        update.phases               = {descriptor::NativePhase::Evaluation};
        update.effects              = {descriptor::NativeEffect::Mutation};
        update.parameters           = {
            {"state", {descriptor::NativeOwnership::Borrowed, {}, true}},
            {"value", {descriptor::NativeOwnership::Value}},
        };
        update.thread_safety = descriptor::NativeThreadSafety::NodeLocal;

        descriptor::NativeDeclaration construct;
        construct.category         = descriptor::NativeDeclarationCategory::Constructor;
        construct.identity         = "checks.reader.make_state";
        construct.cpp_symbol       = "checks::reader::make_state";
        construct.signature.result = 4U;
        construct.phases           = {descriptor::NativePhase::Start};
        construct.effects          = {descriptor::NativeEffect::Allocation};
        construct.result.ownership = descriptor::NativeOwnership::Owned;
        construct.exception_policy = descriptor::NativeExceptionPolicy::Translated;
        result.native_declarations = {std::move(update), std::move(construct)};

        result.provider_requirements     = {"hgraph.std"};
        result.build.public_headers      = {"checks_reader.h"};
        result.build.cmake_packages      = {"hgraph"};
        result.build.imported_targets    = {"hgraph::core"};
        result.build.runtime_images      = {"libchecks_reader.so"};
        result.build.registration_symbol = "checks::reader::register_operators";
        result.build.lifecycle           = {1U, "hgl_query_native_module_v1"};
        descriptor::seal(result);
        return result;
    }

    descriptor::ModuleDescriptor scalar_native_descriptor() {
        descriptor::ModuleDescriptor result = minimal_descriptor();
        result.types                        = {
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "f64"},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
        };
        descriptor::NativeDeclaration blend;
        blend.identity                = "checks.reader::blend";
        blend.cpp_symbol              = "checks::reader::blend";
        blend.signature.parameters    = {{"value", "checks.reader::blend::value", false, 0U, descriptor::no_schema_id},
                                         {"window", "checks.reader::blend::window", true, 1U, descriptor::no_schema_id}};
        blend.signature.result        = 0U;
        blend.phases                  = {descriptor::NativePhase::Evaluation};
        blend.parameters              = {{"value", {}}, {"window", {}}};
        result.native_declarations    = {std::move(blend)};
        result.build.public_headers   = {"checks/reader.h"};
        result.build.cmake_packages   = {"checks"};
        result.build.imported_targets = {"checks::reader"};
        result.build.runtime_images   = {"libchecks_reader.so"};
        descriptor::seal(result);
        return result;
    }

    descriptor::ModuleDescriptor collection_native_descriptor() {
        descriptor::ModuleDescriptor result = minimal_descriptor();
        result.types                        = {
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
            descriptor::TypeRecord{
                .category = descriptor::TypeCategory::Symbol, .nominal_identity = "T", .binding_identity = "checks.reader::len::T"},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::List, .children = {1U}, .size = 0U},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Set, .children = {1U}},
        };
        result.constant_expressions = {descriptor::ConstantExpressionRecord{
            .category           = descriptor::ConstantExpressionCategory::Parameter,
            .parameter_identity = "checks.reader::len::N",
        }};

        descriptor::NativeDeclaration list_len;
        list_len.identity             = "checks.reader::len";
        list_len.cpp_symbol           = "checks::reader::len";
        list_len.signature.generics   = {{"T", "checks.reader::len::T", false, descriptor::no_schema_id},
                                         {"N", "checks.reader::len::N", true, 0U}};
        list_len.signature.parameters = {{"value", "checks.reader::len::value", false, 2U, descriptor::no_schema_id}};
        list_len.signature.result     = 0U;
        list_len.phases               = {descriptor::NativePhase::Evaluation};
        list_len.parameters           = {{"value", {}, descriptor::NativeParameterAccess::InputView}};

        descriptor::NativeDeclaration set_len = list_len;
        set_len.signature.generics.resize(1U);
        set_len.signature.parameters.front().type = 3U;
        result.native_declarations                = {std::move(list_len), std::move(set_len)};
        descriptor::seal(result);
        return result;
    }

    void replace_once(std::string &text, std::string_view from, std::string_view to) {
        const std::size_t offset = text.find(from);
        REQUIRE(offset != std::string::npos);
        text.replace(offset, from.size(), to);
    }

    void check_error(const descriptor::ReadResult &result, std::string_view path, std::string_view message) {
        REQUIRE_FALSE(result);
        REQUIRE(result.error);
        CHECK(result.error->path == path);
        CHECK(result.error->message == message);
    }
}  // namespace

TEST_CASE("module descriptor reader round-trips the complete model", "[descriptor][reader]") {
    const descriptor::ModuleDescriptor expected = rich_descriptor();
    const descriptor::ReadResult       result   = descriptor::read_json(descriptor::to_json(expected));

    REQUIRE(result);
    REQUIRE(result.value);
    CHECK(*result.value == expected);
    CHECK_FALSE(result.error);
}

TEST_CASE("operator properties round trip and contribute to the descriptor fingerprint", "[descriptor][reader][properties]") {
    descriptor::ModuleDescriptor source;
    source.module_identity   = "checks.properties";
    source.provider_identity = source.module_identity;
    source.types             = {
        {.category = descriptor::TypeCategory::Scalar, .scalar_name = "str"},
        {.category = descriptor::TypeCategory::Symbol, .nominal_identity = "T", .binding_identity = "checks.properties::join::T"}};
    source.constant_expressions = {{.literal = hgl::ir::hir::Constant{std::string{}}}};
    descriptor::InterfaceDeclaration operation;
    operation.category             = descriptor::DeclarationCategory::Operator;
    operation.identity             = "checks.properties::join";
    operation.signature.generics   = {{"T", "checks.properties::join::T", false, descriptor::no_schema_id}};
    operation.signature.parameters = {{"lhs", "checks.properties::join::lhs", false, 1U},
                                      {"rhs", "checks.properties::join::rhs", false, 1U}};
    operation.signature.result     = 1U;
    operation.properties           = {{{0U}, true, false, 0U}};
    source.interface.push_back(operation);
    descriptor::seal(source);
    const auto decoded = descriptor::read_json(descriptor::to_json(source));
    INFO((decoded.error ? decoded.error->message : ""));
    REQUIRE(decoded);
    CHECK(*decoded.value == source);
    const auto fingerprint                                  = source.descriptor_fingerprint;
    source.interface.front().properties.front().commutative = true;
    descriptor::seal(source);
    CHECK(source.descriptor_fingerprint != fingerprint);
    source.descriptor_fingerprint.clear();  // validate the malformed shape, not a stale checksum
    source.interface.front().properties.front().domain = {999U};
    REQUIRE(descriptor::validate(source));
    CHECK(descriptor::validate(source)->path.find("properties") != std::string::npos);
    source.interface.front().properties.front().domain = {1U};
    REQUIRE(descriptor::validate(source));
    CHECK(descriptor::validate(source)->message == "properties require concrete type domains");
    source.interface.front().properties.front().domain        = {0U};
    source.interface.front().signature.parameters.back().pack = descriptor::ParameterPack::Positional;
    REQUIRE(descriptor::validate(source));
    CHECK(descriptor::validate(source)->message == "operator laws require two fixed non-const inputs");
    source.interface.front().signature.parameters.back().pack   = descriptor::ParameterPack::None;
    source.interface.front().signature.generics.front().is_pack = true;
    REQUIRE(descriptor::validate(source));
    CHECK(descriptor::validate(source)->message == "properties require concrete type domains");
    source.interface.front().signature.generics.front().is_pack = false;
    source.interface.front().properties.push_back(source.interface.front().properties.front());
    CHECK(descriptor::validate(source));
}

TEST_CASE("descriptor identities are assignable to the specialized result", "[descriptor][reader][properties]") {
    using hgl::ir::hir::Constant;
    struct Example
    {
        std::string scalar;
        Constant    identity;
        bool        accepted;
    };
    for (const auto &[scalar, identity, accepted] :
         std::vector<Example>{{"i64", std::string{}, false},
                              {"i64", std::int64_t{0}, true},
                              {"i64", 0.0, false},
                              {"f64", std::int64_t{0}, true},
                              {"f64", 0.0, true},
                              {"f64", true, false},
                              {"str", std::string{}, true},
                              {"str", std::int64_t{0}, false},
                              {"bool", false, true},
                              {"bool", std::int64_t{0}, false},
                              {"duration", hgl::syntax::TemporalValue{hgl::syntax::TemporalKind::Duration, 0}, true},
                              {"date", hgl::syntax::TemporalValue{hgl::syntax::TemporalKind::Duration, 0}, false}}) {
        for (const descriptor::SchemaId result : {0U, 1U, 2U}) {
            descriptor::ModuleDescriptor source;
            source.module_identity   = "checks.properties";
            source.provider_identity = source.module_identity;
            source.types = {{.category = descriptor::TypeCategory::Scalar, .scalar_name = scalar},
                            {.category = descriptor::TypeCategory::Symbol, .nominal_identity = "O", .binding_identity = "op::O"},
                            {.category = descriptor::TypeCategory::Atomic, .children = {1U}},
                            {.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"}};
            source.constant_expressions = {{.literal = identity}};
            descriptor::InterfaceDeclaration operation;
            operation.category = descriptor::DeclarationCategory::Operator;
            operation.identity = "checks.properties::op";
            // O is the SECOND generic; substitution uses binding identity and
            // declaration order, not the first domain entry or symbol spelling.
            operation.signature.generics   = {{"T", "op::T", false, descriptor::no_schema_id},
                                              {"O", "op::O", false, descriptor::no_schema_id}};
            operation.signature.parameters = {{"lhs", "op::lhs", false, result}, {"rhs", "op::rhs", false, result}};
            operation.signature.result     = result;
            operation.properties           = {{{3U, 0U}, false, false, 0U}};
            source.interface.push_back(operation);
            INFO(scalar);
            INFO(result);
            const auto error = descriptor::validate(source);
            CHECK(error.has_value() != accepted);
            if (error) {
                CHECK(error->path == "$.interface[0].properties[0].identity");
                CHECK(error->message == "operator identity is not assignable to the specialized result type");
            }
            // A fresh checksum must not allow malformed metadata through JSON.
            descriptor::seal(source);
            CHECK(static_cast<bool>(descriptor::read_json(descriptor::to_json(source))) == accepted);
        }
    }
}

TEST_CASE("validated native scalar functions form a deterministic import catalog", "[descriptor][catalog]") {
    hgl::semantics::ModuleCatalog catalog;
    const auto                    error = descriptor::add_to_catalog(scalar_native_descriptor(), catalog);
    REQUIRE_FALSE(error);

    const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
    REQUIRE(function != nullptr);
    CHECK(function->identity == "checks.reader::blend");
    CHECK(function->cpp_symbol == "checks::reader::blend");
    REQUIRE(function->parameters.size() == 2U);
    CHECK(function->parameters[0].type == hgl::semantics::ImportedScalarType::F64);
    CHECK(function->parameters[1].is_const);
    CHECK(function->result == hgl::semantics::ImportedScalarType::F64);
    CHECK(function->phases == std::vector{hgl::semantics::NativeCallPhase::Evaluation});
    CHECK(function->public_headers == std::vector<std::string>{"checks/reader.h"});
    CHECK(function->cmake_packages == std::vector<std::string>{"checks"});
    CHECK(function->imported_targets == std::vector<std::string>{"checks::reader"});
    CHECK(function->runtime_images == std::vector<std::string>{"libchecks_reader.so"});
}

TEST_CASE("validated native collection views form one overload family", "[descriptor][catalog]") {
    hgl::semantics::ModuleCatalog catalog;
    REQUIRE_FALSE(descriptor::add_to_catalog(collection_native_descriptor(), catalog));

    const std::span<const hgl::semantics::ImportedFunction> functions = catalog.find_functions("checks.reader", "len");
    REQUIRE(functions.size() == 2U);
    CHECK(functions[0].identity == "checks.reader::len");
    CHECK(functions[1].identity == "checks.reader::len");
    CHECK(functions[0].candidate_identity != functions[1].candidate_identity);
    CHECK(functions[0].parameters.front().access == hgl::semantics::NativeParameterAccess::InputView);
    CHECK(functions[1].parameters.front().access == hgl::semantics::NativeParameterAccess::InputView);
}

TEST_CASE("validated native signal views enter the import catalog", "[descriptor][catalog][signal]") {
    descriptor::ModuleDescriptor source = scalar_native_descriptor();
    source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
    source.native_declarations.front().signature.parameters.front().type = 2U;
    source.native_declarations.front().parameters.front().access         = descriptor::NativeParameterAccess::InputView;
    source.descriptor_fingerprint.clear();
    descriptor::seal(source);

    hgl::semantics::ModuleCatalog catalog;
    REQUIRE_FALSE(descriptor::add_to_catalog(source, catalog));
    const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
    REQUIRE(function != nullptr);
    REQUIRE(function->parameters.size() == 2U);
    CHECK(function->parameters.front().type.kind == hgl::semantics::ImportedTypeKind::Signal);
    CHECK(function->parameters.front().access == hgl::semantics::NativeParameterAccess::InputView);
    CHECK(function->support_error.empty());
}

TEST_CASE("validated native schema parameters enter the import catalog", "[descriptor][catalog][schema]") {
    descriptor::ModuleDescriptor source = scalar_native_descriptor();
    source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Schema});
    auto &parameter         = source.native_declarations.front().signature.parameters.front();
    parameter.type          = 2U;
    parameter.runtime_value = true;
    source.native_declarations.front().parameters.front().value.ownership = descriptor::NativeOwnership::Borrowed;
    source.descriptor_fingerprint.clear();
    descriptor::seal(source);

    hgl::semantics::ModuleCatalog catalog;
    REQUIRE_FALSE(descriptor::add_to_catalog(source, catalog));
    const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
    REQUIRE(function != nullptr);
    REQUIRE(function->parameters.size() == 2U);
    CHECK(function->parameters.front().type.kind == hgl::semantics::ImportedTypeKind::Schema);
    CHECK(function->parameters.front().access == hgl::semantics::NativeParameterAccess::Value);
    CHECK(function->support_error.empty());
}

TEST_CASE("catalog import rejects native identities outside their module namespace", "[descriptor][catalog]") {
    descriptor::ModuleDescriptor source         = scalar_native_descriptor();
    source.native_declarations.front().identity = "checks.reader.blend";
    source.descriptor_fingerprint.clear();
    descriptor::seal(source);

    hgl::semantics::ModuleCatalog catalog;
    const auto                    error = descriptor::add_to_catalog(source, catalog);
    REQUIRE(error);
    CHECK(error->path == "$.native.declarations[0].identity");
    CHECK(error->message == "native function identity must be 'checks.reader::<name>'");
    CHECK(catalog.modules().empty());
}

TEST_CASE("catalog preserves unsupported native declarations for precise import diagnostics", "[descriptor][catalog]") {
    SECTION("effects") {
        descriptor::ModuleDescriptor source        = scalar_native_descriptor();
        source.native_declarations.front().effects = {descriptor::NativeEffect::Allocation};
        source.descriptor_fingerprint.clear();
        descriptor::seal(source);

        hgl::semantics::ModuleCatalog catalog;
        REQUIRE_FALSE(descriptor::add_to_catalog(source, catalog));
        const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
        REQUIRE(function != nullptr);
        CHECK(function->support_error == "native value calls with declared effects are not supported yet");
    }

    SECTION("phase") {
        descriptor::ModuleDescriptor source       = scalar_native_descriptor();
        source.native_declarations.front().phases = {descriptor::NativePhase::Wiring};
        source.descriptor_fingerprint.clear();
        descriptor::seal(source);

        hgl::semantics::ModuleCatalog catalog;
        REQUIRE_FALSE(descriptor::add_to_catalog(source, catalog));
        const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
        REQUIRE(function != nullptr);
        CHECK(function->support_error == "native value calls currently require the evaluation phase only");
    }
}

TEST_CASE("module descriptor reader accepts compatible unknown members", "[descriptor][reader]") {
    const descriptor::ModuleDescriptor expected = rich_descriptor();
    std::string                        json     = descriptor::to_json(expected);
    replace_once(json, "  \"format\":", "  \"future_top_level\": {\"value\": true},\n  \"format\":");
    replace_once(json, "    \"identity\": \"checks.reader\",",
                 "    \"identity\": \"checks.reader\",\n    \"future_module_member\": [1, 2, 3],");

    const descriptor::ReadResult result = descriptor::read_json(json);
    REQUIRE(result);
    CHECK(result.value == expected);
}

TEST_CASE("module descriptor reader rejects a stale canonical fingerprint", "[descriptor][reader][fingerprint]") {
    descriptor::ModuleDescriptor source = rich_descriptor();
    std::string                  json   = descriptor::to_json(source);
    replace_once(json, "\"identity\": \"checks.reader\"", "\"identity\": \"checks.changed\"");

    check_error(descriptor::read_json(json), "$.module.descriptor_fingerprint",
                "descriptor fingerprint does not match canonical contents");
}

TEST_CASE("module descriptor reader rejects malformed envelopes", "[descriptor][reader]") {
    SECTION("invalid JSON") {
        const descriptor::ReadResult result = descriptor::read_json("{");
        REQUIRE_FALSE(result);
        REQUIRE(result.error);
        CHECK(result.error->path == "$");
        CHECK(result.error->message.starts_with("invalid JSON:"));
    }

    SECTION("duplicate members") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "  \"format\": \"hgl.module\",", "  \"format\": \"hgl.module\",\n  \"format\": \"hgl.module\",");
        check_error(descriptor::read_json(json), "$.format", "duplicate member");
    }

    SECTION("duplicate members inside compatible unknown values") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "  \"format\":", "  \"future\": {\"nested\": {\"value\": 1, \"value\": 2}},\n  \"format\":");
        check_error(descriptor::read_json(json), "$.future.nested.value", "duplicate member");
    }

    SECTION("wrong format") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "\"format\": \"hgl.module\"", "\"format\": \"other.module\"");
        check_error(descriptor::read_json(json), "$.format", "expected 'hgl.module'");
    }

    SECTION("unsupported version") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "\"format_version\": 5", "\"format_version\": 6");
        check_error(descriptor::read_json(json), "$.format_version", "unsupported descriptor format version 6");
    }
}

TEST_CASE("module descriptor reader rejects malformed schema records", "[descriptor][reader]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.types = {descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"}};

    SECTION("unknown record kind") {
        std::string json = descriptor::to_json(source);
        replace_once(json, "\"kind\": \"scalar\"", "\"kind\": \"mystery\"");
        check_error(descriptor::read_json(json), "$.schema.types[0].kind", "unknown value 'mystery'");
    }

    SECTION("record id does not match its position") {
        std::string json = descriptor::to_json(source);
        replace_once(json, "\"id\": 0", "\"id\": 1");
        check_error(descriptor::read_json(json), "$.schema.types[0].id", "record id does not match array index");
    }

    SECTION("dangling reference") {
        source.types.front().category = descriptor::TypeCategory::Tuple;
        source.types.front().scalar_name.clear();
        source.types.front().children = {4U};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children[0]",
                    "dangling type reference 4");
    }

    SECTION("missing category payload") {
        source.types.front().scalar_name.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].name",
                    "scalar type is missing its name");
    }

    SECTION("unknown scalar name") {
        source.types.front().scalar_name = "mystery";
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].name", "unknown scalar type 'mystery'");
    }

    SECTION("fixed-shape type has the wrong child count") {
        source.types.front().category = descriptor::TypeCategory::Map;
        source.types.front().scalar_name.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 2 children");
    }

    SECTION("reference type has the wrong child count") {
        source.types.front().category = descriptor::TypeCategory::Reference;
        source.types.front().scalar_name.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 1 child");
    }

    SECTION("leaf type has children") {
        source.types.push_back(source.types.front());
        source.types.front().children = {1U};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 0 children");
    }

    SECTION("signal cannot be nested") {
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
        source.types.front().category = descriptor::TypeCategory::List;
        source.types.front().scalar_name.clear();
        source.types.front().children = {1U};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children[0]",
                    "'signal' is only valid as a complete non-const parameter type");
    }
}

TEST_CASE("module descriptors restrict signal to non-const inputs", "[descriptor][reader][signal]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.types                        = {
        descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
        descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal},
    };

    descriptor::InterfaceDeclaration observe;
    observe.identity             = "checks.reader.observe";
    observe.execution            = descriptor::ExecutionKind::RuntimeNode;
    observe.signature.parameters = {{"pulse", "checks.reader.observe::pulse", false, 1U, descriptor::no_schema_id}};
    observe.signature.result     = 0U;
    source.interface             = {observe};

    SECTION("a non-const signal parameter is valid") { REQUIRE(descriptor::read_json(descriptor::to_json(source))); }

    SECTION("signal cannot be const") {
        source.interface.front().signature.parameters.front().is_const = true;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].signature.parameters[0].type",
                    "'signal' is only valid as a complete non-const parameter type");
    }

    SECTION("signal cannot have a default") {
        source.constant_expressions.push_back(descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{true}});
        source.interface.front().signature.parameters.front().default_value = 0U;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].signature.parameters[0].default",
                    "a 'signal' input cannot have a default value");
    }

    SECTION("signal cannot be a result") {
        source.interface.front().signature.result = 1U;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].signature.result",
                    "'signal' is only valid as a complete non-const parameter type");
    }

    SECTION("signal cannot be a struct field") {
        source.interface.front().category  = descriptor::DeclarationCategory::Structure;
        source.interface.front().signature = {};
        source.interface.front().fields    = {{"pulse", 1U, descriptor::no_schema_id, "checks.reader.observe", false}};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].fields[0].type",
                    "'signal' is only valid as a complete non-const parameter type");
    }
}

TEST_CASE("module descriptors validate quantified constraints", "[descriptor][reader][parameter-pack]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.constraints                  = {
        descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Symbol, .identity = "Ts"},
        descriptor::ConstraintRecord{
            .category = descriptor::ConstraintCategory::Each, .identity = "checks.reader.pack::T", .source = 0U, .body = 0U},
    };

    SECTION("a complete quantified constraint round trips") {
        const auto decoded = descriptor::read_json(descriptor::to_json(source));
        INFO((decoded.error ? decoded.error->message : ""));
        REQUIRE(decoded);
        REQUIRE(decoded.value->constraints.size() == 2U);
        CHECK(decoded.value->constraints[1] == source.constraints[1]);
    }
    SECTION("the local binding is required") {
        source.constraints[1].identity.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constraints[1].identity",
                    "each constraint is missing its binding identity");
    }
    SECTION("the source is required") {
        source.constraints[1].source = descriptor::no_schema_id;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constraints[1].source",
                    "missing required constraint reference");
    }
    SECTION("the body is required") {
        source.constraints[1].body = descriptor::no_schema_id;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constraints[1].body",
                    "missing required constraint reference");
    }
}

TEST_CASE("module descriptors preserve and validate pack cardinality", "[descriptor][reader][parameter-pack]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.types = {descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"}};
    descriptor::InterfaceDeclaration operation;
    operation.category             = descriptor::DeclarationCategory::Operator;
    operation.identity             = "checks.reader.bounded";
    operation.registry_name        = "bounded";
    operation.signature.parameters = {{.name             = "values",
                                       .binding_identity = "checks.reader.bounded::values",
                                       .type             = 0U,
                                       .pack             = descriptor::ParameterPack::Positional,
                                       .cardinality      = descriptor::PackCardinality{1U, 3U}}};
    operation.signature.result     = 0U;
    source.interface               = {operation};

    const std::string json    = descriptor::to_json(source);
    const auto        decoded = descriptor::read_json(json);
    INFO((decoded.error ? decoded.error->message : ""));
    REQUIRE(decoded);
    CHECK(decoded.value->interface.front().signature.parameters.front().cardinality.minimum == 1U);
    CHECK(decoded.value->interface.front().signature.parameters.front().cardinality.maximum == 3U);

    SECTION("a pack requires bounds") {
        std::string invalid = json;
        replace_once(invalid, "\"cardinality\": {\"minimum\": 1, \"maximum\": 3}", "\"cardinality\": null");
        check_error(descriptor::read_json(invalid), "$.interface[0].signature.parameters[0].cardinality",
                    "a parameter pack requires cardinality bounds");
    }
    SECTION("the maximum cannot precede the minimum") {
        std::string invalid = json;
        replace_once(invalid, "\"maximum\": 3", "\"maximum\": 0");
        check_error(descriptor::read_json(invalid), "$.interface[0].signature.parameters[0].cardinality",
                    "pack cardinality maximum cannot be less than minimum");
    }
}

TEST_CASE("module descriptor reader rejects unknown constant operators", "[descriptor][reader]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.constant_expressions         = {
        descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{std::int64_t{1}}},
        descriptor::ConstantExpressionRecord{
            .category = descriptor::ConstantExpressionCategory::Unary, .operator_spelling = "bogus", .lhs = 0U},
    };

    check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constant_expressions[1].operator",
                "unknown unary operator 'bogus'");

    source.constant_expressions[1] = descriptor::ConstantExpressionRecord{
        .category          = descriptor::ConstantExpressionCategory::Binary,
        .operator_spelling = "bogus",
        .lhs               = 0U,
        .rhs               = 0U,
    };
    check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constant_expressions[1].operator",
                "unknown binary operator 'bogus'");
}

TEST_CASE("module descriptor validation rejects incomplete semantic records", "[descriptor][reader]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.constant_expressions.emplace_back();

    const std::optional<descriptor::ReadError> error = descriptor::validate(source);
    REQUIRE(error);
    CHECK(error->path == "$.schema.constant_expressions[0].literal");
    CHECK(error->message == "missing literal payload");
}

TEST_CASE("native descriptor validation enforces the initial safety envelope", "[descriptor][reader][native]") {
    SECTION("native overloads require distinct signatures") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.native_declarations.push_back(source.native_declarations.front());
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[1].identity",
                    "duplicate native overload for declaration identity 'checks.reader::blend'");
    }

    SECTION("native overload signatures compare schema structure and alpha-equivalent generics") {
        descriptor::ModuleDescriptor source = collection_native_descriptor();
        const descriptor::SchemaId   i64{static_cast<descriptor::SchemaId>(source.types.size())};
        source.types.push_back({.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"});
        const descriptor::SchemaId element{static_cast<descriptor::SchemaId>(source.types.size())};
        source.types.push_back({.category         = descriptor::TypeCategory::Symbol,
                                .nominal_identity = "Element",
                                .binding_identity = "checks.reader::len#duplicate::Element"});
        const descriptor::SchemaId extent{static_cast<descriptor::SchemaId>(source.constant_expressions.size())};
        source.constant_expressions.push_back({.category           = descriptor::ConstantExpressionCategory::Parameter,
                                               .parameter_identity = "checks.reader::len#duplicate::Extent"});
        const descriptor::SchemaId list{static_cast<descriptor::SchemaId>(source.types.size())};
        source.types.push_back({.category = descriptor::TypeCategory::List, .children = {element}, .size = extent});

        descriptor::NativeDeclaration duplicate = source.native_declarations.front();
        duplicate.cpp_symbol                    = "checks::reader::duplicate_len";
        duplicate.signature.generics = {{"Element", "checks.reader::len#duplicate::Element", false, descriptor::no_schema_id},
                                        {"Extent", "checks.reader::len#duplicate::Extent", true, i64}};
        duplicate.signature.parameters.front().binding_identity = "checks.reader::len#duplicate::value";
        duplicate.signature.parameters.front().type             = list;
        source.native_declarations.push_back(std::move(duplicate));
        source.descriptor_fingerprint.clear();

        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[2].identity",
                    "duplicate native overload for declaration identity 'checks.reader::len'");
    }

    SECTION("collection parameters require explicit input-view access") {
        descriptor::ModuleDescriptor source                          = collection_native_descriptor();
        source.native_declarations.front().parameters.front().access = descriptor::NativeParameterAccess::Value;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "a native collection parameter requires input-view access");
    }

    SECTION("input-view access requires a collection or signal pattern") {
        descriptor::ModuleDescriptor source                          = scalar_native_descriptor();
        source.native_declarations.front().parameters.front().access = descriptor::NativeParameterAccess::InputView;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "input-view access requires a collection or signal parameter");
    }

    SECTION("signal parameters require explicit input-view access") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
        source.native_declarations.front().signature.parameters.front().type = 2U;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "a native signal parameter requires input-view access");
    }

    SECTION("signal input-view parameters are valid") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
        source.native_declarations.front().signature.parameters.front().type = 2U;
        source.native_declarations.front().parameters.front().access         = descriptor::NativeParameterAccess::InputView;
        source.descriptor_fingerprint.clear();
        REQUIRE(descriptor::read_json(descriptor::to_json(source)));
    }

    SECTION("schema parameters are borrowed runtime metadata") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Schema});
        auto &parameter         = source.native_declarations.front().signature.parameters.front();
        parameter.type          = 2U;
        parameter.runtime_value = true;

        parameter.is_const = true;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "'schema' is only valid as a complete non-const parameter type");

        parameter.is_const = false;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].parameters[0].value.ownership",
                    "a native schema parameter requires borrowed ownership");

        source.native_declarations.front().parameters.front().value.ownership = descriptor::NativeOwnership::Borrowed;
        source.descriptor_fingerprint.clear();
        REQUIRE(descriptor::read_json(descriptor::to_json(source)));

        source.native_declarations.front().parameters.front().value.mutable_value = true;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].parameters[0].value.mutable",
                    "a native schema parameter is immutable");
    }

    SECTION("schema metadata cannot be returned") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Schema});
        source.native_declarations.front().signature.result = 2U;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.result",
                    "a borrowed schema handle cannot be returned");
    }

    SECTION("schema metadata cannot be nested in collection parameters") {
        descriptor::ModuleDescriptor source = collection_native_descriptor();
        const descriptor::SchemaId   schema{static_cast<descriptor::SchemaId>(source.types.size())};
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Schema});
        source.types[source.native_declarations.front().signature.parameters.front().type].children.front() = schema;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)),
                    "$.native.declarations[0].signature.parameters[0].type.children[0]",
                    "'schema' is supported only as a complete native parameter type");
    }

    SECTION("native types name a nominal descriptor type") {
        descriptor::ModuleDescriptor source  = rich_descriptor();
        source.native_types.front().identity = "checks.reader.Missing";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.types[0].identity",
                    "native type does not name a nominal descriptor type");
    }

    SECTION("evaluation functions are noexcept") {
        descriptor::ModuleDescriptor source                 = rich_descriptor();
        source.native_declarations.front().exception_policy = descriptor::NativeExceptionPolicy::Translated;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].exception",
                    "evaluation native functions must be noexcept");
    }

    SECTION("native symbols are exact qualified identifiers") {
        descriptor::ModuleDescriptor source           = rich_descriptor();
        source.native_declarations.front().cpp_symbol = "checks::reader::update(); injected";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].cpp_symbol",
                    "C++ symbol must be one exact qualified identifier");

        source.native_declarations.front().cpp_symbol = "::checks::reader::update";
        CHECK(descriptor::validate(source) == std::nullopt);
    }

    SECTION("mutation identifies one borrowed argument") {
        descriptor::ModuleDescriptor source                                       = rich_descriptor();
        source.native_declarations.front().parameters.front().value.mutable_value = false;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].effects",
                    "mutation requires exactly one explicitly mutable borrowed argument");
    }

    SECTION("borrowed results name their lifetime source") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        auto                        &result = source.native_declarations.front().result;
        result.ownership                    = descriptor::NativeOwnership::Borrowed;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].result.dependent_on",
                    "a borrowed native result must name its lifetime parameter");
    }

    SECTION("borrowed results depend on borrowed parameters") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        auto                        &result = source.native_declarations.front().result;
        result.ownership                    = descriptor::NativeOwnership::Borrowed;
        result.dependent_on                 = "value";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].result.dependent_on",
                    "a dependent lifetime must name a borrowed parameter");
    }

    SECTION("mutable parameters require mutation metadata") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        auto                        &update = source.native_declarations.front();
        update.effects.clear();
        update.parameters[1].value = {descriptor::NativeOwnership::Borrowed, {}, true};
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].effects",
                    "mutation requires exactly one explicitly mutable borrowed argument");
    }

    SECTION("native collection patterns reject undeclared generics") {
        descriptor::ModuleDescriptor source                             = rich_descriptor();
        source.native_declarations.front().signature.parameters[1].type = 2U;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)),
                    "$.native.declarations[0].signature.parameters[1].type.children[0]",
                    "native signature type is outside the scalar, declared-native, and collection-view envelope");
    }

    SECTION("native signatures reject undeclared nominal types") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        source.native_types.clear();
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "native signature type is outside the scalar, declared-native, and collection-view envelope");
    }

    SECTION("constructors declare their owned result type") {
        descriptor::ModuleDescriptor source                = rich_descriptor();
        source.native_declarations.back().signature.result = descriptor::no_schema_id;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[1].signature.result",
                    "a native constructor must declare its result type");
    }

    SECTION("lifecycle ABI and query symbol agree") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        source.build.lifecycle.query_symbol = "wrong_query";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.build.lifecycle.query_symbol",
                    "query symbol does not match native module ABI version 1");
    }
}
