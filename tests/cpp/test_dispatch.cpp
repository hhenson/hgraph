#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/context_wiring.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/wired_fn.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <array>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace
{
    using namespace hgraph;

    // The declared input types are ordinary C++ value-layer Bundle schemas.
    using Animal = Bundle<"tests.dispatch::Animal", Field<"id", Int>>;
    using Food = Bundle<"tests.dispatch::Food", Field<"id", Int>>;
    using EmptyAnimal = Bundle<"tests.dispatch::EmptyAnimal">;

    struct DispatchTypes
    {
        const ValueTypeMetaData *animal;
        const ValueTypeMetaData *dog;
        const ValueTypeMetaData *puppy;
        const ValueTypeMetaData *cat;
        const ValueTypeMetaData *bird;
        const ValueTypeMetaData *left;
        const ValueTypeMetaData *right;
        const ValueTypeMetaData *both;
        const ValueTypeMetaData *food;
        const ValueTypeMetaData *meat;
        const ValueTypeMetaData *kibble;
    };

    DispatchTypes register_dispatch_types()
    {
        auto &registry = TypeRegistry::instance();
        const auto *integer = registry.value_type("int");
        const auto *text = registry.value_type("str");
        const auto *boolean = registry.value_type("bool");
        const auto *animal = scalar_descriptor<Animal>::value_meta();
        const auto *food = scalar_descriptor<Food>::value_meta();

        const auto *dog = registry.bundle(
            "tests.dispatch", "Dog", {{"id", integer}, {"sound", text}}, {animal});
        const auto *puppy = registry.bundle(
            "tests.dispatch", "Puppy",
            {{"id", integer}, {"sound", text}, {"young", boolean}}, {dog});
        const auto *cat = registry.bundle(
            "tests.dispatch", "Cat", {{"id", integer}, {"sound", text}}, {animal});
        const auto *bird = registry.bundle(
            "tests.dispatch", "Bird", {{"id", integer}, {"sound", text}}, {animal});
        const auto *left = registry.bundle(
            "tests.dispatch", "Left", {{"id", integer}, {"left", integer}}, {animal}, true);
        const auto *right = registry.bundle(
            "tests.dispatch", "Right", {{"id", integer}, {"right", integer}}, {animal}, true);
        const auto *both = registry.bundle(
            "tests.dispatch", "Both",
            {{"id", integer}, {"left", integer}, {"right", integer}}, {left, right});
        const auto *meat = registry.bundle(
            "tests.dispatch", "Meat", {{"id", integer}, {"raw", boolean}}, {food});
        const auto *kibble = registry.bundle(
            "tests.dispatch", "Kibble", {{"id", integer}, {"size", integer}}, {food});

        return {animal, dog, puppy, cat, bird, left, right, both, food, meat, kibble};
    }

    struct EmptyDispatchTypes
    {
        const ValueTypeMetaData *animal;
        const ValueTypeMetaData *dog;
        const ValueTypeMetaData *puppy;
    };

    EmptyDispatchTypes register_empty_dispatch_types()
    {
        auto &registry = TypeRegistry::instance();
        const auto *animal = scalar_descriptor<EmptyAnimal>::value_meta();
        const auto *dog = registry.bundle("tests.dispatch", "EmptyDog", {}, {animal});
        const auto *puppy = registry.bundle("tests.dispatch", "EmptyPuppy", {}, {dog});
        return {animal, dog, puppy};
    }

    Value bundle_value(
        const ValueTypeMetaData *schema,
        std::initializer_list<std::pair<std::string_view, Value>> fields)
    {
        BundleBuilder builder{ValuePlanFactory::instance().type_for(schema)};
        for (const auto &[name, value] : fields) { builder.set(name, value.view()); }
        return builder.build();
    }

    Value dog_value(const DispatchTypes &types, Int id, std::string_view sound)
    {
        return bundle_value(
            types.dog, {{"id", Value{id}}, {"sound", Value{Str{sound}}}});
    }

    Value puppy_value(const DispatchTypes &types, Int id, std::string_view sound)
    {
        return bundle_value(
            types.puppy,
            {{"id", Value{id}}, {"sound", Value{Str{sound}}},
             {"young", Value{Bool{true}}}});
    }

    Value cat_value(const DispatchTypes &types, Int id)
    {
        return bundle_value(
            types.cat, {{"id", Value{id}}, {"sound", Value{Str{"meow"}}}});
    }

    Value bird_value(const DispatchTypes &types, Int id)
    {
        return bundle_value(
            types.bird, {{"id", Value{id}}, {"sound", Value{Str{"chirp"}}}});
    }

    Value meat_value(const DispatchTypes &types, Int id, Bool raw)
    {
        return bundle_value(
            types.meat, {{"id", Value{id}}, {"raw", Value{raw}}});
    }

    Value kibble_value(const DispatchTypes &types, Int id, Int size)
    {
        return bundle_value(
            types.kibble, {{"id", Value{id}}, {"size", Value{size}}});
    }

    Value both_value(const DispatchTypes &types, Int id)
    {
        return bundle_value(
            types.both,
            {{"id", Value{id}}, {"left", Value{Int{2}}}, {"right", Value{Int{3}}}});
    }

    std::vector<std::optional<Value>> string_values(
        std::initializer_list<std::string_view> values)
    {
        std::vector<std::optional<Value>> result;
        result.reserve(values.size());
        for (const auto value : values) { result.emplace_back(Value{Str{value}}); }
        return result;
    }

    struct Sound
    {
        static constexpr auto name = "dispatch_sound";

        static Port<TS<Str>> compose(Wiring &w, Port<void> animal)
        {
            return wire<stdlib::getattr_, TS<Str>>(w, animal, Str{"sound"});
        }
    };

    struct SoundWithCount
    {
        static constexpr auto name = "dispatch_sound_with_count";

        static Port<TS<Str>> compose(
            Wiring &w, Port<void> animal, NamedPort<"count", TS<Int>> count)
        {
            static_cast<void>(count);
            return wire<stdlib::getattr_, TS<Str>>(w, animal, Str{"sound"});
        }
    };

    struct DowncastRefSoundGraph
    {
        static constexpr auto name = "downcast_ref_sound_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Animal>> animal)
        {
            const auto types = register_dispatch_types();
            auto source_ref = animal.template as<REF<TS<Animal>>>();

            WiringArg input;
            input.kind = WiringArg::Kind::TimeSeries;
            input.port = source_ref.erased();
            std::array<WiringArg, 1> inputs{std::move(input)};

            auto &registry = TypeRegistry::instance();
            const auto *dog_ref = registry.ref(registry.ts(types.dog));
            OperatorWireResult narrowed = wire_operator(
                w, "downcast_ref", std::span<const WiringArg>{inputs}, true, dog_ref);
            if (!narrowed.has_output)
            {
                throw std::logic_error("downcast_ref did not produce an output");
            }
            return wire<stdlib::getattr_, TS<Str>>(w, narrowed.output, Str{"sound"});
        }
    };

    struct DowncastRefValueGraph
    {
        static constexpr auto name = "downcast_ref_value_graph";

        static Port<void> compose(Wiring &w, Port<TS<Animal>> animal)
        {
            const auto types = register_dispatch_types();
            auto source_ref = animal.template as<REF<TS<Animal>>>();

            WiringArg input;
            input.kind = WiringArg::Kind::TimeSeries;
            input.port = source_ref.erased();
            std::array<WiringArg, 1> inputs{std::move(input)};

            auto &registry = TypeRegistry::instance();
            const auto *dog_ts = registry.ts(types.dog);
            const auto *dog_ref = registry.ref(dog_ts);
            OperatorWireResult narrowed = wire_operator(
                w, "downcast_ref", std::span<const WiringArg>{inputs}, true, dog_ref);
            if (!narrowed.has_output)
            {
                throw std::logic_error("downcast_ref did not produce an output");
            }

            // Present the REF output as its dereferenced schema. Input binding
            // inserts the reference adapter, matching Python's public Port view.
            WiringPortRef dereferenced = narrowed.output;
            dereferenced.schema = dog_ts;
            return Port<void>{w, std::move(dereferenced)};
        }
    };

    struct EmptyDowncastRefValueGraph
    {
        static constexpr auto name = "empty_downcast_ref_value_graph";

        static Port<void> compose(Wiring &w, Port<TS<EmptyAnimal>> animal)
        {
            const auto types = register_empty_dispatch_types();
            auto source_ref = animal.template as<REF<TS<EmptyAnimal>>>();
            WiringArg input{.kind = WiringArg::Kind::TimeSeries, .port = source_ref.erased()};
            std::array<WiringArg, 1> inputs{std::move(input)};

            auto &registry = TypeRegistry::instance();
            const auto *dog_ts = registry.ts(types.dog);
            OperatorWireResult narrowed = wire_operator(
                w, "downcast_ref", std::span<const WiringArg>{inputs}, true,
                registry.ref(dog_ts));
            WiringPortRef dereferenced = narrowed.output;
            dereferenced.schema = dog_ts;
            return Port<void>{w, std::move(dereferenced)};
        }
    };

    template <fixed_string Text, std::size_t Arity>
    struct ConstantBranch;

    template <fixed_string Text>
    struct ConstantBranch<Text, 1>
    {
        static constexpr auto name = Text;

        static Port<TS<Str>> compose(Wiring &w, Port<void>)
        {
            return wire<stdlib::const_, TS<Str>>(w, Str{Text.sv()});
        }
    };

    template <fixed_string Text>
    struct ConstantBranch<Text, 2>
    {
        static constexpr auto name = Text;

        static Port<TS<Str>> compose(Wiring &w, Port<void>, Port<void>)
        {
            return wire<stdlib::const_, TS<Str>>(w, Str{Text.sv()});
        }
    };

    using DefaultBranch = ConstantBranch<"default", 1>;
    using AnimalFoodBranch = ConstantBranch<"animal-food", 2>;
    using DogFoodBranch = ConstantBranch<"dog-food", 2>;
    using DogMeatBranch = ConstantBranch<"dog-meat", 2>;
    using CatBranch = ConstantBranch<"cat", 2>;
    using LeftBranch = ConstantBranch<"left", 1>;
    using RightBranch = ConstantBranch<"right", 1>;

    struct CapturedDispatchContext
    {
        static constexpr auto name = "captured_dispatch_context";

        static Port<TS<Str>> compose(Wiring &w, Port<void>)
        {
            return context::get<TS<Str>>(w, "label");
        }
    };

    struct DispatchContextGraph
    {
        static constexpr auto name = "dispatch_context_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Animal>> animal,
                                     Port<TS<Str>> label,
                                     Scalar<"cases", stdlib::DispatchCases> cases)
        {
            context::scope<"label"> ctx{w, label};
            return wire<stdlib::dispatch_>(w, cases.value(), animal).as<TS<Str>>();
        }
    };

    struct RefDispatchGraph
    {
        static constexpr auto name = "ref_dispatch_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Animal>> animal,
                                     Scalar<"cases", stdlib::DispatchCases> cases)
        {
            auto source_ref = animal.template as<REF<TS<Animal>>>();
            return wire<stdlib::dispatch_>(w, cases.value(), source_ref).as<TS<Str>>();
        }
    };

    using DispatchStructuralResult =
        UnNamedTSB<Field<"id", TS<Int>>, Field<"sound", TS<Str>>>;
    using DispatchRepository =
        TSB<"DispatchRepository",
            Field<"lhs", TS<Int>>,
            Field<"rhs", TS<Int>>>;

    struct StructuralSound
    {
        static constexpr auto name = "dispatch_structural_sound";

        static Port<DispatchStructuralResult> compose(
            Wiring &w, Port<void> animal)
        {
            auto id = wire<stdlib::getattr_, TS<Int>>(
                w, animal, Str{"id"});
            auto sound = wire<stdlib::getattr_, TS<Str>>(
                w, animal, Str{"sound"});
            return stdlib::to_tsb<DispatchStructuralResult>(w, id, sound);
        }
    };

    struct StructuralDispatchGraph
    {
        static constexpr auto name = "structural_dispatch_graph";

        static Port<TS<Str>> compose(
            Wiring &w, Port<TS<Animal>> animal,
            Scalar<"cases", stdlib::DispatchCases> cases)
        {
            auto result = wire<stdlib::dispatch_>(w, cases.value(), animal)
                              .as<DispatchStructuralResult>();
            return wire<stdlib::getitem_>(w, result, Str{"sound"})
                .as<TS<Str>>();
        }
    };

    struct SumRepository
    {
        static constexpr auto name = "sum_dispatch_repository";

        static void eval(
            In<"repository", DispatchRepository> repository,
            Out<TS<Int>> out)
        {
            const auto lhs = repository.field<"lhs">();
            const auto rhs = repository.field<"rhs">();
            if (lhs.valid() && rhs.valid())
            {
                out.set(lhs.value() + rhs.value());
            }
        }
    };

    struct ApplyDogToRepository
    {
        static constexpr auto name = "apply_dog_to_repository";

        static Port<TS<Int>> compose(
            Wiring &w, Port<void>, Port<DispatchRepository> repository)
        {
            return wire<SumRepository>(w, repository);
        }
    };

    struct StructuralInputDispatchGraph
    {
        static constexpr auto name = "structural_input_dispatch_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TS<Animal>> animal, Port<TS<Int>> lhs,
            Port<TS<Int>> rhs,
            Scalar<"cases", stdlib::DispatchCases> cases)
        {
            auto repository =
                stdlib::to_tsb<DispatchRepository>(w, lhs, rhs);
            return wire<stdlib::dispatch_>(
                       w, cases.value(), animal, repository)
                .as<TS<Int>>();
        }
    };
}

TEST_CASE("dispatch_: C++ wiring selects exact and inherited Bundle cases")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();

    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.dog, fn<SoundWithCount>()),
        stdlib::dispatch_case(types.cat, fn<SoundWithCount>()),
    });

    CHECK_OUTPUT(
        (testing::eval_node<stdlib::dispatch_, TS<Animal>>(
            cases,
            testing::values<Value>(
                dog_value(types, 1, "woof"),
                puppy_value(types, 2, "yip"),
                cat_value(types, 3)),
            arg<"count">(testing::values<Int>(1, 2, 3)))),
        string_values({"woof", "yip", "meow"}));
}

TEST_CASE("dispatch_: composed structural branch outputs retain their public schema")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();

    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.dog, fn<StructuralSound>()),
        stdlib::dispatch_case(types.cat, fn<StructuralSound>()),
    });

    CHECK_OUTPUT(
        (testing::eval_node<StructuralDispatchGraph>(
            testing::values<Value>(dog_value(types, 1, "woof"),
                                   cat_value(types, 2)),
            cases)),
        testing::values<Str>(Str{"woof"}, Str{"meow"}));
}

TEST_CASE("dispatch_: compiled branches preserve structural TSB inputs")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();
    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.dog, fn<ApplyDogToRepository>()),
    }).on({0});

    CHECK_OUTPUT(
        testing::eval_node<StructuralInputDispatchGraph>(
            testing::values<Value>(dog_value(types, 1, "woof")),
            testing::values<Int>(2), testing::values<Int>(3),
            arg<"cases">(cases)),
        testing::values<Int>(5));
}

TEST_CASE("dispatch_: compiled branches import an enclosing context")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();
    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.dog, fn<CapturedDispatchContext>()),
    });

    CHECK_OUTPUT(
        testing::eval_node<DispatchContextGraph>(
            testing::values<Value>(dog_value(types, 1, "woof"), testing::none,
                                   dog_value(types, 2, "bark")),
            testing::values<Str>(Str{"alpha"}, Str{"beta"}, testing::none),
            arg<"cases">(cases)),
        testing::values<Str>(Str{"alpha"}, Str{"beta"}, testing::none));
}

TEST_CASE("dispatch_: C++ wiring dereferences selected REF inputs")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();
    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.dog, fn<Sound>()),
        stdlib::dispatch_case(types.cat, fn<Sound>()),
    });

    CHECK_OUTPUT(
        testing::eval_node<RefDispatchGraph>(
            testing::values<Value>(dog_value(types, 1, "woof"), cat_value(types, 2)),
            arg<"cases">(cases)),
        testing::values<Str>(Str{"woof"}, Str{"meow"}));
}

TEST_CASE("downcast_ref: C++ wiring reads a registered derived Bundle without materializing it")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();

    CHECK_OUTPUT(
        testing::eval_node<DowncastRefSoundGraph>(testing::values<Value>(
            dog_value(types, 1, "woof"), puppy_value(types, 2, "yip"))),
        testing::values<Str>("woof", "yip"));
}

TEST_CASE("downcast_ref: C++ eval_node records the concrete derived Bundle")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();

    const auto dog = dog_value(types, 1, "woof");
    const auto puppy = puppy_value(types, 2, "yip");
    CHECK_OUTPUT(
        testing::eval_node<DowncastRefValueGraph>(testing::values<Value>(dog, puppy)),
        testing::values<Value>(dog, puppy));
}

TEST_CASE("downcast_ref: C++ eval_node records zero-storage derived Bundles")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_empty_dispatch_types();
    const Value dog{ValuePlanFactory::instance().type_for(types.dog)};
    const Value puppy{ValuePlanFactory::instance().type_for(types.puppy)};

    CHECK_OUTPUT(
        testing::eval_node<EmptyDowncastRefValueGraph>(testing::values<Value>(dog, puppy)),
        testing::values<Value>(dog, puppy));
}

TEST_CASE("dispatch_: C++ wiring chooses the most-specific multi-argument case")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();

    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case({types.animal, types.food}, fn<AnimalFoodBranch>()),
        stdlib::dispatch_case({types.dog, types.food}, fn<DogFoodBranch>()),
        stdlib::dispatch_case({types.dog, types.meat}, fn<DogMeatBranch>()),
    }).on({0, 1});

    CHECK_OUTPUT(
        (testing::eval_node<stdlib::dispatch_, TS<Animal>, TS<Food>>(
            cases,
            testing::values<Value>(
                dog_value(types, 1, "woof"),
                dog_value(types, 2, "woof"),
                cat_value(types, 3)),
            testing::values<Value>(
                kibble_value(types, 1, 2),
                meat_value(types, 2, true),
                meat_value(types, 3, false)))),
        string_values({"dog-food", "dog-meat", "animal-food"}));
}

TEST_CASE("dispatch_: C++ wiring supports default and no-match behavior")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();
    const auto bird = bird_value(types, 4);

    const auto with_default = stdlib::dispatch_cases(
        {stdlib::dispatch_case(types.dog, fn<Sound>())}, fn<DefaultBranch>());
    CHECK_OUTPUT(
        (testing::eval_node<stdlib::dispatch_, TS<Animal>>(
            with_default, testing::values<Value>(bird))),
        string_values({"default"}));

    const auto without_default = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.dog, fn<Sound>()),
    });
    REQUIRE_THROWS_WITH(
        (testing::eval_node<stdlib::dispatch_, TS<Animal>>(
            without_default, testing::values<Value>(bird))),
        Catch::Matchers::ContainsSubstring("No suitable overload"));
}

TEST_CASE("dispatch_: C++ wiring can restrict dispatch to selected arguments")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();

    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.cat, fn<CatBranch>()),
    }).on({0});

    CHECK_OUTPUT(
        (testing::eval_node<stdlib::dispatch_, TS<Animal>, TS<Food>>(
            cases,
            testing::values<Value>(cat_value(types, 1)),
            testing::values<Value>(meat_value(types, 2, true)))),
        string_values({"cat"}));
}

TEST_CASE("dispatch_: C++ wiring rejects ambiguous multiple inheritance")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto types = register_dispatch_types();

    const auto cases = stdlib::dispatch_cases({
        stdlib::dispatch_case(types.left, fn<LeftBranch>()),
        stdlib::dispatch_case(types.right, fn<RightBranch>()),
    });
    const auto both = both_value(types, 1);

    REQUIRE_THROWS_WITH(
        (testing::eval_node<stdlib::dispatch_, TS<Animal>>(
            cases, testing::values<Value>(both))),
        Catch::Matchers::ContainsSubstring("Ambiguous dispatch"));
}
