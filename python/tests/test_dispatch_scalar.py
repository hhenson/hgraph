"""Dispatch coverage beyond the upstream ported wiring cases."""
import inspect
from dataclasses import dataclass
from typing import Type, Union

import pytest

from hgraph import (
    AUTO_RESOLVE,
    OUT,
    CompoundScalar,
    TS,
    TSB,
    TimeSeriesSchema,
    combine,
    compute_node,
    const,
    dispatch,
    dispatch_,
    downcast_,
    downcast_ref,
    graph,
    operator,
    switch_,
)
from hgraph.test import eval_node


class Pet: ...


class Dog(Pet): ...


class Cat(Pet): ...


def test_dispatch_decorator():
    @compute_node
    def sound_default(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return f"unknown {count.value}"

    @dispatch
    def pet_sound(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return sound_default(pet, count)

    @graph(overloads=pet_sound)
    def pet_sound_dog(pet: TS[Dog], count: TS[int]) -> TS[str]:
        return "woof"

    @graph(overloads=pet_sound)
    def pet_sound_cat(pet: TS[Cat], count: TS[int]) -> TS[str]:
        return "meow"

    @graph
    def make_sound(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return pet_sound(pet, count)

    assert pet_sound_dog in pet_sound.overloads
    assert pet_sound_cat in pet_sound.overloads
    assert eval_node(
        make_sound, [None, Dog(), None, Cat(), Pet(), None], [None, 1, None, None, 2, 3]
    ) == [None, "woof", None, "meow", "unknown 2", "unknown 3"]


def test_dispatch_fn_multi():
    class Food: ...

    class Plant(Food): ...

    class Meat(Food): ...

    @operator
    def eats(animal: TS[Pet], food: TS[Food]) -> TS[bool]: ...

    @graph(overloads=eats)
    def eats_default(animal: TS[Pet], food: TS[Food]) -> TS[bool]:
        return False

    @graph(overloads=eats)
    def cat_eats_meat(animal: TS[Cat], food: TS[Meat]) -> TS[bool]:
        return True

    @graph(overloads=eats)
    def dog_eats_everything(animal: TS[Dog], food: TS[Food]) -> TS[bool]:
        return True

    @graph
    def eat(animal: TS[Pet], food: TS[Food]) -> TS[bool]:
        return dispatch_(eats, animal, food)

    assert eval_node(
        eat,
        [None, Cat(), None, Dog(), Cat()],
        [None, Plant(), Meat(), Plant(), Meat()],
    ) == [None, False, True, True, True]


def test_dispatch_preserves_a_structurally_combined_tsb_argument():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    class Repository(TimeSeriesSchema):
        lhs: TS[int]
        rhs: TS[int]

    @operator
    def apply(animal: TS[Animal], repository: TSB[Repository]) -> TS[int]: ...

    @compute_node(overloads=apply)
    def apply_dog(animal: TS[Dog], repository: TSB[Repository]) -> TS[int]:
        return repository.as_schema.lhs.value + repository.as_schema.rhs.value

    @graph
    def app(animal: TS[Animal], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        repository = combine[TSB[Repository]](lhs=lhs, rhs=rhs)
        return dispatch_(apply, animal, repository)

    assert eval_node(app, [Dog()], [2], [3]) == [5]


def test_compound_scalar_dispatch_to_compute_node_overload():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    class Puppy(Dog): ...

    @operator
    def sound(animal: TS[Animal]) -> TS[str]: ...

    @compute_node(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TS[str]:
        return type(animal.value).__name__

    @graph
    def app(animal: TS[Animal]) -> TS[str]:
        return dispatch_(sound, animal)

    assert eval_node(app, [Dog(), Puppy()]) == ["Dog", "Puppy"]


def test_dispatch_uses_declared_concrete_output_schema():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    class Result(TimeSeriesSchema):
        sound: TS[str]

    @dispatch
    @operator
    def sound(animal: TS[Animal]) -> TSB[Result]: ...

    @graph(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TSB[Result]:
        return TSB[Result].from_ts(sound="woof")

    @graph
    def app(animal: TS[Animal]) -> TSB[Result]:
        return sound(animal)

    @graph
    def projected(animal: TS[Animal]) -> TS[str]:
        return sound(animal).sound

    assert eval_node(app, [Dog()]) == [{"sound": "woof"}]
    assert eval_node(projected, [Dog()]) == ["woof"]


def test_union_overload_is_registered_for_direct_operator_dispatch():
    class Food(CompoundScalar): ...

    class Plant(Food): ...

    class Meat(Food): ...

    @operator
    def eat(food: TS[Food]) -> TS[str]: ...

    @graph(overloads=eat)
    def eat_known(food: Union[TS[Plant], TS[Meat]]) -> TS[str]:
        return "yes"

    @graph
    def app(food: TS[Plant]) -> TS[str]:
        return eat(food)

    assert eval_node(app, [Plant()]) == ["yes"]


def test_dispatch_preserves_scalar_requirements_and_ts_defaults():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    @operator
    def sound(animal: TS[Animal], upper: bool, count: TS[int] = None) -> TS[str]: ...

    @graph(overloads=sound, requires=lambda m, upper: upper)
    def dog_upper(animal: TS[Dog], upper: bool, count: TS[int] = None) -> TS[str]:
        return "WOOF"

    @graph(overloads=sound, requires=lambda m, upper: not upper)
    def dog_lower(animal: TS[Dog], upper: bool, count: TS[int] = None) -> TS[str]:
        return "woof"

    @graph
    def app(animal: TS[Animal]) -> TS[str]:
        return dispatch_(sound, animal, upper=True)

    assert eval_node(app, [Dog()]) == ["WOOF"]


def test_dispatch_reports_no_matching_runtime_class():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    class Cat(Animal): ...

    @operator
    def sound(animal: TS[Animal]) -> TS[str]: ...

    @graph(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TS[str]:
        return "woof"

    @graph
    def app(animal: TS[Animal]) -> TS[str]:
        return dispatch_(sound, animal)

    with pytest.raises(RuntimeError, match="No suitable overload"):
        eval_node(app, [Cat()])


def test_dispatch_reports_ambiguous_multiple_inheritance():
    class Animal(CompoundScalar): ...

    class Left(Animal): ...

    class Right(Animal): ...

    class Hybrid(Left, Right): ...

    @operator
    def sound(animal: TS[Animal]) -> TS[str]: ...

    @graph(overloads=sound)
    def left_sound(animal: TS[Left]) -> TS[str]:
        return "left"

    @graph(overloads=sound)
    def right_sound(animal: TS[Right]) -> TS[str]:
        return "right"

    @graph
    def app(animal: TS[Animal]) -> TS[str]:
        return dispatch_(sound, animal)

    with pytest.raises(RuntimeError, match="Ambiguous dispatch"):
        eval_node(app, [Hybrid()])


def test_dispatch_on_restricts_dynamic_parameters():
    class Animal(CompoundScalar): ...

    class Cat(Animal): ...

    class Food(CompoundScalar): ...

    class Meat(Food): ...

    @dispatch(on="animal")
    def eat(animal: TS[Animal], food: TS[Food]) -> TS[str]:
        return "default"

    @graph(overloads=eat)
    def cat_meat(animal: TS[Cat], food: TS[Meat]) -> TS[str]:
        return "specific meat"

    @graph(overloads=eat)
    def cat_food(animal: TS[Cat], food: TS[Food]) -> TS[str]:
        return "cat"

    @graph
    def app(animal: TS[Animal], food: TS[Food]) -> TS[str]:
        return eat(animal, food)

    assert eval_node(app, [Cat()], [Meat()]) == ["cat"]


def test_dispatch_preserves_keyword_only_ts_parameters():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    @dispatch
    def sound(animal: TS[Animal], *, count: TS[int]) -> TS[str]:
        return "default"

    @graph(overloads=sound)
    def dog_sound(animal: TS[Dog], *, count: TS[int]) -> TS[str]:
        return "woof"

    @graph
    def app(animal: TS[Animal], count: TS[int]) -> TS[str]:
        return sound(animal, count=count)

    assert eval_node(app, [Dog()], [1]) == ["woof"]


def test_compound_scalar_dispatch_propagates_specialized_output_to_overload():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    @dispatch
    @operator
    def value(animal: TS[Animal]) -> OUT: ...

    @graph(overloads=value)
    def dog_value(animal: TS[Dog], tp: Type[OUT] = AUTO_RESOLVE) -> OUT:
        return const(7, tp)

    @graph
    def app(animal: TS[Animal]) -> TS[int]:
        return value[TS[int]](animal)

    assert eval_node(app, [Dog()]) == [7]


def test_compound_scalar_dispatch_can_be_a_switch_branch():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    @dispatch
    def sound(animal: TS[Animal]) -> TS[str]:
        return "default"

    @graph(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TS[str]:
        return "woof"

    @graph
    def app(enabled: TS[bool], animal: TS[Dog]) -> TS[str]:
        return switch_(enabled, {True: sound, False: sound}, animal)

    assert eval_node(app, [True], [Dog()]) == ["woof"]


def test_compound_scalar_dispatch_accepts_statically_narrower_input():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    @dispatch
    def sound(animal: TS[Animal]) -> TS[str]:
        return "default"

    @graph(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TS[str]:
        return "woof"

    @graph
    def app(animal: TS[Dog]) -> TS[str]:
        return sound(animal)

    assert eval_node(app, [Dog()]) == ["woof"]


def test_dispatch_preserves_python_structured_parent_through_multiple_inheritance():
    @dataclass(frozen=True)
    class Animal:
        name: str

    @dataclass(frozen=True)
    class Tagged:
        tag: int

    @dataclass(frozen=True)
    class Dog(Tagged, Animal):
        pass

    @dispatch
    def sound(animal: TS[Animal]) -> TS[str]:
        return "default"

    @graph(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TS[str]:
        return "woof"

    @graph
    def app(animal: TS[Animal]) -> TS[str]:
        return sound(animal)

    assert eval_node(app, [Dog(name="Fido", tag=1)]) == ["woof"]


def test_compound_scalar_multi_dispatch_does_not_expand_descendant_product():
    class Animal(CompoundScalar): ...

    class Food(CompoundScalar): ...

    class SelectedAnimal(Animal): ...

    class SelectedFood(Food): ...

    animals = tuple(
        type(
            f"DispatchScaleAnimal{i}",
            (Animal,),
            {"__module__": __name__},
        )
        for i in range(256)
    )
    foods = tuple(
        type(
            f"DispatchScaleFood{i}",
            (Food,),
            {"__module__": __name__},
        )
        for i in range(256)
    )

    @dispatch
    def choose(animal: TS[Animal], food: TS[Food]) -> TS[str]:
        return "base"

    @graph(overloads=choose)
    def choose_animal(animal: TS[SelectedAnimal], food: TS[Food]) -> TS[str]:
        return "animal"

    @graph(overloads=choose)
    def choose_both(animal: TS[SelectedAnimal], food: TS[SelectedFood]) -> TS[str]:
        return "both"

    @graph
    def app(animal: TS[Animal], food: TS[Food]) -> TS[str]:
        return choose(animal, food)

    assert eval_node(
        app,
        [animals[-1](), SelectedAnimal(), SelectedAnimal()],
        [foods[-1](), foods[-1](), SelectedFood()],
    ) == ["base", "animal", "both"]


def test_compound_scalar_downcast_rejects_the_wrong_active_leaf():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    class Cat(Animal): ...

    @graph
    def app(animal: TS[Animal]) -> TS[Dog]:
        return downcast_[TS[Dog]](animal)

    with pytest.raises(RuntimeError, match="active Bundle value does not match"):
        eval_node(app, [Cat()])


def test_compound_scalar_downcast_accepts_compatible_and_output_selected_syntax():
    @dataclass(frozen=True)
    class Animal(CompoundScalar):
        identifier: int

    @dataclass(frozen=True)
    class Dog(Animal):
        sound: str

    @graph
    def positional(animal: TS[Animal]) -> TS[Dog]:
        return downcast_(Dog, animal)

    @graph
    def keyword(animal: TS[Animal]) -> TS[Dog]:
        return downcast_(tp=Dog, ts=animal)

    @graph
    def output_selected(animal: TS[Animal]) -> TS[Dog]:
        return downcast_[TS[Dog]](animal)

    samples = [Dog(identifier=1, sound="woof"), Dog(identifier=2, sound="yip")]
    assert eval_node(positional, samples) == samples
    assert eval_node(keyword, samples) == samples
    assert eval_node(output_selected, samples) == samples
    assert tuple(inspect.signature(downcast_).parameters) == ("tp", "ts")
    assert tuple(inspect.signature(downcast_[TS[Dog]]).parameters) == ("ts",)


def test_compound_scalar_reference_downcast_uses_the_native_reference_operator():
    class Animal(CompoundScalar): ...

    class Dog(Animal): ...

    class Puppy(Dog): ...

    @graph
    def app(animal: TS[Animal]) -> TS[Dog]:
        return downcast_ref(Dog, animal)

    samples = [Dog(), Puppy()]
    assert [type(value) for value in eval_node(app, samples)] == [Dog, Puppy]
    assert [type(value) for value in eval_node(app, samples, __elide__=True)] == [Dog, Puppy]
