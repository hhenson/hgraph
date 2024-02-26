from hgraph import graph, TS, CompoundScalar
from hgraph import dispatch_, dispatch
from hgraph.nodes import format_, cast_
from hgraph.test import eval_node


def test_dispatch_1():
    class Pet(CompoundScalar): ...
    class Dog(Pet): ...
    class Cat(Pet): ...

    @dispatch
    def pet_sound(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return format_("unknown {}", cast_(str, count))

    @graph(overloads=pet_sound)
    def pet_sound_dog(pet: TS[Dog], count: TS[int]) -> TS[str]:
        return "woof"

    @graph(overloads=pet_sound)
    def pet_sound_cat(pet: TS[Cat], count: TS[int]) -> TS[str]:
        return "meow"

    @graph
    def make_sound(pet: TS[Pet], count: TS[int]) -> TS[str]:
        return pet_sound(pet, count)

    assert (eval_node(make_sound, [None, Dog(), None, Cat(), Pet(), None], [None, 1, None, None, 2, 3])
            == [None, "woof", None, "meow", "unknown 2", "unknown 3"])


def test_dispatch_2():
    class Animal(CompoundScalar): ...
    class Cat(Animal): ...
    class Cow(Animal): ...
    class Bear(Animal): ...

    class Food(CompoundScalar): ...
    class Plant(Food): ...
    class Meat(Food): ...

    @graph
    def eats(animal: TS[Animal], food: TS[Food]) -> TS[bool]:
        return False

    @graph(overloads=eats)
    def cat_eats_meat(animal: TS[Cat], food: TS[Meat]) -> TS[bool]:
        return True

    @graph(overloads=eats)
    def cat_eats_meat_only(animal: TS[Cat], food: TS[Plant]) -> TS[bool]:
        return False

    @graph(overloads=eats)
    def cow_eats_grass(animal: TS[Cow], food: TS[Plant]) -> TS[bool]:
        return True

    @graph(overloads=eats)
    def cow_eats_grass_only(animal: TS[Cow], food: TS[Food]) -> TS[bool]:
        return False

    @graph(overloads=eats)
    def bear_eats_everything(animal: TS[Bear], food: TS[Food]) -> TS[bool]:
        return True

    @graph
    def eat(animal: TS[Animal], food: TS[Food]) -> TS[bool]:
        return dispatch_(eats, animal, food)

    assert (eval_node(eat,
                      [None, Cat(),   None,   Cow(),   None,   Bear()],
                      [None, Plant(), Meat(), Plant(), Meat(), Plant(), Meat()]) ==
                      [None, False,   True,   True,    False,  True,    None])

    # Note: last tick is None because there is no change in key value - there is only one Bear related overload and
    # hence the implementation graph is not swapped out so that const(True) node is not ticked.
