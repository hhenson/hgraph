"""
Traits provide the ability to capture attributes about a graph that can be accessed by
a node at start / eval time.
"""


class Traits:
    """
    Contains a collection of traits. The trait is a nestable structure, with a chained lookup.
    Thus, a resolution of a trait will bubble up to the chain of traits.
    """

    def __init__(self, parent: "Traits" = None, **kwargs):
        self._parent = parent
        self._traits = dict(kwargs)

    def set_traits(self, **kwargs):
        self._traits.update(kwargs)

    def get_trait(self, trait: str):
        if trait in self._traits:
            return self._traits[trait]
        elif self._parent is not None:
            return self._parent.get_traits(trait)
        else:
            raise ValueError(f"Trait {trait} not found")

    def get_trait_or(self, trait: str, default=None):
        return self._traits.get(trait, default)

    def copy(self) -> "Traits":
        return type(self)(self._parent, **self._traits)
