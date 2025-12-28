# Value

## Overview

1. Type erased data storage.
2. Type controlled by type schema.
3. Type schema will support resolved HgTypeMetaData (python).
4. Using the schema, the value will be able to allocate memory upfront to store value.
5. Type schema supports nesting.
6. Type schema supports type registration and lookup.
7. The value is able to be accessed at various levels of nesting.

## Schema

1. Review HgScalarTypeMetadata to see typical types that need to be supported.
2. The schema is extensible, i.e. new types can be added, including in sub-modules.
3. The schema is comparable, that is it supports the concept of instanceof / ==
4. The schema supports:
    - atomic values (which have no nesting)
    - bundle / struct values
    - lists of nestable values
    - maps of hashable keys and nestable values
    - sets of hashable values
5. Associated with the schema are traits / functions that operate on the value.
6. The basic traits are:
    - equality, support C++ == operator and std::equal_to
    - to_string
    - to_python, from_python
    - instance_of / type equality
7. Hashable values:
    - hash_value -> size_t, supports integration with C++ hashable.
8. Comparable:
    - less_than -> bool, supports C++ < operator and std::less
9. Iterable:
    - iterate -> iterator, supports C++ range-based for loop and std::begin/end
    - size -> size_t,
10. Indexable (extends Iterable):
    - get(size_t) -> value_type, []
    - set(size_t, value) -> void, supports C++ assignment operator and std::assign
11. Bundle (extends Indexable):
    - get(string) -> value_type,:
    - get(string) -> value_type,
    - set(string, value) -> void,
11. Set (extends Iterable):
    - insert -> void, add element to set
    - erase -> void, remove element from set
    - contains -> bool, check if element is in set
12. Map (extends Set, but works with pairs of key/value):
    - insert -> void, add element to map
    - erase -> void, remove element from map
    - contains -> bool, check if element is in map
    - get -> value_type, []

There needs to be a type register to be able to add types and new types to the system.

# Value

The value is a vector of char, which is indexed by the schmea. Thus, the schema is required to know the size of the value.
This will support full allocation when the values are not dynamically sized.
When dynamically sized, we will allocate additional storage at the end of the value preferably, although it could use a
dynamically allocated structure. This affects maps, sets and dynamically sized lists as well as types such as strings.

It would be useful if the type system could support polymorphic memory allocation or some form of memory buffer. This 
requirement should be balanced with the complexity of implementation.

# Views

The value is a multi-level structure, but a user should be able to access the value at each level with limited
overhead. The view holds a reference to the value from the index where the value begins, thus the memory allocation
must be nested as well, so as we down the hierachy we can safely wrap the memory and sub-schema to it will appear to
the top level value.

It is useful to be able to access to the root of the value as well. This is especially useful when we are needing to 
implement the notification system, this actually needs use to be able to chain upwards through the schema.

# Modification

The value should support in-place modification of the value wherever possible. The objective is to avoid unnecessary 
allocation.

# Type access

We need some way to access the value in a type-safe way, something like the visit approach in std::any.
We need to be able to do nested structure visiting (see https://github.com/uentity/ddvisitor and the associated https://github.com/uentity/tpack)
I don't think the above will actually work in this case, but gives some ideas of what we may be able to do.

# GOALS

Other goals to focus on:
- Memory co-location and support for cache lines.
- buffer support for python exposure. This would be implemented in the to/from python functions.
- Where possible, using a structure that supports arrow formatted memory.

# Testing

- Unit tests in both C++ and python.
- Ensure testing covers simple and complex structures.
- Test all edge cases, ensure no memory leaks or buffer overruns.

# Documentation

- I want user guides with overview, examples, and API documentation.
- I want to have a design document that explains the design choices.

# References

- [Arrow Format](https://arrow.apache.org/docs/format/Columnar.html)
- [Flatbuffers](https://google.github.io/flatbuffers/)
- Type erasure, Microsoft has an interesting take on type erasure and providing behaviour.
- Look at Flux for interation [Flux](https://github.com/tcbrindle/flux)

# Future work

This will need to be extended to support change tracking and notification when we create the TSValue.
The TSValue will use the same schema as the value, I don't want to have multiple copies of the schema.


# Design Principles:
- Type erasure
- Type safe
- Memory co-location
- Support for cache lines
- Composition instead of inheritance.
- Reduce memory churn


