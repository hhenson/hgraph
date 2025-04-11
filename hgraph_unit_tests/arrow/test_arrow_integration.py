from hgraph import eq_, not_, replace, format_, split, TS, add_
from hgraph.arrow import eval_, assert_, a, flatten_tsl, Pair
from hgraph.arrow._pair_operators import flatten_tsb


def test_native_hgraph_integration():
    """Ensure operator to operator integration works."""
    eval_("a", "b") | eq_ >> not_ >> assert_(True)


def test_tuple_unpacking():
    """Ensure tuple unpacking works."""
    eval_("a", ("z", "abcabcabc")) | replace >> assert_("zbczbczbc")


def test_binding_parameters():
    """Ability to bind a node with parameters"""
    eval_(["a, b"]) | a(split)(separator=", ") >> assert_(("a", "b"))


def test_flatten_to_tsl():
    """Ensure that the arrow can be flattened to TSL"""
    eval_((1, (2, 3)), (4, (5, 6))) | flatten_tsl >> assert_((1, 2, 3, 4, 5, 6))


def test_flatten_to_tsb():
    """Ensure that the arrow can be flattened to TSB"""
    eval_(1, "A") | flatten_tsb({"a": TS[int], "b": TS[str]}) >> assert_({"a": 1, "b": "A"})


def test_bind_tsb_to_node():
    """When there is on TSB shaped input, bind it to the nodes input"""
    eval_(1, 2) | flatten_tsb({"lhs": TS[int], "rhs": TS[int]}) >> add_ >> assert_(3)

