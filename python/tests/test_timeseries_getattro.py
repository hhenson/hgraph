"""The native ``tp_getattro`` slot on the TimeSeries runtime view.

Bundle fields are served by name before the generic lookup when the name is
not an attribute of the type; the set of type attribute names is cached and
must follow the type's ``__dict__`` when it changes after first use.
"""

import hgraph as hg
from hgraph import TS, TSB, TimeSeriesSchema, eval_node, sink_node


class Quote(TimeSeriesSchema):
    price: TS[int]
    size: TS[int]


def _collect(seen):
    @sink_node
    def observe(quote: TSB[Quote]):
        price = quote.price
        seen.append(price if isinstance(price, str) else price.value)

    return observe


def test_bundle_field_served_by_name():
    seen = []
    eval_node(_collect(seen), [dict(price=1, size=10), dict(price=2, size=20)])
    assert seen == [1, 2]


def test_missing_bundle_field_raises_attribute_error():
    @sink_node
    def observe(quote: TSB[Quote]):
        quote.no_such_field

    try:
        eval_node(observe, [dict(price=1, size=10)])
    except Exception as exc:  # noqa: BLE001 - the engine may wrap the node error
        assert "no_such_field" in str(exc)
    else:
        raise AssertionError("expected an AttributeError for a missing bundle field")


def test_type_attribute_added_after_first_use_wins_over_bundle_field():
    """Monkeypatching the view type after the name set was built must be seen."""
    seen = []
    eval_node(_collect(seen), [dict(price=1, size=10)])  # builds the cached name set
    assert seen == [1]

    hg.TimeSeries.price = property(lambda self: "shadowed")
    try:
        seen.clear()
        eval_node(_collect(seen), [dict(price=3, size=30)])
        assert seen == ["shadowed"], seen
    finally:
        del hg.TimeSeries.price

    seen.clear()
    eval_node(_collect(seen), [dict(price=5, size=50)])
    assert seen == [5], seen
