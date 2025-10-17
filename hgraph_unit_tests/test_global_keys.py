from hgraph._runtime._global_keys import (
    set_output_key_builder,
    DefaultOutputKeyBuilder,
    output_key,
    output_subscriber_key,
    context_output_key,
    component_key,
    reset_output_key_builder
)


def test_default_output_key_builder_matches_existing_formats():
    b = DefaultOutputKeyBuilder()
    assert b.output_key("svc://path") == "svc://path"
    assert b.output_subscriber_key("svc://path") == "svc://path_subscriber"
    assert b.context_output_key((1, 2), "ctx") == "context-(1, 2)-ctx"
    assert b.component_key("abc") == "component::abc"
    assert b.component_key(123) == "component::123"


def test_set_and_get_output_key_builder_customization():
    class Custom(DefaultOutputKeyBuilder):
        def output_key(self, path: str) -> str:
            return f"custom::{path}"

    try:
        set_output_key_builder(Custom())
        assert output_key("x") == "custom::x"
        # Ensure other methods still work (from Custom via inheritance)
        assert output_subscriber_key("x") == "x_subscriber"
        assert context_output_key((9,), "p") == "context-(9,)-p"
        assert component_key("c") == "component::c"
    finally:
        # restore
        reset_output_key_builder()  # type: ignore
