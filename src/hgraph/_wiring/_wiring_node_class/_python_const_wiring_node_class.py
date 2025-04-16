from contextlib import nullcontext
from typing import TYPE_CHECKING, TypeVar, Any, NamedTuple

from hgraph import (
    HgTypeMetaData,
    WiringContext,
    WiringNodeInstanceContext,
    generator,
    with_signature,
    EvaluationEngineApi,
)
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    BaseWiringNodeClass,
    validate_and_resolve_signature,
)

if TYPE_CHECKING:
    pass

__all__ = ("PythonConstWiringNodeClass",)


ValueTuple = NamedTuple("ValueTuple", [("value", Any)])


class PythonConstWiringNodeClass(BaseWiringNodeClass):

    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
        __return_sink_wp__: bool = False,
        **kwargs,
    ) -> "WiringPort":
        if WiringNodeInstanceContext.instance() is None:
            wiring_node_context = WiringNodeInstanceContext()
            in_graph = False
        else:
            wiring_node_context = nullcontext()
            in_graph = True

        with wiring_node_context, WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, _ = validate_and_resolve_signature(
                self.signature,
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                __enforce_output_type__=False,
                **kwargs,
            )
            if in_graph:
                if resolved_signature.injectables:
                    # wrap this as a generator
                    @generator
                    @with_signature(
                        args={
                            k: resolved_signature.input_types[k].py_type
                            for k in resolved_signature.args
                            if k not in resolved_signature.defaults
                        },
                        kwargs={
                            k: v.py_type
                            for k, v in resolved_signature.input_types.items()
                            if k in resolved_signature.defaults and k != "_api"
                        },
                        defaults=resolved_signature.defaults,
                        return_annotation=resolved_signature.output_type.py_type,
                    )
                    def const_fn_generator(*args, _api: EvaluationEngineApi = None, **kwargs):
                        if resolved_signature.uses_engine and "_api" in resolved_signature.args:
                            kwargs["_api"] = _api
                        yield _api.start_time, self.fn(*args, **kwargs)

                    return const_fn_generator(**kwargs_)
                else:
                    from hgraph import const

                    out = const(self.fn(**kwargs_), tp=resolved_signature.output_type.py_type)
                    return out
            else:
                # Call this as a function
                return ValueTuple(value=self.fn(**kwargs_))
