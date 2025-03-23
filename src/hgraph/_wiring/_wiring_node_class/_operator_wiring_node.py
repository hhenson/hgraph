import functools
from typing import Mapping, Any, TypeVar, Callable, TYPE_CHECKING, List, Tuple

from hgraph._types._generic_rank_util import scale_rank, combine_ranks
from hgraph._wiring._wiring_errors import WiringError
from hgraph._wiring._wiring_errors import WiringFailureError
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    WiringNodeClass,
    HgTypeMetaData,
    WiringNodeSignature,
    validate_and_resolve_signature,
    PreResolvedWiringNodeWrapper,
)
from hgraph._wiring._wiring_node_signature import WiringNodeType, AUTO_RESOLVE
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_utils import pretty_str_types

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._runtime._node import NodeSignature

__all_ = ("OperatorWiringNodeClass", "OverloadedWiringNodeHelper")


class OperatorWiringNodeClass(WiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        super().__init__(signature, fn)
        self._overload_helper: OverloadedWiringNodeHelper = OverloadedWiringNodeHelper(self)
        functools.update_wrapper(self, fn)

    def overload(self, other: "WiringNodeClass"):
        self._overload_helper.overload(other)

    def _check_overloads(self, *args, **kwargs) -> tuple[bool, "WiringPort"]:
        best_overload = self._overload_helper.get_best_overload(*args, **kwargs)
        best_overload: WiringNodeClass
        if best_overload is not self:
            return True, best_overload(*args, **kwargs)
        else:
            return False, None

    @property
    def overload_list(self):
        return self._overload_helper

    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
        __return_sink_wp__: bool = False,
        **kwargs,
    ) -> "WiringNodeInstance":
        found_overload, r = self._check_overloads(
            *args, **kwargs, __pre_resolved_types__=__pre_resolved_types__, __return_sink_wp__=__return_sink_wp__
        )
        if found_overload:
            return r
        else:
            raise NotImplementedError(f"No overload found for {repr(self)} and parameters: {args}, {kwargs}")

    def __getitem__(self, item) -> "WiringNodeClass":
        if item:
            return PreResolvedWiringNodeWrapper(
                signature=self.signature, fn=self.fn, underlying_node=self, resolved_types=self._convert_item(item)
            )
        else:
            return self

    def resolve_signature(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringNodeSignature":
        _, resolved_signature, _ = validate_and_resolve_signature(
            self.signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
        )
        return resolved_signature

    def create_node_builder_instance(
        self, resolved_wiring_signature, node_signature: "NodeSignature", scalars: Mapping[str, Any]
    ) -> "NodeBuilder":
        raise RuntimeError("Should not be instantiating an operator definition")

    def __repr__(self):
        from inspect import signature

        s = signature(self.fn)
        return f"{self.fn.__name__}{str(s)}"


class OverloadedWiringNodeHelper:
    """
    This meta wiring node class deals with graph/node declaration overloads, for example when we have an implementation
    of a node that is generic

        def n(t: TIME_SERIES_TYPE)

    and another one that is more specific like

        def n(t: TS[int])

    in this case if wired with TS[int] input we should choose the more specific implementation and the generic one in
    other cases.

    This problem becomes slightly trickier with more inputs or more complex types, consider:

        def m(t1: TIME_SERIES_TYPE, t2: TIME_SERIES_TYPE)  # choice 1
        def m(t1: TS[SCALAR], t2: TS[SCALAR])  # choice 2
        def m(t1: TS[int], t2: TIME_SERIES_TYPE)  # choice 3

    What should we wire provided two TS[int] inputs? In this case choice 2 is the right answer because it is more
    specific about ints inputs even if choice 3 matches one of the input types exactly. We consider a signature with
    top level generic inputs as always less specified than a signature with generics as parameters to specific
    collection types. This rule applies recursively so TSL[V, 2] is less specific than TSL[TS[SCALAR], 2]
    """

    base: WiringNodeClass
    overloads: List[Tuple[WiringNodeClass, float]]

    def __init__(self, base: WiringNodeClass):
        self.base = base
        if base.signature.node_type == WiringNodeType.OPERATOR or getattr(self.base, "skip_overload_check", False):
            self.overloads = []
        else:
            self.overloads = [(base, self._calc_rank(base.signature))]

    def overload(self, impl: WiringNodeClass):
        self.overloads.append((impl, self._calc_rank(impl.signature)))

    @staticmethod
    def _calc_rank(signature: WiringNodeSignature) -> float:
        if signature.node_type == WiringNodeType.OPERATOR:
            return 1e6  # Really not a good ranking
        ranks = []
        for k, t in signature.input_types.items():
            if signature.defaults.get(k) != AUTO_RESOLVE:
                if t.is_scalar:
                    rank = scale_rank(t.generic_rank, 0.001)
                elif k in (signature.var_arg, signature.var_kwarg):
                    rank = scale_rank(
                        t.generic_rank, 100.0
                    )  # FIXME - this assumes the reciprocal of the 0.01 multiplier, need a constant
                else:
                    rank = t.generic_rank
                ranks.append(rank)
        ranks = combine_ranks(ranks)
        return sum(ranks.values())

    def get_best_overload(self, *args, __return_sink_wp__: bool = False, **kwargs):
        candidates = []
        rejected_candidates = []
        for c, r in self.overloads:
            try:
                # Attempt to resolve the signature, if this fails then we don't have a candidate
                if "is_operator" in c.signature.args:
                    kwargs_ = dict(kwargs)
                    kwargs_["is_operator"] = True
                else:
                    kwargs_ = kwargs
                c.resolve_signature(
                    *args, **kwargs_, __enforce_output_type__=c.signature.node_type != WiringNodeType.GRAPH
                )
                candidates.append((c, r))
            except (WiringError, SyntaxError) as e:
                if isinstance(e, WiringFailureError):
                    exception = e.__cause__
                    traceback = exception.__traceback__
                    while traceback.tb_next:
                        traceback = traceback.tb_next
                    e = f"{exception} at {traceback.tb_frame.f_code.co_filename}:{traceback.tb_lineno}"

                p = lambda x: (
                    pretty_str_types(x.output_type.py_type) if isinstance(x, WiringPort) else pretty_str_types(x)
                )
                reject_reason = (
                    f"Did not resolve {c.signature.name} with {','.join(p(i) for i in args)}, "
                    f"{','.join(f'{k}:{p(v)}' for k, v in kwargs.items())} : {e}"
                )

                rejected_candidates.append((c.signature.signature, reject_reason))
            except Exception as e:
                raise

        best_candidates = sorted(candidates, key=lambda x: x[1])
        pick = (
            best_candidates[0][0]
            if (lbc := len(best_candidates)) > 0 and (lbc == 1 or best_candidates[0][1] < best_candidates[1][1])
            else None
        )
        from hgraph._wiring._wiring_observer import WiringObserverContext

        WiringObserverContext.instance().notify_overload_resolution(
            self.base.signature,
            best_candidates[0] if pick else None,
            [(c, r) for c, r in rejected_candidates],
            [(c, r) for c, r in candidates if c is not pick],
        )

        if not candidates:
            args_tp = [str(a.output_type) if isinstance(a, WiringPort) else str(a) for a in args]
            kwargs_tp = [
                (str(k), str(v.output_type) if isinstance(v, WiringPort) else str(v))
                for k, v in kwargs.items()
                if not k.startswith("_")
            ]
            _msg_part = "\n".join(str(c) for c in rejected_candidates)
            raise WiringError(
                f"Cannot wire overload {self.base.signature.signature} with args {args_tp}, kwargs {kwargs_tp}: no"
                f" matching candidates found\nRejected candidates:\n{_msg_part}"
            )

        if len(best_candidates) > 1 and pick is None:
            p = lambda x: str(x.output_type) if isinstance(x, WiringPort) else str(x)
            newline = "\n"
            raise WiringError(
                "Overloads are ambiguous with given parameters:\n"
                f" {newline.join(f'{c.signature.signature} with rank {r}' for c, r in best_candidates if r == best_candidates[0][1])}\nwhen"
                f" wired with {','.join(p(i) for i in args)}, {','.join(f'{k}:{p(v)}' for k, v in kwargs.items())}"
            )

        return pick
