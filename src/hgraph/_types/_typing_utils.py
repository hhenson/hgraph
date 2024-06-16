import itertools
from typing import TypeVar

__all__ = ("clone_typevar", "nth", "with_signature")


def clone_typevar(tp: TypeVar, name: str) -> TypeVar:
    """Creates a copy of a typevar and sets the name to the copies name"""
    if tp.__constraints__:
        rv = TypeVar(name, *tp.__constraints__, covariant=tp.__covariant__, contravariant=tp.__contravariant__)
    else:
        rv = TypeVar(name, bound=tp.__bound__, covariant=tp.__covariant__, contravariant=tp.__contravariant__)
    return rv


class Sentinel:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"Sentinel({self.name})"

    def __str__(self):
        return self.name


def nth(iterable, n):
    """Very trivial implementation of nth, to avoid the need to import more-itertools"""
    return next(itertools.islice(iterable, n, n + 1))


def take(n, iterable):
    """Very trivial implementation of take, to avoid the need to import more-itertools"""
    return itertools.islice(iterable, n)


def with_signature(fn=None, *, annotations=None, args=None, kwargs=None, defaults=None, return_annotation=None):
    from inspect import signature, Parameter, Signature

    if fn is None:
        return lambda fn: with_signature(
            fn,
            annotations=annotations,
            args=args,
            kwargs=kwargs,
            defaults=defaults,
            return_annotation=return_annotation,
        )

    sig = signature(fn)
    annotations = annotations or {}
    defaults = defaults or {}
    new_params = []
    new_annotations = {}
    for n, parameter in sig.parameters.items():
        if parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.POSITIONAL_ONLY, Parameter.KEYWORD_ONLY):
            if n in annotations:
                new_params.append(
                    Parameter(
                        n,
                        Parameter.POSITIONAL_OR_KEYWORD,
                        annotation=annotations[n],
                        default=defaults.get(n, Parameter.empty),
                    )
                )
                new_annotations[n] = annotations[n]
            else:
                new_params.append(parameter)
                new_annotations[n] = parameter.annotation
        if parameter.kind == Parameter.VAR_POSITIONAL:
            for n, a in args.items():
                new_params.append(
                    Parameter(
                        n, Parameter.POSITIONAL_OR_KEYWORD, annotation=a, default=defaults.get(n, Parameter.empty)
                    )
                )
                new_annotations[n] = a
            args = None
        if parameter.kind == Parameter.VAR_KEYWORD:
            for n, a in kwargs.items():
                new_params.append(
                    Parameter(n, Parameter.KEYWORD_ONLY, annotation=a, default=defaults.get(n, Parameter.empty))
                )
                new_annotations[n] = a
            kwargs = None

    if args is not None:
        raise ValueError(
            f"with_signature was provided annotaitons for *args however there is no *argument in the current function signature"
        )

    if kwargs is not None:
        raise ValueError(
            f"with_signature was provided annotaitons for **kwargs however there is no **argument in the current function signature"
        )

    if return_annotation is not None:
        new_annotations["return"] = return_annotation

    sig = Signature(parameters=new_params, return_annotation=return_annotation)
    fn.__signature__ = sig
    fn.__annotations__ = new_annotations
    return fn
