from abc import abstractmethod
from dataclasses import dataclass, Field, MISSING, field, InitVar, KW_ONLY
from datetime import date
from inspect import isfunction, signature

from .op import Op, Expression, is_op, lazy

__all__ = ("dataclassex", 'exprclass', 'ExprClass')


class _BaseExDescriptor:
    def __init__(self, expr):
        self.expr = expr

    def __get__(self, instance, owner = None):
        if instance is not None:
            if (v := instance.__dict__.get(self.override_name, MISSING)) is not MISSING:
                return v
            if (v := instance.__dict__.get(self.cache_name, MISSING)) is not MISSING:
                return v

            v = self.__calc__(instance)
            object.__setattr__(instance, self.cache_name, v)
            return v
        elif owner:
            return self

    @abstractmethod
    def __calc__(self, instance): ...

    def __set__(self, instance, value):
        if value is not self and instance is not None:
            if not instance.__dataclass_params__.frozen:
                setattr(instance, self.override_name, value)
            else:
                raise AttributeError(f'field {self.name} in {instance} is readonly')

    def __override__(self, instance, value):
        if value is not self and instance is not None:
            object.__setattr__(instance, self.override_name, value)

    def __overriden__(self, instance):
        return getattr(instance, self.override_name, MISSING) is not MISSING

    def __set_name__(self, owner, name):
        self.name = name
        self.cache_name = f'__cache_{self.name or id(self)}__'
        self.override_name = f'_override_{self.name or id(self)}'


class CallableDescriptor(_BaseExDescriptor):
    def __calc__(self, instance):
        return self.expr(instance)


class CallableExpressionDescriptor(_BaseExDescriptor):
    def __calc__(self, instance):
        expr = self.expr(SELF=instance, __partial__=True)
        if is_op(expr):
            return Expression(expr)
        else:
            return expr


class DateDescriptor(_BaseExDescriptor):
    def __calc__(self, instance):
        from hg_oap.dates import is_dgen, make_date

        r = self.expr(instance)
        if isinstance(r, date):
            return r
        elif is_dgen(r):
            return next(r())
        else:
            return make_date(r)


class DateListDescriptor(_BaseExDescriptor):
    def __calc__(self, instance):
        from hg_oap.dates import is_dgen, make_date

        r = self.expr(instance)
        if isinstance(r, date):
            return [r]
        elif is_dgen(r):
            return tuple(r())
        else:
            return make_date(r)


def _process_ops_and_lambdas(cls):
    cls.__annotations__.pop('SELF', None)

    overridable = []
    new_annotations = {}

    for k, a in cls.__annotations__.items():
        if (op := getattr(cls, k, None)) is not None:
            if a == date:  # special treatment for dates
                descriptor_type = DateDescriptor
            elif a == list[date] or a == tuple[date]:
                descriptor_type = DateListDescriptor
            elif getattr(a, '__origin__', None) is Expression:
                descriptor_type = CallableExpressionDescriptor
            else:
                descriptor_type = CallableDescriptor

            if isinstance(op, Field) and op.default is not None:
                d = _make_descriptor(a, cls, descriptor_type, k, op.default)
                if d is not None:
                    op.default = d
            else:
                d = _make_descriptor(a, cls, descriptor_type, k, op)
                if d is not None:
                    setattr(cls, k, d)

            if d:
                cls.__annotations__[k] = InitVar[a]
                overridable.append(k)

                override = f"_override_{k}"
                new_annotations[override] = a
                setattr(cls, override, field(default=None, init=False, repr=False, metadata={'hidden': True}))

    cls.__annotations__ = {'_': KW_ONLY, **cls.__annotations__, **new_annotations}

    original_post_init = getattr(cls, '__post_init__', None)

    def post_init(self, *args):
        for k, v in zip(overridable, args):
            if not isinstance(v, _BaseExDescriptor):
                setattr(self, f'_override_{k}', v)
        if original_post_init:
            # ideally we would figure out if there were any initvars in the original annotations and filter on those
            # and pass in but dataclasses using positional args for initvars makes it difficult to do this
            original_post_init(self)

    setattr(cls, '__post_init__', post_init)

    return cls


def _make_descriptor(annotation, cls, descriptor_type, name, op):
    if isinstance(op, Op):
        descriptor = descriptor_type(Expression(op))
        descriptor.__set_name__(cls, name)
        return descriptor
    elif isfunction(op) and op.__name__ == '<lambda>':
        if len(signature(op).parameters) == 1:
            descriptor = descriptor_type(lambda SELF, __partial__=None: op(SELF) if __partial__ is None else op)
            descriptor.__set_name__(cls, name)
            return descriptor


class ExprClass:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _process_ops_and_lambdas(cls)


def dataclassex(cls):
    return dataclass(_process_ops_and_lambdas(cls))


def exprclass(cls):
    return _process_ops_and_lambdas(cls)
