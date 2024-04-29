from abc import abstractmethod
from dataclasses import dataclass, Field
from datetime import date
from inspect import isfunction, signature

from hg_oap.dates.dgen import make_date, is_dgen
from hg_oap.utils.op import Op, Expression, is_op

__all__ = ("dataclassex", 'exprclass', 'ExprClass')


class _BaseExDescriptor:
    def __init__(self, expr):
        self.expr = expr

    def __get__(self, instance, owner = None):
        if instance is not None:
            if (v := instance.__dict__.get(self.cache_name, None)) is not None:
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
                setattr(instance, self.cache_name, value)
            else:
                raise AttributeError(f'field {self.name} in {instance} is readonly')

    def __override__(self, instance, value):
        if value is not self and instance is not None:
            object.__setattr__(instance, self.cache_name, value)
            object.__setattr__(instance, self.overriden_name, True)

    def __overriden__(self, instance):
        return getattr(instance, self.overriden_name, False)

    def __set_name__(self, owner, name):
        self.name = name
        self.cache_name = f'__cache_{self.name or id(self)}__'
        self.overriden_name = f'__overriden_{self.name or id(self)}__'


class CallableDescriptor(_BaseExDescriptor):
    def __calc__(self, instance):
        return self.expr(instance)


class DateDescriptor(_BaseExDescriptor):
    def __calc__(self, instance):
        r = self.expr(instance)
        if isinstance(r, date):
            return r
        elif is_dgen(r):
            return next(r())
        else:
            return make_date(r)


class DateListDescriptor(_BaseExDescriptor):
    def __calc__(self, instance):
        r = self.expr(instance)
        if isinstance(r, date):
            return [r]
        elif is_dgen(r):
            return tuple(r())
        else:
            return make_date(r)


def _process_ops_and_lambdas(cls):
    cls.__annotations__.pop('SELF', None)

    for k, a in cls.__annotations__.items():
        if (op := getattr(cls, k, None)) is not None:

            if a == date:  # special treatment for dates
                descriptor_type = DateDescriptor
            elif a == list[date] or a == tuple[date]:
                descriptor_type = DateListDescriptor
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

    return cls


def _make_descriptor(annotation, cls, descriptor_type, name, op):
    if isinstance(op, Op) and not is_op(annotation):
        descriptor = descriptor_type(Expression(op))
        descriptor.__set_name__(cls, name)
        return descriptor
    elif isfunction(op) and op.__name__ == '<lambda>':
        if len(signature(op).parameters) == 1:
            descriptor = descriptor_type(op)
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
