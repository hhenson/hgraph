from dataclasses import dataclass
from decimal import Decimal

from hg_oap.units import Quantity
from hg_oap.units.unit import Unit
from hg_oap.units.unit_system import UnitConversionContext
from hg_oap.utils.exprclass import ExprClass
from hgraph import CompoundScalar


@dataclass(frozen=True)
class Asset(CompoundScalar, ExprClass, UnitConversionContext):
    """
    A thing of value that can be held. An asset can be an underlyer of an instrument.
    Whilst technically an asset is not an instrument, but for our purposes, it is convenient to thing of them
    as instruments.
    """
    symbol: str


@dataclass(frozen=True)
class PhysicalAsset(Asset):
    """
    A tangible thing, for example: raw materials, infrastructure, equipment, etc.
    """
    name: str
    unit: Unit  # The basic unit used to measure the asset
    unit_conversion_factors: tuple[Quantity[Decimal]]  # Properties of the asset that can be used to convert between
                                                       # units of different dimensions - i.e. density for mass/volume


@dataclass(frozen=True)
class FinancialAsset(Asset):
    """
    A financial asset is a thing of value that can be held. Examples include cash, cash equivalents, stocks
    and bonds. They are not instruments, but can be used in an instrument as an underlyer.
    """
