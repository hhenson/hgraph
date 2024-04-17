from dataclasses import dataclass

from hgraph import CompoundScalar


@dataclass(frozen=True)
class Asset(CompoundScalar):
    """
    A thing of value that can be held. An asset can be an underlyer of an instrument.
    """
    symbol: str


@dataclass(frozen=True)
class PhysicalAsset(Asset):
    """
    A tangible thing, for example: raw materials, infrastructure, equipment, etc.
    """
    name: str


@dataclass(frozen=True)
class FinancialAsset(Asset):
    """
    A financial asset is a thing of value that can be held. Examples include cash, cash equivalents, stocks
    and bonds. They are not instruments, but can be used in an instrument as an underlyer.
    """
