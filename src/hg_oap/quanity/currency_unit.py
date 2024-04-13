from hg_oap.quanity.unit import Unit


class CurrencyUnit(Unit):

    @classmethod
    def from_string(cls, value: str) -> 'CurrencyUnit':
        # TODO: validate is meaningful and select from ENUM of currencies.
        return cls(family_unit="Currency", symbol=value)