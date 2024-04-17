from hg_oap.units.unit import Unit


class Mass(Unit):

    def __init__(self, symbol: str):
        super().__init__("Mass", symbol)

    def normal_unit(self) -> "AtomicUnit":
        return Gram()

#
# class Gram(Mass):
#
#     def __init__(self):
#         super().__init__('g')
#
#
# class Kilogram(Mass):
#
#     def __init__(self):
#         super().__init__('kg')
#
#     def ratio_to_normal(self) -> float:
#         return 1e3
#
#
# class Milligram(Mass):
#
#     def __init__(self):
#         super().__init__('mg')
#
#     def ratio_to_normal(self) -> float:
#         return 1e3
#
#
# class Ounce(Mass):
#
#     def __init__(self):
#         super().__init__('oz')
#
#     def ratio_to_normal(self) -> float:
#         return 28.3495
