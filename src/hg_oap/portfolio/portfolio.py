from hgraph import TSB, TimeSeriesSchema, TSD, TS, TSS, compute_node, subscription_service

from hg_oap.instruments.instrument import INSTRUMENT_ID
from hg_oap.units.unit import Unit

BOOK_ID = str


class PositionQuantity(TimeSeriesSchema):
    qty: TS[float]
    qty_unit: TS[Unit]


class Book(TimeSeriesSchema):
    """
    A book contains a unique collection of trades and the resultant positions. Since the focus in this case is
    on positions management and not trade booking, this book will represent the resultant positions of trading activity
    over time. The implementation can be backed by a trade-booking system or can just track positions without tracking
    the raw trades. For scenario testing, it is the book, that will need to ensure it applies the appropriate
    instrument life-cycle operations.

    Typically, books contain a related set of instruments. This can be related to a strategy, a trader, or a collection
    of related assets. A position represent in a book is unique and will not be found in another book.
    """
    positions: TSD[INSTRUMENT_ID, TSB[PositionQuantity]]


@subscription_service
def get_book(book_id: TS[BOOK_ID]) -> Book:
    """Returns a book for the given id"""


PORTFOLIO_ID = str


class Portfolio(TimeSeriesSchema):
    """
    A portfolio collects other portfolios and books together into a hierarchical collection that is useful to
    present a view over the books. A portfolio hierarchy is not unique and positions that are represented in one
    portfolio can be represented in another.

    Portfolios are used to slice books into various views that can be used to provide insights into the performance
    of an organisation.
    """

    books: TSS[BOOK_ID]
    portfolios: TSS[PORTFOLIO_ID]


@subscription_service
def get_portfolio(portfolio_id: TS[PORTFOLIO_ID]) -> TSB[Portfolio]:
    """Returns the portfolio TSB of a given portfolio_id"""


@subscription_service
def rolled_positions(portfolio_id: TS[PORTFOLIO_ID]) -> TSD[INSTRUMENT_ID, TSB[PositionQuantity]]:
    """
    Return the positions associated to this portfolio. This will recursively traverse the portfolio collecting up the
    books and then reducing the positions of all the books.
    """
