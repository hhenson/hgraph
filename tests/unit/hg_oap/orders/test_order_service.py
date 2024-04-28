from hgraph import graph, TS, TSB, compute_node, register_service
from hgraph.test import eval_node

from hg_oap.instruments.instrument import Instrument
from hg_oap.orders.order import OrderState, SingleLegOrder, OriginatorInfo
from hg_oap.orders.order_service import order_handler, OrderRequest, OrderResponse, order_client, \
    CreateOrderRequest
from hg_oap.orders.order_type import MarketOrderType
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import Unit


@order_handler
@compute_node(active=("request",))
def simple_handler(
        request: TS[OrderRequest],
        order_state: TSB[OrderState[SingleLegOrder]]
) -> TS[OrderResponse]:
    """
    Simple example
    """
    request: OrderRequest = request.value
    result = OrderResponse.accept(request)
    return result


def test_simple_handler():
    @graph
    def g(ts: TS[OrderRequest]) -> TS[OrderResponse]:
        register_service("order.simple_handler", simple_handler)
        return order_client("order.simple_handler", ts)

    requests = [
        OrderRequest.create_request(
            CreateOrderRequest, None, 'Howard', order_id="1",
            order_type=MarketOrderType(instrument=Instrument(symbol="MCU_3M"),
                                       quantity=Quantity[float](qty=1.0, unit=Unit())),
            originator_info=OriginatorInfo(account="account")
        )
    ]
    result = eval_node(g, requests)
    assert result == [
        None, None,
        OrderResponse.accept(requests[0]),
    ]
