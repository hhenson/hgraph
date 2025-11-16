import builtins
import datetime
import logging
import re
from asyncio import Future
from collections import deque

import pyarrow

from hgraph import (
    MIN_DT,
    Node,
    PythonNestedNodeImpl,
    TimeSeriesInput,
    TimeSeriesOutput,
    PythonTimeSeriesReferenceOutput,
    TimeSeriesReference,
    PythonTimeSeriesReferenceInput,
    to_table,
)
from hgraph._impl._operators._to_table_dispatch_impl import extract_table_schema
from hgraph.adaptors.tornado.http_server_adaptor import HttpGetRequest, HttpResponse, HttpRequest
from hgraph.debug._inspector_item_id import InspectorItemId, NodeValueType
from hgraph.debug._inspector_publish import process_graph_stats, process_node_stats, process_item_stats
from hgraph.debug._inspector_state import InspectorState
from hgraph.debug._inspector_util import (
    enum_items,
    format_type,
    format_value,
    format_modified,
    format_name,
    format_scheduled,
)

logger = logging.getLogger(__name__)


def graph_object_from_id(state: InspectorState, item_id: InspectorItemId):
    gi = state.observer.get_graph_info(item_id.graph)
    if gi is None:
        raise ValueError(f"Graph {item_id.graph} not found")

    graph = gi.graph

    value = item_id.find_item_on_graph(graph)
    if value is None:
        raise ValueError(f"Item {item_id} not found")

    return graph, value


def graph_type_from_id(state: InspectorState, item_id: InspectorItemId):
    gi = state.observer.get_graph_info(item_id.graph)
    if gi is None:
        raise ValueError(f"Graph {item_id.graph} not found")

    graph = gi.graph

    tp = item_id.find_item_type(graph)
    if tp is None:
        raise ValueError(f"Item {item_id} not found or its type is not known")

    return tp


def item_iterator(item_id, value):
    if isinstance(value, Node):
        items = []
        if value.input:
            items.append(("INPUTS", value.input, item_id.sub_item("INPUTS", NodeValueType.Inputs)))
        if value.output:
            items.append(("OUTPUT", value.output, item_id.sub_item("OUTPUT", NodeValueType.Output)))
        if isinstance(value, PythonNestedNodeImpl):
            items.append(("GRAPHS", value.nested_graphs(), item_id.sub_item("GRAPHS", NodeValueType.Graphs)))
        if value.scalars:
            items.append(("SCALARS", value.scalars, item_id.sub_item("SCALARS", NodeValueType.Scalars)))
        item_iter = items
    else:
        item_iter = ((k, v, item_id.sub_item(k, v)) for k, v in enum_items(value))

    return item_iter


def inspector_expand_item(state: InspectorState, item_id: InspectorItemId):
    graph, value = graph_object_from_id(state, item_id)

    item_iter = item_iterator(item_id, value)

    data = state.value_data
    items = 0
    for k, v, i in item_iter:
        if i.graph != graph.graph_id:
            gi = state.observer.get_graph_info(i.graph)
            if gi is None:
                continue
            else:
                graph = gi.graph

        str_id = i.to_str()
        data.setdefault(str_id, dict(id=str_id)).update(
                ord=i.sort_key(),
                X="+",
                name=i.indent(graph) + format_name(v, k),
                type=format_type(v),
                value=format_value(v),
                modified=format_modified(v),
                scheduled=format_scheduled(v),
            )

        subscribe_item(state, i)
        items += 1

    if item_id.graph != () or item_id.node is not None:
        str_id = item_id.to_str()
        if items:
            data.setdefault(str_id, dict(id=str_id))['X'] = "-"
        else:
            data.setdefault(str_id, dict(id=str_id))['X'] = "º"

    return "", []


def inspector_show_item(state: InspectorState, item_id: InspectorItemId):
    if item_id.node is None:
        if item_id.graph in state.graph_subscriptions:
            return "", []
    elif item_id.value_type is None:
        if item_id.graph + (item_id.node,) in state.node_subscriptions:
            return "", []
    elif subscriptions := state.node_item_subscriptions.get(item_id.graph + (item_id.node,)):
        if item_id in subscriptions:
            return "", []

    graph, value = graph_object_from_id(state, item_id)

    key = item_id.value_path[-1] if item_id.value_path else item_id.value_type.value if item_id.value_type else None

    str_id = item_id.to_str()
    state.value_data.setdefault(str_id, dict(id=str_id)).update(
            id=item_id.to_str(),
            ord=item_id.sort_key(),
            X="+",
            name=item_id.indent(graph) + format_name(value, key),
            type=format_type(value),
            value=format_value(value),
            modified=format_modified(value),
            scheduled=format_scheduled(value),
        )

    subscribe_item(state, item_id)

    return "", []


def inspector_collapse_item(state, item_id):
    graphs_to_unsubscribe = []
    for k, v in state.graph_subscriptions.items():
        if item_id.is_parent_of(v):
            graphs_to_unsubscribe.append(v)

    nodes_to_unsubscribe = []
    for k, v in state.node_subscriptions.items():
        if item_id.is_parent_of(v):
            nodes_to_unsubscribe.append(v)

    items_to_unsubscribe = []
    if item_id.node is not None:
        subscriptions = state.node_item_subscriptions.get(item_id.graph + (item_id.node,))
        if subscriptions is not None:
            if item_id.value_type is None:  # everything under the node has to go, but not the node itself
                items_to_unsubscribe.extend(subscriptions - {item_id})
            else:
                for sub_item_id in subscriptions:
                    if item_id.is_parent_of(sub_item_id):
                        items_to_unsubscribe.append(sub_item_id)

    for graph_i in graphs_to_unsubscribe:
        unsubscribe_item(state, graph_i)

    for node_i in nodes_to_unsubscribe:
        unsubscribe_item(state, node_i)

    for sub_item_id in items_to_unsubscribe:
        unsubscribe_item(state, sub_item_id)

    str_id = item_id.to_str()
    state.value_data.setdefault(str_id, dict(id=str_id))['X'] = "+"

    return "", []


def inspector_pin_item(state, item_id):
    return "", []


def find_output(item_id, value):
    if isinstance(value, TimeSeriesInput):
        if value._reference_output is not None:
            item_id = InspectorItemId.from_object(value._reference_output)
        elif value.output is not None:
            item_id = InspectorItemId.from_object(value.output)
        elif isinstance(value, PythonTimeSeriesReferenceInput):
            if value.valid and value.value.has_output:
                item_id = InspectorItemId.from_object(value.value.output)
            else:
                raise ValueError(f"Reference input {item_id} has no output and no value")
        else:
            raise ValueError(f"Input {item_id} has no output")
    else:
        raise ValueError(f"Item {item_id} is not a bound input")
    
    return item_id


def inspector_find_output(state, item_id):
    graph, value = graph_object_from_id(state, item_id)

    item_id = find_output(item_id, value)

    if item_id is None:
        raise ValueError(f"Referenced item not found")

    commands = [("show", i) for i in item_id.parent_item_ids()] + [("show", item_id)]
    
    node_id = InspectorItemId(graph=item_id.graph, node=item_id.node)
    commands.append(("expand", node_id))
    
    _, node = graph_object_from_id(state, node_id)
    if node.input:
        inputs = InspectorItemId(graph=item_id.graph, node=item_id.node, value_type=NodeValueType.Inputs)
        commands.append(("expand", inputs))

    return item_id.to_str(), commands


def find_ref_output(item_id, value):
    if isinstance(value, TimeSeriesInput):
        if value.output is not None:
            item_id = InspectorItemId.from_object(value.output)
        elif isinstance(value, PythonTimeSeriesReferenceInput):
            if value.valid and value.value.has_output:
                item_id = InspectorItemId.from_object(value.value.output)
            else:
                raise ValueError(f"Reference input {item_id} has no output and no value")
        else:
            raise ValueError(f"Input {item_id} has no output")
    elif isinstance(value, PythonTimeSeriesReferenceOutput):
        if value.valid and value.value.has_output:
            item_id = InspectorItemId.from_object(value.value.output)
        else:
            raise ValueError(f"TimeSeriesReference {item_id} references no output")
    elif isinstance(value, TimeSeriesReference):
        if value.is_valid and value.has_output:
            item_id = InspectorItemId.from_object(value.output)
        else:
            raise ValueError(f"TimeSeriesReference {item_id} references no output")
    else:
        raise ValueError(f"Item {item_id} is not a reference or bound inputs")
    
    return item_id


def inspector_follow_ref(state, item_id):
    graph, value = graph_object_from_id(state, item_id)

    item_id = find_ref_output(item_id, value)

    if item_id is None:
        raise ValueError(f"Referenced item not found")

    commands = [("show", i) for i in item_id.parent_item_ids()] + [("show", item_id)]
    
    node_id = InspectorItemId(graph=item_id.graph, node=item_id.node)
    commands.append(("expand", node_id))
    
    _, node = graph_object_from_id(state, node_id)
    if node.input:
        inputs = InspectorItemId(graph=item_id.graph, node=item_id.node, value_type=NodeValueType.Inputs)
        commands.append(("expand", inputs))

    return item_id.to_str(), commands


def inspector_follow_refs(state, item_id):
    graph, value = graph_object_from_id(state, item_id)

    if isinstance(value, Node) and value.output:
        value = value.output

    while True:
        item_id = find_ref_output(item_id, value)
        if value.value is None:
            break
        if item_id.value_type != NodeValueType.Output:
            break

        node_id = InspectorItemId(graph=item_id.graph, node=item_id.node)
        graph, node = graph_object_from_id(state, node_id)
        if not isinstance(node, Node):
            break

        _, value = graph_object_from_id(state, item_id)
        if isinstance(value, (TimeSeriesOutput, TimeSeriesOutput)) and value.valid and value.value is not None:
            value = value.value
        
        for k, v in node.inputs.items():
            in_id = InspectorItemId(graph=item_id.graph, node=item_id.node, value_type=NodeValueType.Inputs, value_path=(k,) + item_id.value_path)
            _, inp = graph_object_from_id(state, in_id)
                
            if isinstance(inp, (TimeSeriesInput)) and inp.value is not None:
                in_value = inp.value
                if in_value is value:
                    value = inp
                    break
            else:
                if inp is value:
                    while inp is not None and not isinstance(inp, TimeSeriesInput):
                        in_id = InspectorItemId(graph=in_id.graph, node=in_id.node, value_type=NodeValueType.Inputs, value_path=in_id.value_path[:-1])
                        try:
                            _, inp = graph_object_from_id(state, )
                        except ValueError:
                            inp = None
                            break
                    value = inp
                    break
        else:
            break
        
        
    if item_id is None:
        raise ValueError(f"Referenced item not found")

    commands = [("show", i) for i in item_id.parent_item_ids()] + [("show", item_id)]
    
    node_id = InspectorItemId(graph=item_id.graph, node=item_id.node)
    commands.append(("expand", node_id))
    
    _, node = graph_object_from_id(state, node_id)
    if node.input:
        inputs = InspectorItemId(graph=item_id.graph, node=item_id.node, value_type=NodeValueType.Inputs)
        commands.append(("expand", inputs))

    return item_id.to_str(), commands


def inspector_pin_ref(state, item_id):
    return "", []


def inspector_unpin_item(state, item_id):
    return "", []


def inspector_search_item(state, item_id, search_re, depth=0, limit=10):
    graph, value = graph_object_from_id(state, item_id)

    item_iter = item_iterator(item_id, value)

    items = 0
    for k, v, i in item_iter:
        name = format_name(v, k)
        if search_re.search(name) is None:
            if depth:
                found, new_commands = inspector_search_item(state, i, search_re, depth - 1)
                if found:
                    return found, new_commands

            continue

        if i.graph != graph.graph_id:
            gi = state.observer.get_graph_info(i.graph)
            if gi is None:
                continue
            else:
                graph = gi.graph

        str_id = i.to_str()
        state.value_data.setdefault(str_id, dict(id=str_id)).update(
                id=i.to_str(),
                ord=i.sort_key(),
                X="?",
                name=i.indent(graph) + name,
                type=format_type(v),
                value=format_value(v),
                modified=format_modified(v),
                scheduled=format_scheduled(v),
            )

        items += 1

        if not is_item_subscribed(state, i):
            state.found_items.add(i.to_str())

        if items >= limit:
            return i.to_str(), []

    return "", []


def inspector_read_value(state, item_id):
    graph, value = graph_object_from_id(state, item_id)

    import polars as pl

    from hgraph import PythonTimeSeriesValueInput, PythonTimeSeriesValueOutput

    if isinstance(value, Node):
        value = value.output
        item_id = item_id.sub_item("OUTPUT", NodeValueType.Output)

    if isinstance(value, (PythonTimeSeriesValueInput, PythonTimeSeriesValueOutput)):
        value = value.value if isinstance(value.value, pl.DataFrame) else value

    if not isinstance(value, pl.DataFrame):
        tp = graph_type_from_id(state, item_id)
        schema = extract_table_schema(tp)
        table = schema.to_table_snap(value)
        if not table:
            return "", []
        if not schema.partition_keys:
            table = [table]

        def map_type(t: type, values):
            match t:
                case builtins.int:
                    return pyarrow.int64(), values
                case builtins.str:
                    return pyarrow.string(), values
                case builtins.float:
                    return pyarrow.float64(), values
                case builtins.bool:
                    return pyarrow.bool_(), values
                case datetime.date:
                    return pyarrow.date32(), values
                case datetime.datetime:
                    return pyarrow.timestamp("us"), values
                case datetime.time:
                    return pyarrow.time64("us"), values
                case datetime.timedelta:
                    return pyarrow.duration("us"), values
                case _:
                    return pyarrow.string(), [str(v) for v in values]

        mapped_types, mapped_values = zip(*(map_type(t, v) for t, v in zip(schema.types, zip(*table))))

        pyarrow_schema = pyarrow.schema([(k, v) for k, v in zip(schema.keys, mapped_types)])

        batch = pyarrow.record_batch(list(mapped_values), schema=pyarrow_schema)
        stream = pyarrow.BufferOutputStream()

        with pyarrow.ipc.new_stream(stream, batch.schema) as writer:
            writer.write_batch(batch)
    else:
        batches = value._df.to_arrow(compat_level=False)
        stream = pyarrow.BufferOutputStream()

        with pyarrow.ipc.new_stream(stream, batches[0].schema) as writer:
            for batch in batches:
                writer.write_batch(batch)

    return stream.getvalue().to_pybytes(), []


def handle_requests(state: InspectorState):
    publish = False
    while f_r := state.requests.dequeue():
        f, r = f_r
        handle_inspector_request(state, r, f)
        publish = True

    return publish


def handle_inspector_request(state: InspectorState, request: HttpGetRequest, f: Future):
    command = request.url_parsed_args[0]
    item_str = request.url_parsed_args[1]
    try:
        item_id = InspectorItemId.from_str(item_str)
    except Exception as e:
        set_result(f, HttpResponse(500, body=f"Invalid item {item_str}"))
        return

    commands = deque()
    commands.append((command, item_id))

    total_response = ""

    while commands:
        command, item_id = commands.popleft()

        try:
            match command:
                case "expand":
                    response, new_commands = inspector_expand_item(state, item_id)
                case "show":
                    response, new_commands = inspector_show_item(state, item_id)
                case "search":
                    if "q" not in request.query:
                        raise ValueError("Search command requires a query parameter")

                    search = re.compile(request.query["q"], re.I)
                    depth = int(request.query.get("depth", 3))
                    limit = request.query.get("limit", 10)

                    prev_found_items = state.found_items
                    state.found_items = set()

                    response, new_commands = inspector_search_item(state, item_id, search, depth=depth, limit=limit)

                    state.value_removals.update(prev_found_items - state.found_items)

                case "applysearch":
                    new_commands = []
                    for i in state.found_items:
                        item_id = InspectorItemId.from_str(i)
                        new_commands += [("show", i) for i in item_id.parent_item_ids()] + [("show", item_id)]
                    state.found_items.clear()

                    response = "OK"

                case "stopsearch":
                    state.value_removals.update(state.found_items)
                    response = "OK"
                    new_commands = []

                case "collapse":
                    response, new_commands = inspector_collapse_item(state, item_id)
                case "pin":
                    response, new_commands = inspector_pin_item(state, item_id)
                case "output":
                    response, new_commands = inspector_find_output(state, item_id)
                case "ref":
                    response, new_commands = inspector_follow_ref(state, item_id)
                case "refs":
                    response, new_commands = inspector_follow_refs(state, item_id)
                case "pin_ref":
                    response, new_commands = inspector_pin_ref(state, item_id)
                case "unpin":
                    response, new_commands = inspector_unpin_item(state, item_id)
                case "value":
                    response, new_commands = inspector_read_value(state, item_id)
                case _:  # pragma: no cover
                    set_result(f, HttpResponse(404, body="Invalid command"))
                    return
        except Exception as e:
            set_result(f, HttpResponse(500, body=f"Error: {e}"))
            logger.exception(f"Inspector error {e}")
            return
            # raise e

        if isinstance(response, str):
            total_response += response if not total_response else (f"\n{response}" if response else "")
        else:
            assert not total_response
            assert not new_commands
            total_response = response

        commands.extend(new_commands)

    set_result(f, HttpResponse(200, body=total_response))


def subscribe_item(state, sub_item_id):
    if sub_item_id.node is None:
        state.graph_subscriptions[sub_item_id.graph] = sub_item_id
        state.observer.subscribe_graph(sub_item_id.graph)
        process_graph_stats(state, sub_item_id.graph, sub_item_id, state.track_detailed_performance)
    elif sub_item_id.value_type is None:
        node_id = sub_item_id.graph + (sub_item_id.node,)
        state.node_subscriptions[node_id] = sub_item_id
        state.observer.subscribe_node(node_id)
        process_node_stats(state, node_id, sub_item_id, state.track_detailed_performance)
    else:
        node_id = sub_item_id.graph + (sub_item_id.node,)
        state.node_item_subscriptions[node_id].add(sub_item_id)
        process_item_stats(state, sub_item_id)
        if node_id not in state.node_subscriptions:
            state.node_subscriptions[node_id] = sub_item_id
            state.observer.subscribe_node(node_id)


def unsubscribe_item(state, sub_item_id):
    if sub_item_id.node is None:
        graph_id = sub_item_id.graph
        state.graph_subscriptions.pop(graph_id, None)
        state.observer.unsubscribe_graph(sub_item_id.graph)
        state.value_removals.add(sub_item_id.to_str())
    elif sub_item_id.value_type is None:
        node_id = sub_item_id.graph + (sub_item_id.node,)
        state.node_subscriptions.pop(node_id, None)
        state.observer.unsubscribe_node(node_id)
        state.value_removals.add(sub_item_id.to_str())
        for item_id in state.node_item_subscriptions.get(node_id, set()):
            state.value_removals.add(item_id.to_str())
        state.node_item_subscriptions.pop(node_id, None)
    else:
        node_id = sub_item_id.graph + (sub_item_id.node,)
        subscriptions = state.node_item_subscriptions.get(node_id)
        state.value_removals.add(sub_item_id.to_str())
        if subscriptions is not None:
            subscriptions.remove(sub_item_id)
            if not subscriptions:
                del state.node_item_subscriptions[node_id]
                if node_id not in state.node_subscriptions:
                    state.observer.unsubscribe_node(node_id)


def is_item_subscribed(state, item_id):
    if item_id.node is None:
        return item_id.graph in state.graph_subscriptions
    elif item_id.value_type is None:
        return item_id.graph + (item_id.node,) in state.node_subscriptions
    else:
        return item_id in state.node_item_subscriptions.get(item_id.graph + (item_id.node,), set())


def set_result(f, r):
    def apply_result(fut, res):
        try:
            fut.set_result(res)
        except:
            pass

    from hgraph.adaptors.tornado._tornado_web import TornadoWeb

    TornadoWeb.get_loop().add_callback(lambda f, r: apply_result(f, r), f, r)
