import json
import importlib
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa

import hgraph as hg
from hgraph import TS, compute_node, eval_node
from hgraph.adaptors.data_catalogue import DataEnvironment, DataEnvironmentEntry
from hgraph.adaptors.json import json_adaptor, json_adaptor_impl
from hgraph.adaptors.json.json_adaptor import _read_json_table
from hgraph.stream import Data, Stream, StreamStatus


@dataclass(frozen=True)
class _JsonRow(hg.CompoundScalar):
    name: str
    value: int


@compute_node
def _load_json(path: TS[str]) -> TS[object]:
    return _read_json_table(Path(path.value))


def test_json_loader_reads_document_arrays_through_eval_node(tmp_path):
    path = tmp_path / "values.json"
    path.write_text(json.dumps([{"name": "a", "value": 1}, {"name": "b", "value": 2}]))

    result = eval_node(_load_json, [str(path)])[0]
    assert result.equals(
        pa.table({"name": ["a", "b"], "value": [1, 2]})
    )


def test_json_loader_reads_json_lines_through_eval_node(tmp_path):
    path = tmp_path / "values.jsonl"
    path.write_text('{"name":"a","value":1}\n{"name":"b","value":2}\n')

    result = eval_node(_load_json, [str(path)])[0]
    assert result.equals(
        pa.table({"name": ["a", "b"], "value": [1, 2]})
    )


def test_json_service_adaptor_loads_a_file(tmp_path):
    path = tmp_path / "values.json"
    path.write_text(json.dumps([{"name": "a", "value": 1}]))
    captured = []
    typed_stream = hg.TSB[Stream[Data[hg.Frame[_JsonRow]]]]

    @hg.push_queue(hg.TS[str])
    def request(sender):
        sender(path.name)

    @hg.sink_node
    def capture(
        response: typed_stream,
        engine: hg.EvaluationEngineApi = None,
    ):
        if response.status.value is StreamStatus.OK:
            captured.append(response["values"].value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("json-test", json_adaptor_impl)
        capture(json_adaptor[_JsonRow]("json-test", request()))

    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("json-test", str(tmp_path)))
    with hg.GlobalContext(hg.GlobalState()):
        with environment:
            hg.run_graph(
                app,
                run_mode=hg.EvaluationMode.REAL_TIME,
                end_time=datetime.now(timezone.utc).replace(tzinfo=None)
                + timedelta(seconds=5),
            )

    assert len(captured) == 1
    assert captured[0].equals(pa.table({"name": ["a"], "value": [1]}))


def test_json_service_adaptor_serves_repeated_client_requests_in_order(tmp_path, monkeypatch):
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(json.dumps([{"name": "first", "value": 1}]))
    second.write_text(json.dumps([{"name": "second", "value": 2}]))
    original_read = _read_json_table

    def delayed_read(path):
        if path.name == first.name:
            time.sleep(0.05)
        return original_read(path)

    json_adaptor_module = importlib.import_module("hgraph.adaptors.json.json_adaptor")
    monkeypatch.setattr(json_adaptor_module, "_read_json_table", delayed_read)
    captured = []
    typed_stream = hg.TSB[Stream[Data[hg.Frame[_JsonRow]]]]

    @hg.push_queue(hg.TS[str])
    def requests(sender):
        def publish():
            sender(first.name)
            time.sleep(0.01)
            sender(second.name)

        threading.Thread(target=publish, daemon=True).start()

    @hg.sink_node
    def capture(response: typed_stream, engine: hg.EvaluationEngineApi = None):
        if response.status.modified and response.status.value is StreamStatus.OK:
            captured.append(response["values"].value.column("name")[0].as_py())
            if len(captured) == 2:
                engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("json-repeat", json_adaptor_impl)
        capture(json_adaptor[_JsonRow]("json-repeat", requests()))

    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("json-repeat", str(tmp_path)))
    with hg.GlobalContext(hg.GlobalState()):
        with environment:
            hg.run_graph(
                app,
                run_mode=hg.EvaluationMode.REAL_TIME,
                end_time=datetime.now(timezone.utc).replace(tzinfo=None)
                + timedelta(seconds=5),
            )

    assert captured == ["first", "second"]
