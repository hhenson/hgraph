import json
import logging
from concurrent.futures import Executor, Future
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.json as pa_json

from hgraph import (
    AUTO_RESOLVE,
    DEFAULT,
    SCHEMA,
    STATE,
    TS,
    TSB,
    TSD,
    Frame,
    map_,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)
from hgraph.adaptors.data_catalogue import DataEnvironment
from hgraph.adaptors.executor import adaptor_executor
from hgraph.stream import Data, Stream, StreamStatus

logger = logging.getLogger(__name__)

__all__ = ("json_adaptor", "json_adaptor_impl")


def _read_json_table(path: Path) -> pa.Table:
    """Read JSON lines natively and fall back to ordinary JSON documents."""
    with path.open("rb") as stream:
        first = stream.read(1)
        while first and first.isspace():
            first = stream.read(1)
    if first != b"[":
        try:
            return pa_json.read_json(path)
        except pa.ArrowInvalid:
            pass

    with path.open("r", encoding="utf-8") as stream:
        document = json.load(stream)
    if isinstance(document, list):
        return pa.Table.from_pylist(document)
    if isinstance(document, dict):
        if document and all(isinstance(value, list) for value in document.values()):
            return pa.table(document)
        return pa.Table.from_pylist([document])
    raise ValueError(f"JSON root in {path} must be an object or array")


@service_adaptor
def json_adaptor(
    path: str,
    file: TS[str],
    _schema: type[SCHEMA] = DEFAULT[SCHEMA],
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=json_adaptor)
def json_adaptor_impl(
    path: str,
    file: TSD[int, TS[str]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    environment = DataEnvironment.current()
    if environment is None:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    directory = Path(environment.get_entry(path).environment_path)
    output_type = TSD[int, TSB[Stream[Data[Frame[_schema]]]]]
    sender_ref = {}

    @push_queue(output_type)
    def json_to_graph(sender):
        sender_ref["sender"] = sender

    def load_json(
        directory: Path,
        request_id: int,
        file_name: str,
        sender,
        previous: Future | None,
    ):
        if previous is not None:
            previous.result()

        file_path = directory / file_name
        try:
            logger.info("Loading json file %s", file_path)
            table = _read_json_table(file_path)
            if table.num_rows:
                result = {
                    "status": StreamStatus.OK,
                    "status_msg": "",
                    "values": table,
                    "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                }
            else:
                message = f"Empty json file {file_path}"
                logger.warning(message)
                result = {
                    "status": StreamStatus.ERROR,
                    "status_msg": message,
                    "values": None,
                    "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                }
        except Exception as error:
            logger.exception("Error loading json file %s", file_path)
            result = {
                "status": StreamStatus.ERROR,
                "status_msg": str(error),
                "values": None,
                "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        sender({request_id: result})

    @sink_node
    def handle_request(
        request_id: TS[int],
        file: TS[str],
        executor: TS[Executor],
        _state: STATE = None,
    ):
        previous = getattr(_state, "future", None)
        _state.future = executor.value.submit(
            load_json,
            directory,
            request_id.value,
            file.value,
            sender_ref["sender"],
            previous,
        )

    map_(
        lambda key, name, executor: handle_request(
            request_id=key, file=name, executor=executor),
        name=file,
        executor=adaptor_executor(),
    )
    return json_to_graph()
