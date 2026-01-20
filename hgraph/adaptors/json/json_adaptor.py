import logging
from concurrent.futures import Executor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Type

import polars as pl
from polars import DataFrame

from hgraph.adaptors.executor.executor import adaptor_executor
from hgraph.adaptors.data_catalogue.catalogue import DataEnvironment
from hgraph import (
    AUTO_RESOLVE,
    DEFAULT,
    SCHEMA,
    STATE,
    TS,
    TSB,
    TSD,
    EvaluationMode,
    GlobalState,
    debug_print,
    generator,
    graph,
    if_true,
    map_,
    push_queue,
    register_adaptor,
    run_graph,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
    valid,
)
from hgraph import stop_engine
from hgraph.stream.stream import Stream, StreamStatus, Data

logger = logging.getLogger(__name__)


__all__ = ['json_adaptor', 'json_adaptor_impl']


@service_adaptor
def json_adaptor(path: str, file: TS[str], _schema: type[SCHEMA] = DEFAULT[SCHEMA]) -> TSB[Stream[Data[DataFrame]]]: ...


@service_adaptor_impl(interfaces=json_adaptor)
def json_adaptor_impl(
    path: str, file: TSD[int, TS[str]], _schema: Type[SCHEMA] = AUTO_RESOLVE
) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    dir_path = de.get_entry(path).environment_path

    @push_queue(TSD[int, TSB[Stream[Data[DataFrame]]]])
    def json_to_graph(sender: callable, path: str) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
        GlobalState.instance()[f"json_adaptor://{path}/queue"] = sender

    @generator
    def start_json_adaptor(dir_path: str, _state: STATE = None) -> TS[Path]:
        _state.directory = Path(dir_path)
        yield timedelta(), _state.directory

    @start_json_adaptor.stop
    def stop_json_adaptor(_state: STATE):
        _state.executor.shutdown()

    def load_json(directory: Path, id: int, file: str, queue):
        try:
            logger.info(f"Loading json file {directory} / {file}")
            r = pl.read_json(directory / file)
            if not r.is_empty():
                tick = {id: {"status": StreamStatus.OK, "status_msg": "", "values": r, "timestamp": datetime.utcnow()}}
                queue(tick)
            else:
                msg = f"Empty json file {directory} / {file}"
                logger.warning(msg)
                tick = {id: {"status": StreamStatus.ERROR, "status_msg": msg, "timestamp": datetime.utcnow()}}
        except Exception as e:
            logger.exception(f"Error loading json file {directory} / {file}")
            error = {id: {"status": StreamStatus.ERROR, "status_msg": str(e)}}
            queue(error)

    @sink_node
    def handle_request(id: TS[int], file: TS[str], path: str, directory: TS[Path], executor: TS[Executor]):
        queue = GlobalState.instance()[f"json_adaptor://{path}/queue"]
        executor.value.submit(load_json, directory=directory.value, id=id.value, file=file.value, queue=queue)

    directory = start_json_adaptor(dir_path)
    executor = adaptor_executor()
    map_(
        lambda key, f, d, e: handle_request(id=key, file=f, path=path, directory=d, executor=e),
        f=file,
        d=directory,
        e=executor,
    )

    return json_to_graph(path)


if __name__ == "__main__":

    @graph
    def g():
        register_adaptor("B:\\LonProfiles\\tolukemi\\json", json_adaptor_impl)
        result = json_adaptor("B:\\LonProfiles\\tolukemi\\json", "portfolio.json")
        debug_print("result", result)
        stop_engine(if_true(valid(result)))

    run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=30))
