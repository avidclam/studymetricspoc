from dataclasses import dataclass
import kuzu
from .processconfig import ProcessConfig


@dataclass
class EngineContext:
    pc: ProcessConfig
    gconn: kuzu.AsyncConnection


engine_context: EngineContext | None = None


def get_engine_context() -> EngineContext:
    global engine_context
    if engine_context is None:
        pc = ProcessConfig()
        gpath = pc.get_path('GRAPH_PATH')
        gdb = kuzu.Database(gpath, read_only=True)
        gopt = pc.get('GRAPH_OPTION', {})
        gconn = kuzu.AsyncConnection(gdb, **gopt)
        engine_context = EngineContext(pc=pc, gconn=gconn)
    return engine_context
