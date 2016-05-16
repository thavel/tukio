import asyncio
import inspect
from uuid import uuid4

from .task import TaskRegistry


class TukioTask(asyncio.Task):
    """
    A simple subclass of `asyncio.Task()` to add an execution ID and optionally
    bind a task holder class.
    """
    def __init__(self, coro, *, loop=None):
        super().__init__(coro, loop=loop)
        self.holder = inspect.getcoroutinelocals(coro).get('self')
        try:
            self.uid = self.holder.uid
        except AttributeError:
            self.uid = str(uuid4())


def tukio_factory(loop, coro):
    """
    A task factory for asyncio that creates `TukioTask()` instances for all
    coroutines registered as Tukio tasks and default `asyncio.Task()` instances
    for all others.
    """
    try:
        _ = TaskRegistry.codes()[coro.cr_code]
        task = TukioTask(coro, loop=loop)
    except (KeyError, AttributeError):
        # When the coroutine is not a registered Tukio task or when `coro` is a
        # simple generator (e.g. upon calling `asyncio.wait()`)
        task = asyncio.Task(coro, loop=loop)
    return task
