import asyncio
import inspect
from uuid import uuid4

from .task import TaskRegistry, TaskExecState
from tukio.broker import get_broker, EXEC_TOPIC


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
        self._broker = get_broker(self._loop)
        self._in_progress = False
        self._template = None

    @property
    def template(self):
        return self._template

    def in_progress(self):
        """
        Returns True if the task execution started, else returns False.
        """
        return self._in_progress

    def set_result(self, result):
        """
        Wrapper around `Future.set_result()` to automatically dispatch a
        `TaskExecState.end` event.
        """
        super().set_result(result)
        data = {'type': TaskExecState.end.value, 'content': result}
        self._broker.dispatch(data=data, topic=EXEC_TOPIC)

    def set_exception(self, exception):
        """
        Wrapper around `Future.set_exception()` to automatically dispatch a
        `TaskExecState.error` event.
        """
        super().set_exception(exception)
        data = {'type': TaskExecState.error.value, 'content': exception}
        self._broker.dispatch(data=data, topic=EXEC_TOPIC)

    def _step(self, exc=None):
        """
        Wrapper around `Task._step()` to automatically dispatch a
        `TaskExecState.begin` event.
        """
        super()._step(exc)
        if not self._in_progress:
            self._in_progress = True
            data = {'type': TaskExecState.begin.value}
            self._broker.dispatch(data=data, topic=EXEC_TOPIC)


def tukio_factory(loop, coro):
    """
    A task factory for asyncio that creates `TukioTask()` instances for all
    coroutines registered as Tukio tasks and default `asyncio.Task()` instances
    for all others.
    """
    try:
        # Trigger exception if not valid
        TaskRegistry.codes()[coro.cr_code]
        task = TukioTask(coro, loop=loop)
    except (KeyError, AttributeError):
        # When the coroutine is not a registered Tukio task or when `coro` is a
        # simple generator (e.g. upon calling `asyncio.wait()`)
        task = asyncio.Task(coro, loop=loop)
    return task
