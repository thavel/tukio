import asyncio
import logging

from .holder import TaskHolder
from .task import register


log = logging.getLogger(__name__)


@register('join', 'execute')
class JoinTask(TaskHolder):

    """
    A join task is an almost-standard task that allows awaiting for multiple
    parents and gathering their results using `data_received()`.
    The `wait_for` config parameter is mandatory and is a list of task IDs.
    """

    def __init__(self, config):
        super().__init__(config)
        self._unlock = asyncio.Event()
        self._data_stash = []
        # Don't wait for other tasks if not specified
        self._wait_for = list(self.config.get('wait_for', []))
        self._timeout = self.config.get('timeout')

    async def execute(self, event):
        log.info(
            'join task waiting for %s (timeout: %s)',
            self._wait_for,
            self._timeout
        )
        self.data_received(event)
        try:
            await asyncio.wait_for(self._unlock.wait(), self._timeout)
        except asyncio.TimeoutError:
            log.warning("join timeout, still waiting for %s", self._wait_for)
        else:
            log.debug('all awaited parents joined')
        return self._data_stash

    def data_received(self, event):
        """
        Called when the task is already started and a new parent just finished.
        """
        from_task = event.from_task
        data = event.data
        if from_task is None:
            log.warning("join task received data from unknown task: %s", event)
            return
        log.debug("parent task '%s' joined with data: %s", from_task, data)
        self._data_stash.append(data)
        if from_task in self._wait_for:
            self._wait_for.remove(from_task)
        if len(self._wait_for) == 0:
            self._unlock.set()
