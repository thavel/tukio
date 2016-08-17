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
        # Can be a number of tasks or a list of ids
        self._wait_for = self.config['wait_for']
        self._timeout = self.config.get('timeout')

    async def execute(self, event):
        log.info(
            'Join task waiting for tasks (%s) (timeout: %s)',
            self._wait_for, self._timeout
        )
        # Trigger data_received for this event
        self.data_received(event)
        try:
            await asyncio.wait_for(self._unlock.wait(), self._timeout)
        except asyncio.TimeoutError:
            log.warning("Join timed out, still waiting for %s", self._wait_for)
        else:
            log.debug('All awaited parents joined')
        return self._data_stash

    def data_received(self, event):
        task_id = event.source.task_template_id
        log.debug("Task with id '%s' joined", task_id)
        # Check if wait for a number of tasks
        if isinstance(self._wait_for, int):
            self._wait_for -= 1
            if self._wait_for == 0:
                self._unlock.set()
        # Else, wait for list of ids
        elif isinstance(self._wait_for, list) and task_id in self._wait_for:
            self._wait_for.remove(task_id)
            if len(self._wait_for) == 0:
                self._unlock.set()
        else:
            # Don't stash unrelevant data
            return
        self._data_stash.append(event.data)
