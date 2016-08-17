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
        self._data_stash = []
        # Must be either a number of tasks or a list of ids
        self._wait_for = self.config['wait_for']
        self._timeout = self.config.get('timeout')

    def _step(self, event):
        """
        Take a new event and consume it with the `wait_for` configuration.
        """
        task_id = event.source.task_template_id
        # Check if wait for a number of tasks
        if isinstance(self._wait_for, int):
            self._data_stash.append(event.data)
            self._wait_for -= 1
            if self._wait_for == 0:
                return True
        # Else, wait for list of ids
        elif isinstance(self._wait_for, list) and task_id in self._wait_for:
            self._data_stash.append(event.data)
            self._wait_for.remove(task_id)
            if len(self._wait_for) == 0:
                return True
        return False

    async def _wait_for_tasks(self):
        while True:
            new_event = await self.queue.get()
            if self._step(new_event) is True:
                return

    async def execute(self, event):
        log.info(
            'Join task waiting for tasks (%s) (timeout: %s)',
            self._wait_for, self._timeout
        )
        # Trigger first step for this event
        self._step(event)
        try:
            await asyncio.wait_for(self._wait_for_tasks(), self._timeout)
        except asyncio.TimeoutError:
            log.warning("Join timed out, still waiting for %s", self._wait_for)
        else:
            log.debug('All awaited parents joined')
        return self._data_stash
