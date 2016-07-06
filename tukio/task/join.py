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

    IS_JOIN_TASK = True

    def __init__(self, config):
        super().__init__(config)
        self.unlock = asyncio.Event()
        self.data_stash = {}
        self.wait_for = self.config['wait_for']
        self.timeout = self.config.get('timeout')

    async def execute(self, data):
        log.info(
            'Join task waiting for %s (timeout: %s)',
            self.wait_for,
            self.timeout
        )
        await asyncio.wait_for(self.unlock.wait(), self.timeout)
        log.debug('All parents joined: %s', self.wait_for)
        return self.data_stash

    def data_received(self, data, from_parent=None):
        """
        Is called when the task is started and a new parents finished.
        configuration takes the number of parents required to follow up.
        """
        if from_parent is None:
            return
        log.debug("Parent '%s' joined with data: %s", from_parent, data)
        self.data_stash[from_parent] = data
        for parent in self.wait_for:
            if parent not in self.data_stash:
                break
        else:
            self.unlock.set()
