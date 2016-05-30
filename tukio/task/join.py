import asyncio
from asyncio import InvalidStateError
import logging
from .holder import TaskHolder
from .task import register

log = logging.getLogger(__name__)


@register('join_task', 'execute')
class JoinTask(TaskHolder):
    """
    A join task is a regular task that awaits multiple parents calls using data_received.
    Task can be overriden for custom behaviours.
    and decide wether or not unlock the task, wait depending on the calls received
    """

    NAME = 'join_task'
    unlock = asyncio.Future()
    data_stash = []

    def ready(self):
        return len(self.data_stash) >= self.config.get('await_parents', 0)

    async def execute(self, data):
        log.debug('join task {} started.'.format(self))
        self.data_received(data)
        await self.unlock
        for call in self.data_stash:
            data.update(call)
        log.debug('join task {} done.'.format(self))
        return data

    async def data_received(self, data):
        """
        Is called when the task is started and a new parents finished.
        configuration takes the number of parents required to follow up.
        """
        log.debug('Received join task data {}'.format(data))
        self.data_stash.append(data)
        if self.ready():
            try:
                self.unlock.set_result('Done')
            except InvalidStateError:
                # Task may have already been unlocked
                pass
