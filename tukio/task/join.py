import asyncio
import logging

from .holder import TaskHolder
from .task import register


log = logging.getLogger(__name__)


@register('join', 'execute')
class JoinTask(TaskHolder):

    """
    A join task is a regular task that awaits multiple parents calls using data_received.
    Task can be overriden for custom behaviours.
    and decide wether or not unlock the task, wait depending on the calls received
    """

    SCHEMA = {
        'type': 'object',
        'required': ['wait_for'],
        'properties': {
            'wait_for': {
                'type': 'array',
                'minItems': 2,
                'maxItems': 64,
                'uniqueItems': True,
                'items': {
                    'type': 'string',
                    'maxLength': 1024
                }
            },
            'timeout': {
                'type': 'integer',
                'minimum': 1
            }
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        self.data_received(data, from_parent=self._parent_uid)
        await asyncio.wait_for(self.unlock.wait(), self.timeout)
        log.info('All parents joined: %s', self.wait_for)
        return self.data_stash

    def data_received(self, data, from_parent=None):
        """
        Is called when the task is started and a new parents finished.
        configuration takes the number of parents required to follow up.
        """
        if from_parent is None:
            return
        log.info("Parent '%s' joined", from_parent)
        log.debug('stashed data: %s', data)
        self.data_stash[from_parent] = data
        for parent in self.wait_for:
            if parent not in self.data_stash:
                break
        else:
            self.unlock.set()
