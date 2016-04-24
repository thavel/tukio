import logging
import asyncio
from uuid import uuid4


logger = logging.getLogger(__name__)


class TaskNoBrokerError(Exception):
    pass


class Task(object):
    """
    A task is a standalone object that executes some logic. While executing, it
    may receive and/or throw events.
    It is the smallest piece of a workflow and doesn't know about his
    parent/children.
    """
    def __init__(self, workflow=None, timeout=None, loop=None, broker=None):
        # Unique execution ID of the task
        self._uid = str(uuid4())[:8]
        self._loop = loop or asyncio.get_event_loop()
        self._workflow = workflow
        self._timeout = timeout
        self._broker = broker
        self._task = None

    @property
    def uid(self):
        return self._uid

    @property
    def workflow(self):
        return self._workflow

    @workflow.setter
    def workflow(self, workflow):
        """
        You can link a task to a workflow only before the task has started.
        """
        if self._task is None:
            self._workflow = workflow
        else:
            raise RuntimeError

    @property
    def loop(self):
        return self._loop

    def _make_timeout_event(self):
        return {'reason': 'timeout', 'result': self._result}

    def _make_cancel_event(self):
        return {'reason': 'cancel', 'result': self._result}

    async def run(self, data=None):
        self._task = asyncio.ensure_future(self.execute(data), loop=self._loop)
        try:
            self._result = await asyncio.wait_for(self._task, self._timeout,
                                                  loop = self._loop)
        except asyncio.TimeoutError:
            logger.warning("task '{}' timed out")
            await self.fire(self._make_timeout_event())
        except asyncio.CancelledError:
            logger.warning("task '{}' has been cancelled")
            await self.fire(self._make_cancel_event())

    async def cancel(self):
        if self._task is not None:
            self._task.cancel()
        else:
            logger.warning("task '{}' not started, cannot be "
                           "cancelled!".format(self.uid))

    async def execute(self, data=None):
        """
        Override this method to code your own logic.
        """
        raise NotImplementedError()

    def register(self, topic, handler):
        """
        A simple wrapper to register handlers from within the task. Note that
        a task cannot handle its own events.
        """
        if self._broker is None:
            raise TaskNoBrokerError
        if topic == self.uid:
            raise ValueError("A task cannot handle its own events")
        self._broker.register(topic, handler)

    async def fire(self, data):
        """
        A simple wrapper to fire an event from within the task. A task always
        fire events on a topic identified by its own UID.
        """
        if self._broker is None:
            raise TaskNoBrokerError
        await self._broker.fire(self.uid, data)
