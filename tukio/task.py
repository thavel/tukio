import logging
import asyncio
from functools import wraps
from uuid import uuid4


logger = logging.getLogger(__name__)


class TaskNoBrokerError(Exception):
    pass


def before_run(func):
    """
    A simple decorator to prevent the execution of `func` after the task has
    been started.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._future is None:
            func(self, *args, **kwargs)
        else:
            raise RuntimeError("cannot call {} once the task"
                               " started".format(func))
    return wrapper


class ConfigBeforeRun(type):
    """
    Ensure the `configure` method will always be decorated so as to prevent its
    execution at runtime. This works even if a new subclass of `Task` is
    defined with its own `configure` method.
    """
    def __new__(cls, name, bases, local):
        for attr in local:
            value = local[attr]
            if callable(value) and attr == 'configure':
                local[attr] = before_run(value)
        return type.__new__(cls, name, bases, local)


class Task(object, metaclass=ConfigBeforeRun):
    """
    A task is a standalone object that executes some logic. While executing, it
    may receive and/or throw events.
    It is the smallest piece of a workflow and is not aware of previous/next
    tasks it may be linked to in the workflow.
    A task can be configured with a dict-like object.
    """
    # __metaclass__ = ConfigBeforeRun

    def __init__(self, config=None, timeout=None, loop=None, broker=None):
        # Unique execution ID of the task
        self._uid = "-".join(['task', str(uuid4())[:8]])
        self._loop = loop or asyncio.get_event_loop()
        # self._workflow = workflow
        self._timeout = timeout
        self._broker = broker
        self._future = None
        self._result = None
        if config is not None:
            # `config` must be a dictionary
            self.configure(**config)

    @property
    def uid(self):
        return self._uid

    @property
    def loop(self):
        """
        Read-only attribute to access the event loop.
        """
        return self._loop

    def configure(self, **kwargs):
        """
        Configure the task prior to running it. Override this method to setup
        your own configuration.
        This method is automatically decorated with `@before_run`.
        """
        raise NotImplementedError

    async def run(self, data=None):
        """
        A task can run only once and can either be cancel or timeout.
        """
        if self._future is not None:
            raise RuntimeError('a task can be run only once')
        self._future = asyncio.ensure_future(self.execute(data),
                                             loop=self._loop)
        try:
            self._result = await asyncio.wait_for(self._future, self._timeout,
                                                  loop=self._loop)
        except asyncio.TimeoutError:
            logger.warning("task '{}' timed out")
            await self.on_timeout(data)
        except asyncio.CancelledError:
            logger.warning("task '{}' has been cancelled")
            await self.on_cancel(data)

    async def cancel(self):
        if self._future is not None:
            self._future.cancel()
        else:
            logger.warning("task '{}' not started, cannot be "
                           "cancelled!".format(self.uid))

    async def execute(self, data=None):
        """
        Override this method to code your own logic.
        """
        raise NotImplementedError

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

    async def on_timeout(self, data=None):
        """
        This method is executed on timeout. Override this method to code your
        own logic (noop by default).
        """
        pass

    async def on_cancel(self, data=None):
        """
        This method is executed on cancel. Override this method to code your
        own logic (noop by default).
        """
        pass


if __name__ == '__main__':
    import sys
    from broker import Broker
    from threading import Event
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    loop = asyncio.get_event_loop()

    class Task1(Task):
        async def execute(self, data=None):
            self._event = Event()
            if data is not None:
                self._event.set()
            logger.info('{} ==> {}'.format(self.uid, self.myconfig))
            logger.info('{} ==> hello world #1'.format(self.uid))
            while not self._event.is_set():
                await asyncio.sleep(1)
            logger.info('{} ==> hello world #2'.format(self.uid))
            await asyncio.sleep(1)
            logger.info('{} ==> hello world #3'.format(self.uid))
            await asyncio.sleep(1)
            logger.info('{} ==> hello world #4'.format(self.uid))
            return 'Oops I dit it again!'

        def on_event(self, data=None):
            self._event.set()
            logger.info('{} ==> Unlocked event'.format(self.uid))
            logger.info('{} ==> Received data: {}'.format(self.uid, data))

        def configure(self, dummy=None, other=None):
            if dummy is None and other is None:
                self.myconfig = 'everything is None in config'
            else:
                self.myconfig = 'got config: {} and {}'.format(dummy, other)


    class Task2(Task):
        async def execute(self, data=None):
            if isinstance(data, Task):
                logger.info("Input data is task UID={}".format(data.uid))
            for i in range(3):
                logger.info("{} ==> fire is coming...".format(self.uid))
                await asyncio.sleep(1)
            await self.fire('hello world')

    broker = Broker(loop=loop)

    ## TEST1: configure 1 task and run it
    print("+++++++ TEST1")
    config = {'dummy': 'world'}
    t1 = Task1(config=config, broker=broker, loop=loop)
    loop.run_until_complete(t1.run('continue'))
    # Must raise RuntimeError
    # t1.configure(**config)
    # loop.run_until_complete(t1.run('continue'))

    ## TEST2: run 2 tasks running in parallel and using the broker
    print("+++++++ TEST2")
    t1 = Task1(config=config, loop=loop, broker=broker)
    t2 = Task2(loop=loop, broker=broker)
    t1.register(t2.uid, t1.on_event)
    tasks = [
        asyncio.ensure_future(t1.run()),
        asyncio.ensure_future(t2.run())
    ]
    loop.run_until_complete(asyncio.wait(tasks))

    loop.close()
