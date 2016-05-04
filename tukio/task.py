import asyncio
from functools import wraps
import logging
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
                               " has started".format(func))
    return wrapper


# Inspired from https://github.com/faif/python-patterns/blob/master/registry.py
class RegisteredTask(type):

    """
    This metaclass is designed to register automatically all classes that
    inherit from `Task`. Subclasses of `Task` are indexed according to their
    `NAME` attribute.
    """

    _registry = {}

    def __new__(cls, name, bases, attrs):
        new_cls = super().__new__(cls, name, bases, attrs)
        cls._registry[new_cls.NAME or new_cls.__name__.lower()] = new_cls
        return new_cls

    @classmethod
    def get(cls, task_name):
        return cls._registry[task_name]


class ConfigBeforeRun(RegisteredTask):

    """
    Ensure the `configure` method will always be decorated so as to prevent its
    execution at runtime or after task completion. This works even if `Task` is
    subclassed and `configure` is overriden without explicitly being decorated
    with `@before_run`.
    """

    def __new__(cls, name, bases, local):
        for attr in local:
            value = local[attr]
            if callable(value) and attr == 'configure':
                local[attr] = before_run(value)
        return super().__new__(cls, name, bases, local)


class Task(metaclass=ConfigBeforeRun):

    """
    A task is a standalone object that executes some logic. While executing, it
    may receive and/or throw events.
    It is the smallest piece of a workflow and is not aware of previous/next
    tasks it may be linked to in the workflow.
    A task can be configured with a dict-like object. If the config dict has a
    'timeout' key, it will be used as the task's timeout (a task has no timeout
    by default).
    """

    # All subclasses of `Task` will be automatically registered with a name.
    NAME = None

    def __init__(self, config=None, loop=None, broker=None):
        # Unique execution ID of the task
        self._uid = "-".join(['task', str(uuid4())[:8]])
        self._loop = loop or asyncio.get_event_loop()
        self._broker = broker
        self._future = None
        self._result = None
        # `config` must be a dictionary
        if config:
            self._timeout = config.get('timeout', None)
            self.configure(**config)
        else:
            self._timeout = None

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
        if self._future is None:
            self._future = asyncio.ensure_future(self.execute(data),
                                                 loop=self.loop)
        try:
            await asyncio.wait_for(self._future, self._timeout, loop=self.loop)
        except asyncio.TimeoutError:
            logger.warning("task '{}' timed out".format(self.uid))
            await self.on_timeout(data)
        except asyncio.CancelledError:
            logger.warning("task '{}' has been cancelled".format(self.uid))
            await self.on_cancel(data)
        else:
            return self._future.result()

    def cancel(self):
        """
        Cancel the task.
        The behavior of this method stick to `asyncio.Future.cancel()`.
        """
        if self._future is None:
            self._future = asyncio.Future()
        return self._future.cancel()

    def cancelled(self):
        """
        Return True if the future was cancelled.
        The behavior of this method stick to `asyncio.Future.cancelled()`.
        """
        if self._future is None:
            return False
        return self._future.cancelled()

    def result(self):
        """
        Return the result of the task.
        The behavior of this method stick to `asyncio.Future.result()`.
        """
        if self._future is None:
            # Raises InvalidStateError
            return asyncio.Future().result()
        return self._future.result()

    def done(self):
        """
        Return True if the task is done.
        Done means either that a result / exception are available, or that the
        task was cancelled.
        The behavior of this method stick to `asyncio.Future.done()`.
        """
        if self._future is None:
            return False
        return self._future.done()

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


class TaskDescription(object):

    """
    The complete description of a task is made of the registered name of the
    class that implements it and its configuration (dict).
    The class that implements the task must be a sub-class of `Task()`.
    With both of these attributes, an instance of task can be created and run.
    Optional `loop` and `broker` attributes can be set to be used in the
    instances of tasks created from the description.
    """

    name = None
    config = None

    def __init__(self, name, config=None, loop=None, broker=None, uid=None):
        self._uid = uid or "-".join(['task-desc', str(uuid4())[:8]])
        self.name = name
        self.config = config or dict()
        self.loop = loop
        self.broker = broker

    @property
    def uid(self):
        return self._uid

    def new_task(self, loop=None, broker=None):
        """
        Create a new instance of task from the description. The default event
        loop and event broker configured in the description can be overrridden.
        """
        if not self.name:
            raise ValueError("no task name in the description")
        t_loop = loop or self.loop
        t_broker = broker or self.broker
        klass = RegisteredTask.get(self.name)
        return klass(self.config, t_loop, t_broker)

    @classmethod
    def from_dict(cls, task_dict):
        """
        Create a new task description object from the given dictionary.
        The dictionary takes the form of:
            {
                "uid": <task-uid>,
                "name": <registered-task-name>,
                "config": <config-dict>
            }
        """
        uid = task_dict.get('uid')
        name = task_dict['name']
        config = task_dict.get('config')
        return cls(name, config=config, uid=uid)


if __name__ == '__main__':
    import sys
    from broker import Broker
    from threading import Event
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    ioloop = asyncio.get_event_loop()

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

        async def on_cancel(self, data=None):
            logger.info("called 'on_cancel' handler")

    class Task2(Task):
        async def execute(self, data=None):
            if isinstance(data, Task):
                logger.info("Input data is task UID={}".format(data.uid))
            for _ in range(3):
                logger.info("{} ==> fire is coming...".format(self.uid))
                await asyncio.sleep(1)
            await self.fire('hello world')

    brokr = Broker(loop=ioloop)

    # TEST1: configure 1 task and run it twice
    print("+++++++ TEST1")
    cfg = {'dummy': 'world'}
    t1 = Task1(config=cfg, broker=brokr, loop=ioloop)

    # Must raise InvalidStateError
    # t1.result()

    ioloop.run_until_complete(t1.run('continue'))
    print("Task is done?: {}".format(t1.done()))

    # Must raise RuntimeError
    # t1.configure(**cfg)

    # Does not re-run the task but returns the result
    # print("----")
    # print(loop.run_until_complete(t1.run('continue')))
    # print(t1.result())

    # TEST2: run 2 tasks running in parallel and using the broker
    print("+++++++ TEST2")
    t1 = Task1(config=cfg, loop=ioloop, broker=brokr)
    t2 = Task2(loop=ioloop, broker=brokr)
    t1.register(t2.uid, t1.on_event)
    tasks = [
        asyncio.ensure_future(t1.run()),
        asyncio.ensure_future(t2.run())
    ]
    ioloop.run_until_complete(asyncio.wait(tasks))

    # TEST3: cancel a task before running
    print("+++++++ TEST3")
    cfg = {'dummy': 'world'}
    t1 = Task1(config=cfg, broker=brokr, loop=ioloop)
    t1.cancel()
    ioloop.run_until_complete(t1.run('continue'))
    print("Task is cancelled?: {}".format(t1.cancelled()))

    ioloop.close()
