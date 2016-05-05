import asyncio
import logging
import abc
from uuid import uuid4


logger = logging.getLogger(__name__)


class TaskNoBrokerError(Exception):
    pass


# Inspired from https://github.com/faif/python-patterns/blob/master/registry.py
class RegisteredTask(abc.ABCMeta):

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


class _BaseTask(asyncio.Task):

    """
    A subclass of `asyncio.Task()` that does not schedule the execution of the
    coroutine at instantiation. A call to `_BaseTask.run()` does that job thus
    enabling better control over the scheduling of the execution of the
    coroutine.
    """

    def __init__(self, coro_fn, *, loop=None):
        """
        Takes a coroutine function instead of a coroutine object as argument.
        """
        assert asyncio.iscoroutinefunction(coro_fn), repr(coro_fn)
        asyncio.Future.__init__(self, loop=loop)
        if self._source_traceback:
            del self._source_traceback[-1]
        self._coro_fn = coro_fn
        self._coro = None
        self._fut_waiter = None
        self._must_cancel = False
        self._inputs = None
        # removed the scheduling of the coroutine and addition to the loop's
        # tasks.

    def run(self, inputs=None):
        """
        Create the coroutine object to run and schedules its execution in the
        task's event loop. It also adds the task to the list of loop's tasks.
        """
        self._inputs = inputs
        self._coro = self._coro_fn(inputs)
        self._loop.call_soon(self._step)
        self.__class__._all_tasks.add(self)


class Task(_BaseTask, metaclass=RegisteredTask):

    """
    A task is a standalone object that executes some logic.
    It is the smallest piece of a workflow and is not aware of previous/next
    tasks it may be linked to in the workflow.
    A task can be configured with a dict-like object. If the config dict has a
    'timeout' key, it will be used as the task's timeout (a task has no timeout
    by default).
    Cleanup actions on cancel or timeout can be implemented (noop by default)
    by overriding `Task.on_cancel()` or `Task.on_timeout()` methods.
    """

    # All subclasses of `Task` will be automatically registered with that name.
    # It is recommended to choose an short lowercase string, hyphen-separated
    # if required (e.g. 'my-task').
    NAME = None

    def __init__(self, config=None, loop=None):
        """
        Force the wrapping of the coroutine `Task.execute()` into the task and
        takes a new `config` dict-like argument that may be used by the
        coroutine at runtime.
        Before being wrapped into the task, `Task.execute()` is wrapped into
        another coroutine function that makes it timeout-able and calls
        cancel and timeout callbacks.
        """
        super().__init__(self._run_with_timeout, loop=loop)
        # Unique execution ID of the task
        self._uid = "-".join([str(self.NAME), str(uuid4())[:8]])
        # `config` is expected to be a dict-like object
        if config:
            self._timeout = config.pop('timeout', None)
        else:
            self._timeout = None

    @property
    def uid(self):
        return self._uid

    async def _run_with_timeout(self, inputs):
        """
        Wraps the coroutine `Task.execute()`
        """
        print("self: type={} and str={}".format(type(self), str(self)))
        coro = self.execute(inputs)
        self._future = fut = asyncio.ensure_future(coro, loop=self._loop)
        try:
            await asyncio.wait_for(fut, self._timeout, loop=self._loop)
        except asyncio.TimeoutError as exc:
            logger.warning("task '{}' timed out".format(self.uid))
            await self.on_timeout()
            self.set_exception(exc)
        except asyncio.CancelledError as exc:
            logger.warning("task '{}' has been cancelled".format(self.uid))
            await self.on_cancel()
        finally:
            return fut.result()

    @abc.abstractmethod
    async def execute(self, inputs):
        """
        This is the coroutine wrapped in the task. It holds the logic to
        execute. Override this method in each subclass of `Task()` to implement
        your own custom task.
        This coroutine can be cancelled or reach timeout.
        """

    async def on_cancel(self):
        """
        Override this method if you want to implement cleanup actions on
        cancel. This method may update task's result.
        No-Op by default.
        """

    async def on_timeout(self):
        """
        Override this method if you want to implement cleanup actions on
        timeout. This method may update task's result.
        No-Op by default.
        """


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
        async def execute(self, inputs=None):
            self._event = Event()
            # if inputs is not None:
            self._event.set()
            logger.info('{} ==> {}'.format(self.uid, inputs))
            logger.info('{} ==> hello world #1'.format(self.uid))
            while not self._event.is_set():
                await asyncio.sleep(1)
            logger.info('{} ==> hello world #2'.format(self.uid))
            await asyncio.sleep(1)
            logger.info('{} ==> hello world #3'.format(self.uid))
            await asyncio.sleep(1)
            logger.info('{} ==> hello world #4'.format(self.uid))
            return 'Oops I dit it again!'

        def on_event(self, inputs=None):
            self._event.set()
            logger.info('{} ==> Unlocked event'.format(self.uid))
            logger.info('{} ==> Received inputs: {}'.format(self.uid, inputs))

        def configure(self, dummy=None, other=None):
            if dummy is None and other is None:
                self.myconfig = 'everything is None in config'
            else:
                self.myconfig = 'got config: {} and {}'.format(dummy, other)

        async def on_cancel(self, inputs=None):
            logger.info("called 'on_cancel' handler")

    class Task2(Task):
        async def execute(self, inputs=None):
            if isinstance(inputs, Task):
                logger.info("Input data is task UID={}".format(inputs.uid))
            for _ in range(3):
                logger.info("{} ==> fire is coming...".format(self.uid))
                await asyncio.sleep(1)
            # await self.fire('hello world')

    brokr = Broker(loop=ioloop)

    # TEST1: configure 1 task and run it twice
    print("+++++++ TEST1")
    cfg = {'dummy': 'world'}
    t1 = Task1(config=cfg, loop=ioloop)

    # Must raise InvalidStateError
    try:
        t1.result()
    except asyncio.InvalidStateError as exc:
        print("raised {}".format(str(exc)))

    # Schedule the execution of the task and run it.
    t1.run()
    ioloop.run_until_complete(asyncio.wait([t1]))
    print("Task is done?: {}".format(t1.done()))
    print("Task's result is: {}".format(t1.result()))

    # Must raise RuntimeError
    # t1.configure(**cfg)

    # Does not re-run the task but returns the result
    # print("----")
    # print(loop.run_until_complete(t1.run('continue')))
    # print(t1.result())

    # TEST2: run 2 tasks running in parallel and using the broker
    # print("+++++++ TEST2")
    # t1 = Task1(config=cfg, loop=ioloop, broker=brokr)
    # t2 = Task2(loop=ioloop, broker=brokr)
    # t1.register(t2.uid, t1.on_event)
    # tasks = [
    #     asyncio.ensure_future(t1.run()),
    #     asyncio.ensure_future(t2.run())
    # ]
    # ioloop.run_until_complete(asyncio.wait(tasks))

    # TEST3: cancel a task before running
    # print("+++++++ TEST3")
    # cfg = {'dummy': 'world'}
    # t1 = Task1(config=cfg, broker=brokr, loop=ioloop)
    # t1.cancel()
    # ioloop.run_until_complete(t1.run('continue'))
    # print("Task is cancelled?: {}".format(t1.cancelled()))

    ioloop.close()
