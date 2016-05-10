"""
That module assumes that a Tukio task fully relies on `asyncio.Task`.
Hence the complete logic of a task is described through a coroutine function or
method.
It provides a registry class and a set of handful functions to deal with those
coroutines and schedule their execution in an event loop.
"""
import asyncio
import logging
import functools
from uuid import uuid4


logger = logging.getLogger(__name__)


# Inspired from https://github.com/faif/python-patterns/blob/master/registry.py
class _TaskRegistry(object):

    """
    This class is designed to be a registry of all functions and methods that
    implement a Tukio task. Functions or methods are registered by name.
    """

    _registry = dict()

    @classmethod
    def register(cls, func, task_name):
        """
        Adds the function or method `func` to the registry dict.
        """
        if task_name in cls._registry:
            raise ValueError("task '{}' already registered with "
                             "{}".format(task_name, cls._registry[task_name]))
        cls._registry[task_name] = func


def register(task_name):
    """
    A simple decorator to register a function or a method as a Tukio task.
    """
    def decorator(func):
        _TaskRegistry.register(func, task_name)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


def get_coro_fn(task_name):
    """
    Returns the function or method registered as `task_name`.
    """
    return _TaskRegistry._registry[task_name]


def all_task_names():
    """
    Returns the list of all task names.
    """
    return list(_TaskRegistry._registry.keys())


def _make_task_uid(task_name):
    """
    Returns a unique ID to tag an `asyncio.Task` instance with.
    """
    return "-".join([str(task_name), str(uuid4())[:8]])


def run_task(task_name, *args, loop=None, **kwargs):
    """
    Schedules the execution of the coroutine registered as `task_name` and
    returns the resulting `asyncio.Task` instance.
    """
    coro_fn = get_coro_fn(task_name)
    coro = coro_fn(*args, **kwargs)
    task = asyncio.ensure_future(coro, loop=loop)
    # Add useful attributes to the instance of `asyncio.Task`.
    task.uid = _make_task_uid(task_name)
    task.inputs = (args, kwargs)
    return task


class TaskDescription(object):

    """
    The complete description of a task is made of the registered name of the
    coroutine that implements it and its configuration (a dict).
    With the config dict and and input data (optional) passed as arguments, an
    instance of task can be created and run.
    """

    name = None
    config = None

    def __init__(self, name, config=None, uid=None):
        self._uid = uid or "-".join(['task-desc', str(uuid4())[:8]])
        self.name = name
        self.config = config or dict()

    @property
    def uid(self):
        return self._uid

    def run_task(self, *args, loop=None, **kwargs):
        """
        Pass the config dict and input data as arguments to the coroutine
        function and schedule its execution in the event loop.
        """
        return run_task(self.name, *args, loop=loop,
                        config=self.config, **kwargs)

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

    @register('task1')
    async def task1(inputs=None, config=None):
        logger.info('unknown ID ==> {}'.format(inputs))
        logger.info('==> hello world #1')
        logger.info('==> hello world #2')
        await asyncio.sleep(1)
        logger.info('==> hello world #3')
        await asyncio.sleep(1)
        logger.info('==> hello world #4')
        return 'Oops I dit it again!'


    # TEST1: configure 1 task and run it twice
    print("+++++++ TEST1")
    cfg = {'dummy': 'world'}
    t1 = run_task('task1', loop=ioloop)

    # Must raise InvalidStateError
    try:
        t1.result()
    except asyncio.InvalidStateError as exc:
        print("raised {}".format(str(exc)))

    # Schedule the execution of the task and run it.
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
