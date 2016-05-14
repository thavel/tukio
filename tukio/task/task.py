"""
That module assumes that a Tukio task runs as an `asyncio.Task()` instance.

It provides a registry `TaskRegistry` to register classes that define
coroutines as well as standard standalone coroutines as Tukio tasks.

It also provides a function `run_task()` that shall be used to schedule the
execution of Tukio tasks in an asyncio event loop.
"""
import asyncio
import logging
import inspect


logger = logging.getLogger(__name__)


# Inspired from https://github.com/faif/python-patterns/blob/master/registry.py
class TaskRegistry:

    """
    This class is designed to be a registry of all coroutines and classes that
    implement a Tukio task. Coroutines and classes are registered by name.
    Each record of the registry is a tuple (class, coro_func). If the
    registered implementation is a simple coroutine, class=None.
    """

    _registry = dict()
    _codes = dict()

    @classmethod
    def register(cls, coro_or_cls, task_name, coro_name=None):
        # A task name can be registered only once!
        if task_name in cls._registry:
            klass, coro_fn = cls._registry[task_name]
            raise ValueError("task name '{}' already registered with "
                             "({}, {})".format(task_name, klass, coro_fn))

        if inspect.isclass(coro_or_cls):
            klass, coro_fn = coro_or_cls, getattr(coro_or_cls, coro_name)
        else:
            klass, coro_fn = None, coro_or_cls
        if not asyncio.iscoroutinefunction(coro_fn):
            raise TypeError("{} is not a coroutine function".format(coro_fn))
        cls._registry[task_name] = klass, coro_fn
        cls._codes[coro_fn.__code__] = task_name

    @classmethod
    def get(cls, task_name):
        return cls._registry[task_name]

    @classmethod
    def all(cls):
        return cls._registry

    @classmethod
    def codes(cls):
        return cls._codes


def register(task_name, coro_name=None):
    """
    A decorator to register a standard coroutine or a task holder class as a
    Tukio task implementation.
    This decorator adds the class attribute `TASK_NAME` to task holder classes.
    """
    def decorator(coro_or_cls):
        TaskRegistry.register(coro_or_cls, task_name, coro_name)
        if inspect.isclass(coro_or_cls):
            coro_or_cls.TASK_NAME = task_name
        return coro_or_cls
    return decorator


def run_task(task_name, inputs=((), {}), config=None, loop=None):
    """
    Schedules the execution of the coroutine registered as `task_name` (either
    defined in a task holder class or not) in the loop and returns an instance
    of `asyncio.Task()` (or a subclass of it).
    """
    klass, coro_fn = TaskRegistry.get(task_name)
    args, kwargs = inputs
    if klass:
        task_holder = klass(config)
        coro = coro_fn(task_holder, *args, **kwargs)
    else:
        task_holder = None
        coro = coro_fn(*args, **kwargs)
    task = asyncio.ensure_future(coro, loop=loop)
    # Give the opportunity to link the asyncio task object to the task holder
    if task_holder:
        task_holder.task_created(task)
    return task


if __name__ == '__main__':
    import sys
    from tukio.task import *

    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    ioloop = asyncio.get_event_loop()
    ioloop.set_task_factory(tukio_factory)

    @register('holder-task', 'execute')
    class MyTaskHolder(TaskHolder):
        async def execute(self, arg=None):
            logger.info('{} ==> inputs: {}'.format(self.uid, arg))
            logger.info('{} ==> config: {}'.format(self.uid, self.config))
            await asyncio.sleep(0.5)
            logger.info('{} ==> {} had a good rest!'.format(self.uid,
                                                            self.TASK_NAME))
            if isinstance(arg, TukioTask):
                arg.cancel()
            await asyncio.sleep(0.5)
            return 'hello from task holder'

    @register('coro-task')
    async def mytask(*args, **kwargs):
        task = asyncio.Task.current_task()
        logger.info('{} ==> inputs: {}, {}'.format(task.uid, args, kwargs))
        logger.info('{} ==> hello world #1'.format(task.uid))
        logger.info('{} ==> hello world #2'.format(task.uid))
        await asyncio.sleep(0.5)
        logger.info('{} ==> hello world #3'.format(task.uid))
        await asyncio.sleep(1)
        logger.info('{} ==> hello world #4'.format(task.uid))
        return 'Oops I dit it again!'

    print('Registry object from main: {}'.format(TaskRegistry))

    # TEST1: configure 1 task and run it twice
    print("+++++++ TEST1")
    cfg = {'dummy': 'world'}

    # run from a standalone coroutine
    t1 = run_task('coro-task', inputs=(('hello',), {}), loop=ioloop)
    print('Task is {}'.format(t1))
    print("Task exec id: {}".format(t1.uid))

    # run from a task holder
    t2 = run_task('holder-task', config=cfg, loop=ioloop)

    # Must raise InvalidStateError
    try:
        t1.result()
    except asyncio.InvalidStateError as exc:
        print("raised {}".format(str(exc)))

    # Schedule the execution of the tasks and run it.
    tasks = [t1, t2]
    ioloop.run_until_complete(asyncio.wait(tasks))
    print("Task {} is done?: {}".format(t1.uid, t1.done()))
    print("Task's result is: {}".format(t1.result()))
    print("Task {} is done?: {}".format(t2.uid, t2.done()))
    print("Task's result is: {}".format(t2.result()))

    # TEST2: run 2 tasks running in parallel, one cancels the other
    print("+++++++ TEST2")
    t1 = run_task('coro-task', config=cfg, loop=ioloop)
    t2 = run_task('holder-task', inputs=((t1,), {}), loop=ioloop)
    tasks = [t1, t2]
    ioloop.run_until_complete(asyncio.wait(tasks))
    print("Task {} is done?: {}".format(t1.uid, t1.done()))
    try:
        result = t1.result()
    except asyncio.CancelledError as exc:
        result = 'Task was cancelled!'
    print("Task's result is: {}".format(result))
    print("Task {} is done?: {}".format(t2.uid, t2.done()))
    print("Task's result is: {}".format(t2.result()))

    ioloop.close()
