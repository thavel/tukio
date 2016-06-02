"""Test all task modules from tukio.task"""
import unittest
import asyncio
from contextlib import contextmanager

from tukio.task import (
    TaskHolder, register, tukio_factory, TukioTask, TaskRegistry,
    UnknownTaskName, new_task
)


MY_TASK_HOLDER_RES = 'Tukio task as task holder'
MY_CORO_TASK_RES = 'Tukio task as coroutine'


@register('my-task-holder', 'do_it')
class MyTaskHolder(TaskHolder):
    async def do_it(self):
        return MY_TASK_HOLDER_RES


@register('my-coro-task')
async def my_coro_task():
    return MY_CORO_TASK_RES


async def other_coro():
    return None


def my_func():
    return None

# utils


@contextmanager
def _temp_registration():
    """
    Temporarily add new registrations to the current TaskRegistry
    """
    try:
        saved_registry = TaskRegistry._registry.copy()
        saved_codes = TaskRegistry._codes.copy()
        yield
    finally:
        TaskRegistry._registry = saved_registry
        TaskRegistry._codes = saved_codes

# tests


class TestTaskRegistry(unittest.TestCase):

    """
    Test task holder and coroutine registrations
    """

    def test_current_registry(self):
        """
        Check the current registry (provisioned by registered task holder and
        coroutine) holds the right references.
        """
        # A task holder is registered as a tuple made of (klass, coro_fn)
        klass, coro_fn = TaskRegistry.get('my-task-holder')
        self.assertIs(klass, MyTaskHolder)
        self.assertIs(coro_fn, MyTaskHolder.do_it)

        # A coroutine is registered as a tuple made of (None, coro_fn)
        klass, coro_fn = TaskRegistry.get('my-coro-task')
        self.assertIs(klass, None)
        self.assertIs(coro_fn, my_coro_task)

    def test_registry_get_unknown(self):
        """
        Looking for an unknown Tukio task must raise a `KeyError` exception
        """
        with self.assertRaises(UnknownTaskName):
            TaskRegistry.get('dummy')

    def test_register_not_coroutine(self):
        """
        Trying to register a function or method that is not a coroutine must
        raise a `TypeError` exception.
        """
        err = 'not a coroutine function'
        registered = len(TaskRegistry.all())

        # A regular function cannot be registered
        with self.assertRaisesRegex(TypeError, err):
            register('dummy')(my_func)

        # A generator cannot be registered either
        def dummy():
            yield 'dummy'
        with self.assertRaisesRegex(TypeError, err):
            register('dummy')(dummy)
        with self.assertRaisesRegex(TypeError, err):
            register('dummy')(dummy())

        # A simple method is not a valid task
        class DummyTask:
            def dummy(self):
                pass
        with self.assertRaisesRegex(TypeError, err):
            register('dummy-task', 'dummy')(DummyTask)

        # No new item must have been registered
        self.assertEqual(registered, len(TaskRegistry.all()))

    def test_register_same_name(self):
        """
        Trying to register a Tukio task with a name already used must raise a
        `ValueError` exception.
        """
        with self.assertRaisesRegex(ValueError, 'already registered'):
            register('my-coro-task')(other_coro)

    def test_bad_register_class(self):
        """
        Trying to register a class with invalid/missing parameters passed to
        `register()` must raise a `ValueError` or `AttributeError` exception.
        """
        registered = len(TaskRegistry.all())

        class DummyTask:
            async def dummy(self):
                pass

        # Register a class without the name of the method to register
        with self.assertRaisesRegex(TypeError, 'getattr()'):
            register('dummy-task')(DummyTask)

        # Register a class with an invalid `coro_name` arg
        with self.assertRaisesRegex(TypeError, 'getattr()'):
            register('dummy-task', object())(DummyTask)

        # Register a class with a wrong method name
        with self.assertRaisesRegex(AttributeError, 'yolo'):
            register('dummy-task', 'yolo')(DummyTask)

        # No new item must have been registered
        self.assertEqual(registered, len(TaskRegistry.all()))

    def test_register_basic_class(self):
        """
        A class that does not inherit from `TaskHolder` is still a valid task
        holder if registered properly.
        """
        class DummyTask:
            async def dummy(self):
                pass

        with _temp_registration():
            register('dummy-task', 'dummy')(DummyTask)
            klass, coro_fn = TaskRegistry.get('dummy-task')
            self.assertIs(klass, DummyTask)
            self.assertIs(coro_fn, DummyTask.dummy)


class TestNewTask(unittest.TestCase):

    """
    Test new tasks are created as expected from registered Tukio tasks.
    """

    def test_new_task_ok(self):
        """
        Various cases which must lead to create asyncio tasks successfully.
        """
        # Create a task from a task holder
        task = new_task('my-task-holder')
        self.assertTrue(isinstance(task, asyncio.Task))

        # Create a task from a simple coroutine
        task = new_task('my-coro-task')
        self.assertTrue(isinstance(task, asyncio.Task))

    def test_new_task_unknown(self):
        """
        Cannot create a new task from an unknown name. It must raise a KeyError
        exception (just like `TaskRegistry.get`).
        """
        with self.assertRaises(UnknownTaskName):
            new_task('dummy')

    def test_new_task_bad_inputs(self):
        """
        Trying to create a new asyncio task with invalid inputs must raise
        a `ValueError` exception.

        XXX: see TODO in the code!
        """
        with self.assertRaisesRegex(ValueError, 'too many values to unpack'):
            new_task('my-coro-task', inputs='dummy')

    def test_new_task_bad_holder(self):
        """
        Trying to create a new task from an invalid task holder may raise
        various exceptions. Ensure those exceptions are raised by `new_task`.
        """
        # __init__ takes no arg/kwarg
        class DummyTask1:
            async def execute(self):
                pass

        with _temp_registration():
            register('dummy-task', 'execute')(DummyTask1)
            with self.assertRaises(TypeError):
                new_task('dummy-task', config={'hello': 'world'})

        # __init__ takes two args
        class DummyTask2:
            def __init__(self, config, dummy):
                pass
            async def execute(self):
                pass

        with _temp_registration():
            register('dummy-task', 'execute')(DummyTask2)
            with self.assertRaises(TypeError):
                new_task('dummy-task', config={'hello': 'world'})

        # __init__ tries to perform an invalid operation on the config dict
        class MyException(Exception):
            pass

        class DummyTask3:
            def __init__(self, config):
                raise MyException
            async def execute(self):
                pass

        with _temp_registration():
            register('dummy-task', 'execute')(DummyTask3)
            with self.assertRaises(MyException):
                new_task('dummy-task')


class TestTaskFactory(unittest.TestCase):

    """
    Test the expected behaviors with and without the Tukio task factory set
    in the event loop.
    """

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()
        cls.holder = MyTaskHolder()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    def test_with_tukio_factory(self):
        """
        The tukio task factory must create `TukioTask` when the coroutine is a
        registered Tukio task, else it must create a regular `asyncio.Task`
        object.
        """
        self.loop.set_task_factory(tukio_factory)

        # Create and run a `TukioTask` from a registered task holder
        task = asyncio.ensure_future(self.holder.do_it())
        self.assertTrue(isinstance(task, TukioTask))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_TASK_HOLDER_RES)

        # Create and run a `TukioTask` from a registered coroutine
        task = asyncio.ensure_future(my_coro_task())
        self.assertTrue(isinstance(task, TukioTask))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_CORO_TASK_RES)

        # Run a regular coroutine
        task = asyncio.ensure_future(other_coro())
        self.assertTrue(isinstance(task, asyncio.Task))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, None)

        # Run a generator (e.g. as returned by `asyncio.wait`)
        # The task factory is also called in this situation and is passed the
        # generator object.
        t1 = asyncio.ensure_future(my_coro_task())
        t2 = asyncio.ensure_future(other_coro())
        self.assertTrue(isinstance(t1, TukioTask))
        self.assertTrue(isinstance(t2, asyncio.Task))
        gen = asyncio.wait([t1, t2])
        # A task is created inside `asyncio.run_until_complete` anyway...
        task = asyncio.ensure_future(gen)
        self.assertTrue(isinstance(task, asyncio.Task))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, ({t1, t2}, set()))

        # Tukio task factory does not affect futures passed to `ensure_future`
        future = asyncio.ensure_future(asyncio.Future())
        self.assertTrue(isinstance(future, asyncio.Future))

    def test_without_tukio_factory(self):
        """
        When the Tukio task factory is not set in the loop, all coroutines must
        be wrapped in a regular `asyncio.Task` object regardless of whether
        they are registered Tukio tasks or not.
        """
        # Reset to default task factory
        self.loop.set_task_factory(None)

        # Create and run a Tukio task implemented as a task holder
        task = asyncio.ensure_future(self.holder.do_it())
        self.assertTrue(isinstance(task, asyncio.Task))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_TASK_HOLDER_RES)

        # Create and run a Tukio task implemented as a coroutine
        task = asyncio.ensure_future(my_coro_task())
        self.assertTrue(isinstance(task, asyncio.Task))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, MY_CORO_TASK_RES)

        # Create and run a regular `asyncio.Task`
        task = asyncio.ensure_future(other_coro())
        self.assertTrue(isinstance(task, asyncio.Task))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, None)

        # Run a generator (e.g. as returned by `asyncio.wait`)
        # The task factory is also called in this situation and is passed the
        # generator object.
        t1 = asyncio.ensure_future(my_coro_task())
        t2 = asyncio.ensure_future(other_coro())
        self.assertTrue(isinstance(t1, asyncio.Task))
        self.assertTrue(isinstance(t2, asyncio.Task))
        gen = asyncio.wait([t1, t2])
        # A task is created inside `asyncio.run_until_complete` anyway...
        task = asyncio.ensure_future(gen)
        self.assertTrue(isinstance(task, asyncio.Task))
        res = self.loop.run_until_complete(task)
        self.assertEqual(res, ({t1, t2}, set()))


if __name__ == '__main__':
    unittest.main()
