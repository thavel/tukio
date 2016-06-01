"""Test all task modules from tukio.task"""
import unittest
import asyncio

from tukio.task import TaskHolder, register, tukio_factory, TukioTask


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


class TestTaskFactory(unittest.TestCase):
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
