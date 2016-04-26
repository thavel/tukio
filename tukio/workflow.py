import logging
import sys
import asyncio
from task import Task
from broker import Broker
from threading import Event
from uuid import uuid4


logger = logging.getLogger(__name__)

# from task import Task, State


class Workflow(object):
    """
    A workflow ties together a set of tasks as a DAG (Directed Acyclic Graph).
    A workflow instance can run only once.
    """
    def __init__(self):
        # Unique execution ID of the workflow
        self._uid = "-".join(['wf', str(uuid4())[:8]])

    def run(self):
        pass

    def cancel(self):
        pass

# class Workflow(object):

#     def __init__(self, task, loop=None):
#         assert isinstance(task, Task)
#         self._loop = loop or asyncio.get_event_loop()
#         self._initial_data = None
#         self._state = State.pending
#         task.workflow = self
#         self._root_task = task

#     @property
#     def root_task(self):
#         return self._root_task

#     @property
#     def exec_id(self):
#         return self._root_task.id

#     @property
#     def loop(self):
#         return self._loop

#     @property
#     def initial_data(self):
#         return self._initial_data

#     def get_tasks(self, state=None):
#         """
#         Return the final tasks required for the workflow to end
#         """
#         return self._root_task.get_tasks(state)

#     async def run(self, data=None):
#         """
#         Run the workflow, waiting for the final tasks to end
#         """
#         if self._state != State.pending:
#             print('Already done')
#             return
#         self._state = State.in_progress
#         self._initial_data = data or {}
#         asyncio.ensure_future(self._root_task.run())
#         done, pending = await asyncio.wait(self._root_task.final_tasks)
#         self._state = State.done
#         # for future in done:
#         #         future.result()
#         return done

#     async def cancel(self):
#         """
#         Recursively cancel all remaining tasks
#         """
#         await self._root_task.cancel()


# === Testing ===

class Task1(Task):
    async def execute(self, data=None):
        self._event = Event()
        logger.info('{} ==> hello world #1'.format(self._uid))
        while not self._event.is_set():
            await asyncio.sleep(1)
        logger.info('{} ==> hello world #2'.format(self._uid))
        await asyncio.sleep(1)
        logger.info('{} ==> hello world #3'.format(self._uid))
        await asyncio.sleep(1)
        logger.info('{} ==> hello world #4'.format(self._uid))
        return 'Oops I dit it again!'

    def on_event(self, data=None):
        self._event.set()
        logger.info('{} ==> Unlocked event'.format(self.uid))
        logger.info('{} ==> Received data: {}'.format(self.uid, data))


class Task2(Task):
    async def execute(self, data=None):
        if isinstance(data, Task):
            logger.info("Input data is task UID={}".format(data.uid))
        for i in range(3):
            logger.info("{} ==> fire is coming...".format(self.uid))
            await asyncio.sleep(1)
        await self.fire('hello world')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    loop = asyncio.get_event_loop()

    ## TEST1
    # t1 = Task(loop=loop)
    # t2 = Task(loop=loop)
    # tasks = [
    #     asyncio.ensure_future(t1.run(t2)),
    #     asyncio.ensure_future(t2.run()),
    # ]
    # loop.run_until_complete(asyncio.wait(tasks))

    ## TEST2
    # broker = Broker(loop=loop)
    # task = Task1(loop=loop, broker=broker)
    # task.register('toto')
    # tasks = [
    #     asyncio.ensure_future(task.run()),
    #     asyncio.ensure_future(broker.fire('toto', 'hello'))
    # ]
    # loop.run_until_complete(asyncio.wait(tasks))
    # loop.close()

    ## TEST3
    broker = Broker(loop=loop)
    t1 = Task1(loop=loop, broker=broker)
    t2 = Task2(loop=loop, broker=broker)
    t1.register(t2.uid, t1.on_event)
    tasks = [
        asyncio.ensure_future(t1.run(t2)),
        asyncio.ensure_future(t2.run())
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
