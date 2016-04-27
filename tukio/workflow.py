import logging
import sys
import asyncio
import weakref
from uuid import uuid4
from dag import DAG
from task import Task


logger = logging.getLogger(__name__)


class WorkflowError(Exception):
    pass


class WorkflowRootTaskError(WorkflowError):
    pass


class Workflow(object):
    """
    This class models a workflow. A workflow is a DAG (Directed Acyclic Graph)
    made up of tasks.
    This class is meant to hold the description of a workflow; it is not a
    workflow execution engine.
    It provides an API to easily build and update a consistent graph.
    """
    def __init__(self):
        # Unique execution ID of the workflow
        self._uid = "-".join(['wf', str(uuid4())[:8]])
        self._dag = DAG()
        self._tasks = weakref.WeakValueDictionary()

    @property
    def dag(self):
        return self._dag

    def add_task(self, task):
        """
        Adds a new task to the workflow. The task will remain orphan until it
        is linked to upstream/downstream tasks.
        This method must be passed a `Task()` instance.
        """
        if not isinstance(task, Task):
            raise TypeError("task must an 'Task()' instance")
        self.dag.add_node(task)
        self._tasks[task.uid] = task

    def delete_task(self, task):
        """
        Remove a task from the workflow and the links to upstream/downstream
        tasks.
        """
        self.dag.delete_node(task)

    def get_task(self, task_id):
        """
        Returns the task object that has the searched task ID. Returns 'None'
        if the task ID was not found.
        """
        return self._tasks.get(task_id, None)

    def root_task(self):
        """
        Returns the root task. If no root task or several root tasks were found
        raises `WorkflowValidationError`.
        """
        root_task = self.dag.root_nodes()
        if len(root_task) == 1:
            return root_task[0]
        else:
            raise WorkflowRootTaskError("expected one root task, "
                                        "found {}".format(root_task))

    def set_downstream(self, task, down_tasks):
        """
        Set a task, or a task task to be directly downstream from the current
        task.
        """
        for dt in down_tasks:
            self.dag.add_edge(task, dt)

    def set_upstream(self, task, up_tasks):
        """
        Set a task, or a task task to be directly upstream from the current
        task.
        """
        for ut in up_tasks:
            self.dag.add_edge(ut, task)


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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    loop = asyncio.get_event_loop()
