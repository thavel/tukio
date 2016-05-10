import asyncio
import logging
import sys
import weakref
import functools
from uuid import uuid4

from tukio.dag import DAG
from tukio.task import TaskDescription


logger = logging.getLogger(__name__)


class WorkflowError(Exception):
    pass


class WorkflowRootTaskError(WorkflowError):
    pass


class WorkflowDescription(object):

    """
    A workflow description is a DAG (Directed Acyclic Graph) made up of task
    description objects (`TaskDescription`). This class is not a workflow
    execution engine.
    It provides an API to easily build and update a consistent workflow as well
    as create runnable workflow instances.
    """

    def __init__(self, uid=None):
        # Unique execution ID of the workflow
        self._uid = uid or "-".join(['wf', str(uuid4())[:8]])
        self._dag = DAG()
        self._tasks = weakref.WeakValueDictionary()

    @property
    def uid(self):
        return self._uid

    @property
    def dag(self):
        return self._dag

    @property
    def tasks(self):
        return dict(self._tasks)

    def add(self, task_desc):
        """
        Adds a new task description to the workflow. The task will remain
        orphan until it is linked to upstream/downstream tasks.
        This method must be passed a `TaskDescription()` instance.
        """
        if not isinstance(task_desc, TaskDescription):
            raise TypeError("expected a 'TaskDescription' instance")
        self.dag.add_node(task_desc)
        self._tasks[task_desc.uid] = task_desc

    def delete(self, task_desc):
        """
        Remove a task description from the workflow and delete the links to
        upstream/downstream tasks.
        """
        self.dag.delete_node(task_desc)

    def get(self, task_id):
        """
        Returns the task object that has the searched task ID. Returns 'None'
        if the task ID was not found.
        """
        return self._tasks.get(task_id, None)

    def root(self):
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

    def link(self, up_task_desc, down_task_desc):
        """
        Create a directed link from an upstream to a downstream task.
        """
        self.dag.add_edge(up_task_desc, down_task_desc)

    def unlink(self, task_desc1, task_desc2):
        """
        Remove the link between two tasks.
        """
        try:
            self.dag.delete_edge(task_desc1, task_desc2)
        except KeyError:
            self.dag.delete_edge(task_desc2, task_desc1)

    @classmethod
    def from_dict(cls, wf_dict):
        """
        Build a new workflow description from the given dictionary.
        The dictionary takes the form of:
            {
                "uid": <workflow-uid>
                "tasks": [
                    {"uid": <task-uid>, "name": <name>, "config": <cfg-dict>},
                    ...
                ],
                "graph": {
                    <t1-uid>: [t2-uid, <t3-uid>],
                    <t2-uid>: [],
                    ...
                }
            }
        """
        uid = wf_dict['uid']
        wf_desc = cls(uid)
        for task_dict in wf_dict['tasks']:
            task_desc = TaskDescription.from_dict(task_dict)
            wf_desc.add(task_desc)
        for up_id, down_ids_set in wf_dict['graph'].items():
            up_desc = wf_desc.get(up_id)
            for down_id in down_ids_set:
                down_desc = wf_desc.get(down_id)
                wf_desc.link(up_desc, down_desc)
        return wf_desc

    def as_dict(self):
        """
        Build a dictionary that represents the current workflow description
        object.
        """
        wf_dict = {"uid": self.uid, "tasks": [], "graph": {}}
        for task_id, task_desc in self.tasks.items():
            task_dict = {"uid": task_id, "name": task_desc.name}
            if task_desc.config:
                task_dict['config'] = task_desc.config
            wf_dict['tasks'].append(task_dict)
        for up_desc, down_descs_set in self.dag.graph.items():
            _record = {up_desc.uid: list(map(lambda x: x.uid, down_descs_set))}
            wf_dict['graph'].update(_record)
        return wf_dict


class Workflow(asyncio.Future):

    """
    This class handles the execution of a workflow.
    """

    def __init__(self, wf_desc, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._wf_desc = wf_desc
        self._wf_exec = dict()

    def _add_task(self, task, parent=None):
        """
        Adds a task to the exec dictionary and maintains the mapping dict
        between the task IDs and task objects.
        """
        try:
            _tasks = self._wf_exec[parent].add(task)
        except KeyError:
            pass
        self._wf_exec[task] = set()

    def run(self, *args, **kwargs):
        """
        Execute the workflow following the description passed at init.
        """
        # Run the root task
        root_desc = self._wf_desc.root()
        root_task = root_desc.run_task(*args, loop=self._loop, **kwargs)
        done_cb = functools.partial(self._run_next_tasks, root_desc)
        root_task.add_done_callback(done_cb)
        self._add_task(root_task)

    def _run_next_tasks(self, task_desc, future):
        """
        A callback to be added to each task in order to select and schedule
        asynchronously downstream tasks once the parent task is done.
        """
        # Don't keep executing the workflow if a task was cancelled.
        try:
            result = future.result()
        except asyncio.CancelledError as exc:
            return
        succ_descs = self._wf_desc.dag.successors(task_desc)
        for succ_desc in succ_descs:
            succ_task = succ_desc.run_task(result, loop=self._loop)
            done_cb = functools.partial(self._run_next_tasks, succ_desc)
            succ_task.add_done_callback(done_cb)
            self._add_task(succ_task, parent=future)
        # If nothing left to execute, the workflow is done
        for task in self._wf_exec.keys():
            if not task.done():
                break
        else:
            # The result of the future may have been set by another task
            # executed in the same iteration of the loop.
            if not self.done():
                self.set_result('done')

    def cancel(self):
        """
        Cancel the workflow by cancelling all pending tasks.
        Note: a 'pending' task from asyncio's point of view means that the
        coroutine has been scheduled and is not done. Aka from the workflow
        point of view, it means the task has been started and is not done.
        """
        for task in self._wf_exec.keys():
            if not task.done():
                task.cancel()
        super().cancel()


if __name__ == '__main__':
    from tukio import task
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    ioloop = asyncio.get_event_loop()

    @task.register('task1')
    async def task1(inputs=None, config=None):
        task = asyncio.Task.current_task()
        logger.info('{} ==> received {}'.format(task.uid, inputs))
        logger.info('{} ==> hello world #1'.format(task.uid))
        logger.info('{} ==> hello world #2'.format(task.uid))
        await asyncio.sleep(1)
        logger.info('{} ==> hello world #3'.format(task.uid))
        await asyncio.sleep(1)
        logger.info('{} ==> hello world #4'.format(task.uid))
        return 'Oops I dit it again! from {}'.format(task.uid)

    wf_dict = {
        "uid": "my-workflow",
        "tasks": [
            {"uid": "f1", "name": "task1"},
            {"uid": "f2", "name": "task1"},
            {"uid": "f3", "name": "task1"},
            {"uid": "f4", "name": "task1"},
            {"uid": "f5", "name": "task1"},
            {"uid": "f6", "name": "task1"}
        ],
        "graph": {
            "f1": ["f2"],
            "f2": ["f3", "f4"],
            "f3": ["f5"],
            "f4": ["f6"],
            "f5": [],
            "f6": []
        }
    }
    wf_desc = WorkflowDescription.from_dict(wf_dict)
    print("workflow uid: {}".format(wf_desc.uid))
    print("workflow graph: {}".format(wf_desc.dag.graph))
    print("workflow tasks: {}".format(wf_desc.tasks))
    print("workflow root task: {}".format(wf_desc.root()))
    print("workflow successors of root task: {}".format(wf_desc.dag.successors(wf_desc.root())))

    # Run the workflow
    wf_exec = Workflow(wf_desc, loop=ioloop)
    wf_exec.run('dada')
    ioloop.run_until_complete(wf_exec)
    print("workflow is done? {}".format(wf_exec.done()))
