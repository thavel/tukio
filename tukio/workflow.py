import asyncio
import logging
import sys
import weakref
import functools
from uuid import uuid4

from tukio.dag import DAG
from tukio.task import TaskTemplate


logger = logging.getLogger(__name__)


class WorkflowError(Exception):
    pass


class WorkflowRootTaskError(WorkflowError):
    pass


class WorkflowTemplate(object):

    """
    A workflow template is a DAG (Directed Acyclic Graph) made up of task
    template objects (`TaskTemplate`). This class is not a workflow execution
    engine.
    It provides an API to easily build and update a consistent workflow.
    """

    def __init__(self, uid=None):
        # Unique execution ID of the workflow
        self.uid = uid or str(uuid4())
        self._dag = DAG()
        self._tasks = weakref.WeakValueDictionary()

    @property
    def dag(self):
        return self._dag

    @property
    def tasks(self):
        return dict(self._tasks)

    def add(self, task_tmpl):
        """
        Adds a new task template to the workflow. The task will remain orphan
        until it is linked to upstream/downstream tasks.
        This method must be passed a `TaskTemplate()` instance.
        """
        if not isinstance(task_tmpl, TaskTemplate):
            raise TypeError("expected a 'TaskTemplate' instance")
        self.dag.add_node(task_tmpl)
        self._tasks[task_tmpl.uid] = task_tmpl

    def delete(self, task_tmpl):
        """
        Remove a task template from the workflow and delete the links to
        upstream/downstream tasks.
        """
        self.dag.delete_node(task_tmpl)

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

    def link(self, up_task_tmpl, down_task_tmpl):
        """
        Create a directed link from an upstream to a downstream task.
        """
        self.dag.add_edge(up_task_tmpl, down_task_tmpl)

    def unlink(self, task_tmpl1, task_tmpl2):
        """
        Remove the link between two tasks.
        """
        try:
            self.dag.delete_edge(task_tmpl1, task_tmpl2)
        except KeyError:
            self.dag.delete_edge(task_tmpl2, task_tmpl1)

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
        wf_tmpl = cls(uid)
        for task_dict in wf_dict['tasks']:
            task_tmpl = TaskTemplate.from_dict(task_dict)
            wf_tmpl.add(task_tmpl)
        for up_id, down_ids_set in wf_dict['graph'].items():
            up_desc = wf_tmpl.get(up_id)
            for down_id in down_ids_set:
                down_desc = wf_tmpl.get(down_id)
                wf_tmpl.link(up_desc, down_desc)
        return wf_tmpl

    def as_dict(self):
        """
        Build a dictionary that represents the current workflow description
        object.
        """
        wf_dict = {"uid": self.uid, "tasks": [], "graph": {}}
        for task_id, task_tmpl in self.tasks.items():
            task_dict = {"uid": task_id, "name": task_tmpl.name}
            if task_tmpl.config:
                task_dict['config'] = task_tmpl.config
            wf_dict['tasks'].append(task_dict)
        for up_desc, down_descs_set in self.dag.graph.items():
            _record = {up_desc.uid: list(map(lambda x: x.uid, down_descs_set))}
            wf_dict['graph'].update(_record)
        return wf_dict


class Workflow(asyncio.Future):

    """
    This class handles the execution of a workflow. Tasks are created along the
    way of workflow execution.
    """

    def __init__(self, wf_tmpl, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._wf_tmpl = wf_tmpl
        self._wf_exec = dict()
        self._task_exception = None
        self._must_cancel = False

    @property
    def tasks(self):
        """
        Returns the list of all tasks pending or done.
        """
        return list(self._wf_exec.keys())

    def _add_task(self, task, parent=None):
        """
        Adds a task to the exec graph and optionally adds an edge from the
        parent task to this task.
        """
        self._wf_exec[task] = set()
        if parent:
            self._wf_exec[parent].add(task)

    def run(self, *args, **kwargs):
        """
        Execute the workflow following the description passed at init.
        """
        # A workflow can be ran only once
        if self._wf_exec:
            return
        # Run the root task
        root_tmpl = self._wf_tmpl.root()
        self._new_task(root_tmpl, (args, kwargs))

    def _new_task(self, task_tmpl, inputs, parent=None):
        """
        Each new task must be created successfully, else the whole workflow
        shall stop running.
        """
        try:
            args, kwargs = inputs
            task = task_tmpl.new_task(*args, loop=self._loop, **kwargs)
        except Exception as exc:
            cancelled = self._cancel_all_tasks()
            # if there are pending tasks being cancelled, we must wait for
            # those tasks to be done and delay the call to `set_exception()`
            if cancelled > 0:
                self._task_exception = exc
                return None
            else:
                self.set_exception(exc)
                raise
        else:
            done_cb = functools.partial(self._run_next_tasks, task_tmpl)
            task.add_done_callback(done_cb)
            self._add_task(task, parent=parent)

    def _run_next_tasks(self, task_tmpl, future):
        """
        A callback to be added to each task in order to select and schedule
        asynchronously downstream tasks once the parent task is done.
        """
        if self._must_cancel:
            self._try_mark_done()
            return
        # Don't execute downstream tasks if the task's result is an exception
        # (may include task cancellation) but don't stop the other branches
        # of the workflow.
        try:
            result = future.result()
        except Exception as exc:
            pass
        else:
            succ_tmpls = self._wf_tmpl.dag.successors(task_tmpl)
            for succ_tmpl in succ_tmpls:
                inputs = ((result,), {})
                succ_task = self._new_task(succ_tmpl, inputs, parent=future)
                if not succ_task:
                    break
        finally:
            self._try_mark_done()

    def _try_mark_done(self):
        """
        If nothing left to execute, the workflow must be marked as done. The
        result is set to either the exec graph (represented by a dict) or to an
        exception raised at task creation.
        """
        # Note: the result of the workflow may already have been set by another
        # done callback from another task executed in the same iteration of the
        # event loop.
        if self._all_tasks_done() and not self.done():
            if self._task_exception:
                self.set_exception(self._task_exception)
            elif self._must_cancel:
                super().cancel()
            else:
                self.set_result(self._wf_exec)

    def _all_tasks_done(self):
        """
        Returns True if all tasks are done, else returns False.
        """
        for task in self.tasks:
            if not task.done():
                return False
        return True

    def _cancel_all_tasks(self):
        """
        Cancels all pending tasks and returns the number of tasks cancelled.
        """
        self._must_cancel = True
        cancelled = 0
        for task in self.tasks:
            if not task.done():
                task.cancel()
                cancelled += 1
        return cancelled

    def cancel(self):
        """
        Cancel the workflow by cancelling all pending tasks (aka all tasks not
        marked as done). We must wait for all tasks to be actually done before
        marking the workflow as cancelled (hence done).
        """
        cancelled = self._cancel_all_tasks()
        if cancelled == 0:
            super().cancel()
        return True


if __name__ == '__main__':
    from tukio.task import *
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    ioloop = asyncio.get_event_loop()
    ioloop.set_task_factory(tukio_factory)

    @register('task1')
    async def task1(inputs=None):
        task = asyncio.Task.current_task()
        logger.info('running task: {}'.format(task))
        logger.info('{} ==> received {}'.format(task.uid, inputs))
        logger.info('{} ==> hello world #1'.format(task.uid))
        logger.info('{} ==> hello world #2'.format(task.uid))
        await asyncio.sleep(0.5)
        logger.info('{} ==> hello world #3'.format(task.uid))
        await asyncio.sleep(0.5)
        logger.info('{} ==> hello world #4'.format(task.uid))
        return 'Oops I dit it again! from {}'.format(task.uid)

    async def cancellator(future):
        await asyncio.sleep(2)
        future.cancel()

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
    wf_tmpl = WorkflowTemplate.from_dict(wf_dict)
    print("workflow uid: {}".format(wf_tmpl.uid))
    # print("workflow graph: {}".format(wf_tmpl.dag.graph))
    # print("workflow tasks: {}".format(wf_tmpl.tasks))
    print("workflow root task: {}".format(wf_tmpl.root()))

    # Run the workflow
    wf_exec = Workflow(wf_tmpl, loop=ioloop)
    wf_exec.run('simple')
    res = ioloop.run_until_complete(wf_exec)
    print("workflow returned: {}".format(res))
    print("workflow is done? {}".format(wf_exec.done()))

    # Run again the same workflow
    wf_exec.run('again')
    res = ioloop.run_until_complete(wf_exec)
    print("workflow returned: {}".format(res))
    print("workflow is done? {}".format(wf_exec.done()))

    # Run and cancel the workflow
    wf_exec = Workflow(wf_tmpl, loop=ioloop)
    cancel_task = asyncio.ensure_future(cancellator(wf_exec))
    wf_exec.run('cancel')
    futs = [wf_exec, cancel_task]
    res = ioloop.run_until_complete(asyncio.wait(futs))
    print("workflow returned: {}".format(res))
    print("workflow is done? {}".format(wf_exec.done()))
    print("workflow is cancelled? {}".format(wf_exec.cancelled()))
