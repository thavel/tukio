import asyncio
import logging
import sys
import functools
from uuid import uuid4
from datetime import datetime

from tukio.dag import DAG
from tukio.task import TaskTemplate
from tukio.utils import future_state


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

    def __init__(self, title, uid=None, tags=None, version=None):
        self.title = title
        self.tags = tags or []
        self.uid = uid or str(uuid4())
        self.version = int(version) if version is not None else 1
        self.dag = DAG()

    @property
    def tasks(self):
        return list(self.dag.graph.keys())

    def add(self, task_tmpl):
        """
        Adds a new task template to the workflow. The task will remain orphan
        until it is linked to upstream/downstream tasks.
        This method must be passed a `TaskTemplate()` instance.
        """
        if not isinstance(task_tmpl, TaskTemplate):
            raise TypeError("expected a 'TaskTemplate' instance")
        self.dag.add_node(task_tmpl)

    def delete(self, task_tmpl):
        """
        Remove a task template from the workflow and delete the links to
        upstream/downstream tasks.
        """
        self.dag.delete_node(task_tmpl)

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
                "title": <workflow-title>,
                "id": <workflow-uid>,
                "tasks": [
                    {"id": <task-uid>, "name": <name>, "config": <cfg-dict>},
                    ...
                ],
                "graph": {
                    <t1-uid>: [t2-uid, <t3-uid>],
                    <t2-uid>: [],
                    ...
                }
            }
        """
        # 'title' is the only mandatory key
        title, uid = wf_dict['title'], wf_dict.get('id')
        tags, version = wf_dict.get('tags'), wf_dict.get('version')
        wf_tmpl = cls(title, uid=uid, tags=tags, version=version)
        task_ids = dict()
        for task_dict in wf_dict['tasks']:
            task_tmpl = TaskTemplate.from_dict(task_dict)
            wf_tmpl.add(task_tmpl)
            task_ids[task_tmpl.uid] = task_tmpl
        for up_id, down_ids_set in wf_dict['graph'].items():
            up_tmpl = task_ids[up_id]
            for down_id in down_ids_set:
                down_tmpl = task_ids[down_id]
                wf_tmpl.link(up_tmpl, down_tmpl)
        return wf_tmpl

    def as_dict(self):
        """
        Builds and returns a dictionary that represents the current workflow
        template object.
        """
        wf_dict = {
            "title": self.title, "id": self.uid,
            "tags": self.tags, "version": int(self.version),
            "tasks": [], "graph": {}
        }
        for task_tmpl in self.tasks:
            wf_dict['tasks'].append(task_tmpl.as_dict())
        for up_tmpl, down_tmpls_set in self.dag.graph.items():
            _record = {up_tmpl.uid: list(map(lambda x: x.uid, down_tmpls_set))}
            wf_dict['graph'].update(_record)
        return wf_dict

    def copy(self):
        """
        Returns a new instance that is a copy of the current workflow template.
        """
        wf_tmpl = WorkflowTemplate(self.title, uid=self.uid,
                                   tags=self.tags, version=self.version)
        wf_tmpl.dag = self.dag.copy()
        return wf_tmpl


class Workflow(asyncio.Future):

    """
    This class handles the execution of a workflow. Tasks are created along the
    way of workflow execution.
    """

    def __init__(self, wf_tmpl, *, loop=None):
        super().__init__(loop=loop)
        self.uid = str(uuid4())
        # Always act on a copy of the original workflow template
        self._wf_tmpl = wf_tmpl.copy()
        # Start and end datetime (UTC) of the execution of the workflow
        self._start, self._end = None, None
        # List of tasks executed
        self.tasks = []
        self._new_task_exc = None
        self._must_cancel = False

    def run(self, *args, **kwargs):
        """
        Execute the workflow following the description passed at init.
        """
        # A workflow can be ran only once
        if self.tasks:
            return
        # Run the root task
        root_tmpl = self._wf_tmpl.root()
        task = self._new_task(root_tmpl, (args, kwargs))
        self._start = datetime.utcnow()
        # The workflow may fail to start at once
        if not task:
            self._try_mark_done()

    def _new_task(self, task_tmpl, inputs, parent=None):
        """
        Each new task must be created successfully, else the whole workflow
        shall stop running (wrong workflow config or bug).
        """
        try:
            args, kwargs = inputs
            task = task_tmpl.new_task(*args, loop=self._loop, **kwargs)
        except Exception as exc:
            logger.warning('failed to created task for'
                           ' {}: {}'.format(task_tmpl, exc))
            self._new_task_exc = exc
            self._cancel_all_tasks()
            return None
        else:
            logger.debug('new task created for {}'.format(task_tmpl))
            done_cb = functools.partial(self._run_next_tasks, task_tmpl)
            task.add_done_callback(done_cb)
            self.tasks.append(task)
            return task

    def _run_next_tasks(self, task_tmpl, future):
        """
        A callback to be added to each task in order to select and schedule
        asynchronously downstream tasks once the parent task is done.
        """
        if self._must_cancel:
            self._try_mark_done()
            return
        # Don't execute downstream tasks if the task's result is an exception
        # (may include task cancellation) but don't stop executing the other
        # branches of the workflow.
        try:
            result = future.result()
        except Exception as exc:
            logger.warning('task {} ended on {}'.format(task_tmpl, exc))
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
            if self._new_task_exc:
                self.set_exception(self._new_task_exc)
                state = 'exception'
            elif self._must_cancel:
                state = 'cancelled'
                super().cancel()
            else:
                state = 'finished'
                self.set_result(self.tasks)
            self._end = datetime.utcnow()

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

    def report(self):
        """
        Creates and returns a complete execution report, including workflow and
        tasks templates and execution details.
        """
        wf_exec = {"id": self.uid, "start": self._start, "end": self._end}
        wf_exec['state'] = future_state(self)
        report = self._wf_tmpl.as_dict()
        report['exec'] = wf_exec
        return report


if __name__ == '__main__':
    import pprint
    from tukio.task import *
    logging.basicConfig(level=logging.DEBUG,
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
        "title": "my-workflow",
        "tasks": [
            {"id": "f1", "name": "task1"},
            {"id": "f2", "name": "task1"},
            {"id": "f3", "name": "task1"},
            {"id": "f4", "name": "task1"},
            {"id": "f5", "name": "task1"},
            {"id": "f6", "name": "task1"}
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
    try:
        res = ioloop.run_until_complete(wf_exec)
    except Exception as exc:
        print("workflow raised exception? {}".format(wf_exec.exception()))
    else:
        print("workflow returned: {}".format(res))
    finally:
        print("workflow is done? {}".format(wf_exec.done()))
        print("workflow report:")
        pprint.pprint(wf_exec.report())

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
