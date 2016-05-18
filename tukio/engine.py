"""
Tukio Workflow Engine
"""
import asyncio
import weakref
import logging

from tukio.workflow import WorkflowTemplate, OverrunPolicy, new_workflow
from tukio.broker import get_broker
from tukio.task import tukio_factory


log = logging.getLogger(__name__)


class DuplicateWorkflowError(Exception):
    pass


class Engine(asyncio.Future):
    def __init__(self, *, loop=None):
        super().__init__(loop=loop)
        # use the custom asyncio task factory
        self._loop.set_task_factory(tukio_factory)
        self._templates = dict()
        self._running = dict()
        self._broker = get_broker(self._loop)
        self._lock = asyncio.Lock()
        self._running_by_id = weakref.WeakValueDictionary()
        self._must_stop = False

    @property
    def templates(self):
        return self._templates

    def _add_wflow(self, wflow):
        """
        Adds a new entry into the dict of running workflows and updates the
        weak value dict to index it by its execution ID.
        """
        try:
            self._running[wflow.template_id].append(wflow)
        except KeyError:
            self._running[wflow.template_id] = [wflow]
        self._running_by_id[wflow.uid] = wflow
        log.debug('new workflow added to the running list: {}'.format(wflow))

    def _remove_wflow(self, wflow):
        """
        Removes a worflow instance from the dict of running workflows.
        """
        self._running[wflow.template_id].remove(wflow)
        # Cleanup the running dict if no more running instance of that template
        if len(self._running[wflow.template_id]) == 0:
            del self._running[wflow.template_id]
        del self._running_by_id[wflow.uid]
        log.debug('workflow removed from the running list: {}'.format(wflow))
        if self._must_stop and not self._running:
            self.set_result(None)
            log.debug('no more workflow running, engine stopped')

    def stop(self, force=False):
        """
        Cancels all workflows and prevent new instances from being run.
        """
        self._must_stop = True
        if force:
            self.cancel_all()

    def _run_in_task(self, callback, *args, **kwargs):
        """
        Wrap a regular function into a coroutine and run it in a task.
        This is intended to wrap time consuming functions into a task so as to
        prevent slow operations from blocking the whole loop.
        """
        async def coro():
            return callback(*args, **kwargs)
        return asyncio.ensure_future(coro(), loop=self._loop)

    def _load(self, tmpl_dict):
        """
        Loads a workflow template into the engine. Each workflow may be
        triggered as soon as it is loaded.
        Duplicates or invalid descriptions raise an exception.
        This operation does not affect workflow executions in progress.
        """
        wf_tmpl = WorkflowTemplate.from_dict(tmpl_dict)
        # A workflow template is uniquely defined by the tuple
        # (template ID, version)
        duplicate = None
        try:
            duplicate = self._templates[wf_tmpl.uid]
        except KeyError:
            self._templates[wf_tmpl.uid] = wf_tmpl
        else:
            if duplicate.version == wf_tmpl.version:
                raise DuplicateWorkflowError
            else:
                self._templates[wf_tmpl.uid] = wf_tmpl
        log.debug("new workflow template added '{}'".format(wf_tmpl.title))
        return wf_tmpl

    async def load(self, tmpl_dict):
        """
        A coroutine that loads a new workflow template while preventing other
        coroutines from updating the dict of loaded templates in the mean time.
        """
        with await self._lock:
            wf_tmpl = await self._run_in_task(self._load, tmpl_dict)
        return wf_tmpl

    async def reload(self, templates):
        """
        Replaces the current list of loaded workflow templates by a new one.
        This operation does not affect workflow executions in progress.
        """
        with await self._lock:
            self._templates = dict()
            for tmpl_dict in templates:
                await self._run_in_task(self._load, tmpl_dict)

    async def unload(self, template_id):
        """
        Unloads a workflow template from the engine. Returns True if the
        template was found and actually unloaded, else, returns False.
        """
        with await self._lock:
            try:
                del self._templates[template_id]
            except KeyError:
                return False
            return True

    async def data_received(self, data):
        """
        This method should be called to pass an event to the workflow engine
        which in turn will disptach this event to the right running workflows
        and may trigger new workflow executions.
        """
        self._broker.dispatch(data)
        # Don't start new workflow instances if `stop()` was called.
        if self._must_stop:
            return
        with await self._lock:
            # Try to trigger new workflows from the current dict of workflow
            # templates at all times!
            for tmpl_id in self._templates:
                await self._run_in_task(self._try_run, tmpl_id, data)

    def _try_run(self, tmpl_id, data):
        """
        Try to run a new instance of workflow defined by `tmpl_id` according to
        the instances already running and the overrun policy.
        """
        running = self._running.get(tmpl_id)
        try:
            wf_tmpl = self._templates[tmpl_id]
        except KeyError:
            # the workflow template is no longer loaded in the engine; nothing
            # to run!
            return
        # Always apply the policy of the current workflow template (workflow
        # instances may run with an old version of the template)
        wflow = new_workflow(wf_tmpl, running=running, loop=self._loop)
        if wflow:
            wflow.add_done_callback(self._remove_wflow)
            if wf_tmpl.policy == OverrunPolicy.abort_running and running:
                def cb():
                    wflow.run(data)
                asyncio.ensure_future(self._wait_abort(running, cb))
            else:
                wflow.run(data)
            self._add_wflow(wflow)

    async def _wait_abort(self, running, callback):
        """
        Wait for the end of a list of aborted (cancelled) workflows before
        starting a new one when the policy is 'abort-running'.
        """
        await asyncio.wait(running)
        callback()

    def run(self, tmpl_id, inputs):
        """
        Starts a new execution of the workflow template identified by `tmpl_id`
        regardless of the overrun policy and already running workflows.
        """
        wf_tmpl = self._templates[tmpl_id]
        wflow = new_workflow(wf_tmpl, loop=self._loop)
        wflow.add_done_callback(self._remove_wflow)
        wflow.run(inputs)
        self._add_wflow(wflow)
        return wflow

    def cancel(self, exec_id):
        """
        Cancels an execution of workflow identified by its execution ID.
        The cancelled workflow instance (a future object) is returned.
        If the workflow could not be found, returns None.
        """
        wflow = self._running_by_id.get(exec_id)
        if wflow:
            wflow.cancel()
            log.debug('cancelled workflow {}'.format(wflow))
        return wflow

    def cancel_all(self):
        """
        Cancels all the running workflows.
        """
        cancelled = 0
        for wf_list in self._running.values():
            for wflow in wf_list:
                is_cancelled = wflow.cancel()
                if is_cancelled:
                    cancelled += 1
        log.debug('cancelled {} workflows'.format(cancelled))
        return cancelled


if __name__ == '__main__':
    import sys
    from tukio import Engine
    from tukio.task import register
    logging.basicConfig(level=logging.DEBUG,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    ioloop = asyncio.get_event_loop()
    engine = Engine(loop=ioloop)

    @register('task1')
    async def task1(inputs=None):
        task = asyncio.Task.current_task()
        log.info('{} ==> received {}'.format(task.uid, inputs))
        log.info('{} ==> hello world #1'.format(task.uid))
        log.info('{} ==> hello world #2'.format(task.uid))
        await asyncio.sleep(0.5)
        log.info('{} ==> hello world #3'.format(task.uid))
        await asyncio.sleep(0.5)
        log.info('{} ==> hello world #4'.format(task.uid))
        return 'Oops I dit it again! from {}'.format(task.uid)

    @register('task2')
    async def task2(inputs=None):
        task = asyncio.Task.current_task()
        log.info('{} ==> received {}'.format(task.uid, inputs))
        log.info('{} ==> bye bob #1'.format(task.uid))
        await asyncio.sleep(0.5)
        log.info('{} ==> bye bob #2'.format(task.uid))
        log.info('{} ==> bye bob #3'.format(task.uid))
        await asyncio.sleep(0.5)
        log.info('{} ==> bye bob #4'.format(task.uid))
        return 'I love jamon! from {}'.format(task.uid)

    async def cancellator(future):
        await asyncio.sleep(2)
        future.cancel()

    wflow_dict1 = {
        "title": "workflow #1",
        "tasks": [
            {"id": "f1", "name": "task1"},
            {"id": "f2", "name": "task2"},
            {"id": "f3", "name": "task1"},
            {"id": "f4", "name": "task2"},
            {"id": "f5", "name": "task2"},
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

    wflow_dict2 = {
        "title": "workflow #2",
        "tasks": [
            {"id": "f1", "name": "task2"},
            {"id": "f2", "name": "task2"},
            {"id": "f3", "name": "task1"},
            {"id": "f4", "name": "task2"},
            {"id": "f5", "name": "task1"},
            {"id": "f6", "name": "task1"},
            {"id": "f7", "name": "task2"}
        ],
        "graph": {
            "f1": ["f2", "f3"],
            "f2": ["f4"],
            "f3": ["f5"],
            "f4": ["f6"],
            "f5": [],
            "f6": ["f7"],
            "f7": []
        }
    }
    asyncio.ensure_future(engine.load(wflow_dict1), loop=ioloop)
    asyncio.ensure_future(engine.load(wflow_dict2), loop=ioloop)
    asyncio.ensure_future(engine.data_received('apple'), loop=ioloop)
    ioloop.call_soon(engine.stop)

    # print("workflow uid: {}".format(wflow_tmpl.uid))
    # print("workflow graph: {}".format(wflow_tmpl.dag.graph))
    # print("workflow tasks: {}".format(wflow_tmpl.tasks))
    # print("workflow root task: {}".format(wflow_tmpl.root()))

    # Run the workflow
    try:
        res = ioloop.run_until_complete(engine)
    except Exception as exc:
        print("engine raised exception? {}".format(engine.exception()))
    else:
        print("engine returned: {}".format(engine.result()))
    finally:
        print("engine is done? {}".format(engine.done()))
