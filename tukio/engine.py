"""
Tukio Workflow Engine
"""
import asyncio
import weakref

from tukio.workflow import WorkflowTemplate, OverrunPolicy, new_workflow
from tukio.broker import get_broker


class DuplicateWorkflowError(Exception):
    pass


class Engine:
    def __init__(self, *, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._templates = dict()
        self._running = dict()
        self._broker = get_broker(self._loop)
        self._lock = asyncio.Lock()
        self._running_by_id = weakref.WeakValueDictionary()

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

    def _remove_wflow(self, wflow):
        """
        Removes a worflow instance from the dict of running workflows.
        """
        self._running[wflow.template_id].remove(wflow)
        # Cleanup the running dict if no more running instance of that template
        if len(self._running[wflow.template_id]) == 0:
            del self._running[wflow.template_id]
        del self._running_by_id[wflow.uid]

    def stop(self):
        """
        Cancels all workflows and prevent new instances from being run.
        """

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

    async def load(self, tmpl_dict):
        """
        A coroutine that loads a new workflow template while preventing other
        coroutines from updating the dict of loaded templates in the mean time.
        """
        with await self._lock:
            await self._run_in_task(self._load, tmpl_dict)

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
            wflow.add_done_callback(self._clean_done)
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

    def _clean_done(self, future):
        """
        Cleanup actions when a workflow is done.
        """
        async def with_lock():
            with await self._lock:
                self._remove_wflow(future)
        asyncio.ensure_future(with_lock())

    def cancel(self, exec_id):
        """
        Cancels an execution of workflow identified by its execution ID.
        The cancelled workflow instance (a future object) is returned.
        If the workflow could not be found, returns None.
        """
        wflow = self._running_by_id.get(exec_id)
        if wflow and not wflow.done():
            wflow.cancel()
        return wflow
