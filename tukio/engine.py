"""
Tukio Workflow Engine
"""
import asyncio

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
        self._lock = asyncio.Lock()

    def stop(self):
        pass

    def _load(self, templates):
        """
        Loads a list of workflow templates into the engine. Each workflow may
        be triggered as soon as it is loaded.
        Each item of the `templates` list must be a dictionary that represents
        a workflow template.
        Duplicates or invalid descriptions raise an exception.
        This operation does not affect workflow executions in progress.
        """
        for tmpl_dict in templates:
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

    async def load(self, templates):
        """
        A coroutine that loads a new set of workflow templates while preventing
        other coroutines from updating the dict of loaded templates in the mean
        time.
        """
        with await self._lock:
            self._load(templates)

    async def reload(self, templates):
        """
        Replaces the current list of loaded workflow templates by a new one.
        This operation does not affect workflow executions in progress.
        """
        with await self._lock:
            self._templates = dict()
            self.load(templates)

    async def data_received(self, data):
        """
        This method should be called to pass an event to the workflow engine
        which in turn will disptach this event to the right running workflows
        and may trigger new workflow executions.
        """
        self._broker.dispatch(data)
        with await self._lock:
            # Try to run new workflow instances at all times
            for tmpl_id in self._running:
                self._try_run(tmpl_id, data)
            # Trigger workflows from templates that has no running instance
            idle_tmpl_ids = self._get_idle_tmpl_ids()
            for tmpl_id in idle_tmpl_ids:
                self._try_run(tmpl_id, data)

    def _get_idle_tmpl_ids(self):
        """
        Returns the set of templates that has no running instance.
        """
        template_ids = set(self._templates.keys())
        running_tmpl_ids = set(self._running.keys())
        return template_ids - running_tmpl_ids

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
            if wf_tmpl.policy == OverrunPolicy.abort_running and running:
                def cb():
                    wflow.run(data)
                asyncio.ensure_future(self._wait_abort(running, cb))
            else:
                wflow.run(data)
            self._running[tmpl_id].append(wflow)

    async def _wait_abort(self, running, callback):
        """
        Wait for the end of a list of aborted (cancelled) workflows before
        starting a new one when the policy is 'abort-running'.
        """
        await asyncio.wait(running)
        callback()

    def cancel(self, exec_id):
        """
        Cancels an execution of workflow identified by its execution ID.
        """
