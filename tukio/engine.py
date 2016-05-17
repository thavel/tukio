"""
Tukio Workflow Engine
"""
import asyncio

from tukio.workflow import WorkflowTemplate, OverrunPolicy, new_workflow
from tukio.broker import get_broker


class DuplicateWorkflowError(Exception):
    pass


class WorkflowEngine:
    def __init__(self, *, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self.templates = dict()
        self._running = dict()
        self._broker = get_broker()

    def start(self):
        pass

    def stop(self):
        pass

    def load(self, templates):
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
                duplicate = self.templates[wf_tmpl.uid]
            except KeyError:
                self.templates[wf_tmpl.uid] = wf_tmpl
            else:
                if duplicate.version == wf_tmpl.version:
                    raise DuplicateWorkflowError
                else:
                    self.templates[wf_tmpl.uid] = wf_tmpl

    def reload(self, templates):
        """
        Replaces the current list of loaded workflow templates by a new one.
        This operation does not affect workflow executions in progress.
        """
        self.templates = dict()
        self.load(templates)

    def data_received(self, data, topic=None):
        """
        This method should be called to pass an event to the workflow engine
        which in turn will disptach this event to the right workflows.
        """
        self._broker.fire(data, topic)
        # Try to run new workflow instances at all times
        for tmpl_id in self._running:
            self._try_run(tmpl_id)

    def _try_run(self, tmpl_id):
        """
        Try to run a new instance of workflow defined by `tmpl_id` according to
        the instances already running and the overrun policy.
        """
        running = self._running[tmpl_id]
        try:
            wf_tmpl = self.templates[tmpl_id]
        except KeyError:
            # the workflow template is no longer loaded in the engine; nothing
            # to run!
            return
        # Always apply the policy of the current workflow template (workflow
        # instances may run with an old version of the template)
        wflow = new_workflow(wf_tmpl, running=running, loop=self._loop)
        if wflow:
            if wf_tmpl.policy == OverrunPolicy.abort_running and running:
                asyncio.ensure_future(self._wait_abort(wflow, running))
            else:
                wflow.run()

    async def _wait_abort(self, wflow, running):
        """
        Wait for the end of a list of aborted (cancelled) workflows before
        starting a new one when the policy is 'abort-running'.
        """
        await asyncio.wait(running)
        wflow.run()
