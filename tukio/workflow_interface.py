import asyncio
import functools
import inspect
import logging

from .workflow import Workflow


log = logging.getLogger(__name__)


class WorkflowInterface(object):

    def __init__(self, task=None):
        self.task = task or asyncio.Task.current_task()
        self.workflow = self.get_workflow(self.task)

    @classmethod
    def get_workflow(cls, task):
        workflow = None
        for cb in task._callbacks:
            # inspect.getcallargs() gives acces to the implicit 'self' arg of the
            # bound method but it is marked as deprecated since Python 3.5.1
            # and the new `inspect.Signature` object does do the job :((
            if inspect.ismethod(cb):
                inst = cb.__self__
            elif isinstance(cb, functools.partial):
                try:
                    inst = cb.func.__self__
                except AttributeError:
                    continue
            else:
                continue
            if isinstance(inst, Workflow):
                workflow = inst

            if workflow is None:
                raise WorkflowNotFoundError
            return workflow

    def unlock_workflow_when_done(self):
        """
        Adds a done callback to the current task so as to unlock the workflow that
        handles its execution when the task gets done.
        If no workflow linked to the task can be found, raise an exception.
        It assumes all tasks scheduled from within a workflow object have at least
        one done callback which is a bound method from the workflow object.

        Note: bound methods from distinct workflow ojects in the task's
        "call when done" list are not supported. It must never happen!
        """
        self.task.add_done_callback(self.workflow.unlock)

    def disable_children(self, children, enable_others=False):
        """
        prevent the workflow from running children of the current task
        if enable_others, all non mentioned task will be enabled
        """
        log.debug('disabling children %s from %s' % (children, self))
        self.workflow.disable_children(self.task.uid, children, enable_others)

    def enable_children(self, children, disable_others=False):
        """
        ask these children to be run at the end of the task.
        if disable_others, all non montioned task will be disabled
        """
        log.debug('enabling children %s from %s' % (children, self))
        self.workflow.enable_children(self.task.uid, children, disable_others)
