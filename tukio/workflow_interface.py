import asyncio
import functools
import inspect
import logging

from .workflow import Workflow


log = logging.getLogger(__name__)


class WorkflowInterface(object):

    workflow = None
    task = None

    def __init__(self):
        self.get_workflow()

    def get_workflow(self):
        self.task = asyncio.Task.current_task()

        for cb in self.task._callbacks:
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
                self.workflow = inst

            if self.workflow is None:
                raise WorkflowNotFoundError

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
        self.task.add_done_callback(inst._unlock)

    def disable_children(self, children):
        """
        prevent the workflow from running children of the current tasks
        """
        log.info('disabling children %s from %s' % (children, self))
        log.warning(dir(self.task.holder))
        self.workflow.disable_children(self.task.uid, children)
