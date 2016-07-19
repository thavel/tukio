"""
Any data received by the engine is wrapped into an event and passed to
workflows and tasks.
"""
import asyncio
import collections

from tukio.workflow import WorkflowInterface
from tukio.task import TukioTask


_BaseEvent = collections.namedtuple(
    '_BaseEvent', ('data', 'source'))
# Init default values to None
_BaseEvent.__new__.__defaults__ = (None,) * len(_BaseEvent._fields)


class Event(_BaseEvent):

    """
    An event is a structure that gathers data and the source of the event.
    The registered coroutine of each task receives an `Event` object as
    argument. The same way, `data_received` (only for task holders) is also
    called with an `Event` object as argument.
    """

    __slots__ = ()

    @classmethod
    def from_context(cls, data, topic=None):
        """
        Create and returns an instance of event from the current execution
        context.
        """
        task = asyncio.Task.current_task()
        workflow = WorkflowInterface(task)
        source = dict()
        if task and isinstance(task, TukioTask):
            source['task_template_id'] = task.template.uid
            source['task_exec_id'] = task.uid
        if workflow:
            source['workflow_template_id'] = workflow._workflow.template.uid
            source['workflow_exec_id'] = workflow._workflow.uid
        source['topic'] = topic
        return cls(data=data, source=EventSource(**source))


_BaseEventSource = collections.namedtuple(
    '_BaseEventSource', ('workflow_template_id', 'workflow_exec_id',
                         'task_template_id', 'task_exec_id', 'topic'))
# Init default values to None
_BaseEventSource.__new__.__defaults__ = (None,) * len(_BaseEventSource._fields)


class EventSource(_BaseEventSource):

    """
    An event source is a structure that gathers everything to know where an
    event comes from and/or the topics it was dispatched to.
    """

    __slots__ = ()
