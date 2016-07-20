"""
Any data received by the engine is wrapped into an event and passed to
workflows and tasks.
"""
import collections


_BaseEvent = collections.namedtuple(
    '_BaseEvent', ('data', 'topic', 'source'))
# Init default values to None, except 'data' which is always mandatory
_BaseEvent.__new__.__defaults__ = (None,) * (len(_BaseEvent._fields) - 1)


class Event(_BaseEvent):

    """
    An event is a structure that gathers data and optionally the topic it shall
    be dispatched to and the source of the event.
    The registered coroutine of each task receives an `Event` object as
    argument. The same way, `data_received` (only for task holders) is also
    called with an `Event` object as argument.
    """

    __slots__ = ()


_BaseEventSource = collections.namedtuple(
    '_BaseEventSource', ('workflow_template_id', 'workflow_exec_id',
                         'task_template_id', 'task_exec_id'))
# Init default values to None
_BaseEventSource.__new__.__defaults__ = (None,) * len(_BaseEventSource._fields)


class EventSource(_BaseEventSource):

    """
    An event source is a structure that gathers everything to know where an
    event comes from and/or the topics it will be dispatched to.
    """

    __slots__ = ()
