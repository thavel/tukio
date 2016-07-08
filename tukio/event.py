"""
Any data received by the engine is wrapped into an event and passed to
workflows and tasks.
"""
import collections


_BaseEvent = collections.namedtuple(
    '_BaseEvent', ('data', 'from_task', 'topic'))
# Init default values to None
_BaseEvent.__new__.__defaults__ = (None,) * len(_BaseEvent._fields)


class Event(_BaseEvent):
    """
    An event is a structure that gathers input data (mandatory) and optionally
    the task it comes from and/or the topics it was dispatched to.
    The registered coroutine of each task receives an `Event` object as
    argument. The same way, `data_received` (only for task holders) is also
    called with an `Event` object as argument.
    """
    __slots__ = ()
