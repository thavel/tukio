from enum import Enum


class FutureState(Enum):

    """
    Lists the execution states. Each state has a string value:
        'pending': means the execution was scheduled in an event loop
        'cancelled': means the future is done but was cancelled
        'exception': means the future is done but raised an exception
        'finished': means the future is done and completed as expected
    Enum values are used in workflows/tasks's execution reports.
    """

    pending = 'pending'
    cancelled = 'cancelled'
    exception = 'exception'
    finished = 'finished'

    @classmethod
    def get(cls, future):
        """
        Returns the state of a future as `FutureState` member
        """
        if not future.done():
            return cls.pending
        if future.cancelled():
            return cls.cancelled
        if future._exception:
            return cls.exception
        return cls.finished


class Listen(Enum):

    """
    A simple enumeration of the expected behaviors of a task regarding its
    ability to receive new data during execution:
        'everything': receive all data dispatched by the event broker
        'nothing': receive no data at all during execution
        'topics': receive data dispatched only in template's topics
    """

    everything = "everything"
    nothing = "nothing"
    topics = "topics"

    @classmethod
    def get(cls, topics):
        """
        Returns event listening behavior of a task as a `Listen` member
        """
        if topics is None:
            return cls.everything
        elif topics == []:
            return cls.nothing
        elif isinstance(topics, list):
            return cls.topics
        else:
            raise TypeError("'{}' is not a list".format(topics))
