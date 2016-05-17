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


def future_state(future):
    """
    Returns the state of a future as an enumeration member (from `FutureState`)
    """
    if not future.done():
        return FutureState.pending
    if future.cancelled():
        return FutureState.cancelled
    if future._exception:
        return FutureState.exception
    return FutureState.finished
