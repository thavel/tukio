def future_state(future):
    """
    Returns a string that represents the state of a future:
        "pending": means the execution was scheduled in an event loop
        "cancelled": means the future is done but was cancelled
        "exception": means the future is done but raised an exception
        "finished": means the future is done and completed as expected
    Those strings are meant to be used in workflows/tasks's execution reports.
    """
    if not future.done():
        return 'pending'
    if future.cancelled():
        return 'cancelled'
    if future._exception:
        return 'exception'
    return 'finished'
