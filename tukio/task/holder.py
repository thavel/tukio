"""
A task holder is a way to implement a Tukio task as a class and not only as a
simple standalone coroutine.
A task holder class must define a coroutine function that implements the Tukio
task.
To register a task holder class as a Tukio task, use the `@register()`
decorator:

    @register('my-holder', 'my_task_impl')
    class MyHolder:
        def __init__(self, config):
            # some code

        async def my_task_impl(self, *args, **kwargs):
            # some code

Which turns into a more compact class when you inherit your own class from
`TaskHolder`:

    @register('my-holder', 'my_task_impl')
    class MyHolder(TaskHolder):
        async def my_task_impl(self, *args, **kwargs):
            # some code

"""
from uuid import uuid4


class TaskHolder:

    """
    A base class that makes the implementation of task holders even easier.
    It is not mandatory to inherit your own task holder classes from this base
    class.
    The requirements of a task holder class are:
        1. the 1st argument passed to `__init__()` is the task's config.
        2. define a coroutine that implements the Tukio task
    If the task holder instance has a `uid` attribute it will be used by
    `TukioTask` as its own task ID (requires to use `tukio_factory` as the task
    factory).
    """

    TASK_NAME = None

    def __init__(self, config=None):
        self._config = config
        self._uid = str(uuid4())
        self._task = None

    @property
    def config(self):
        return self._config

    @property
    def uid(self):
        """
        Returns the task holder's ID.
        There's no setter for this attribute as it should never change.
        """
        return self._uid
