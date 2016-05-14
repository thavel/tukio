"""
A task holder is a way to implement a Tukio task as a class and not only as a
simple standalone coroutine.
A task holder class must define a coroutine function and a `task_created()`
method.
To register a task holder class as a Tukio task, use the `@register()`
decorator:

    @register('my-holder', 'my_task_impl')
    class MyHolder:
        def __init__(self, config):
            # some code

        def task_created(self, task):
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


class TaskHolder:

    """
    A base class that makes the implementation of task holders even easier.
    It is not mandatory to inherit your own task holder classes from this base
    class.
    The requirements of a task holder class are:
        1. the 1st argument passed to `__init__()` is the task config.
        2. define a `task_created()` method that receives an `asyncio.Task()`
           instance as its only argument
        3. define a coroutine that implements the Tukio task
    """

    TASK_NAME = None

    def __init__(self, config=None):
        self._config = config
        self._uid = None
        self._task = None

    def task_created(self, task):
        """
        Called when the task holder coroutine registered as a Tukio task
        implementation has been wrapped in a `asyncio.Task()`.
        """
        self._task = task
        # if the custom tukio factory (asyncio) is used, the task object has
        # an exec id
        try:
            self._uid = task.uid
        except AttributeError:
            pass

    @property
    def config(self):
        return self._config

    @property
    def uid(self):
        return self._uid

    @property
    def task(self):
        return self._task
