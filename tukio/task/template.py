from uuid import uuid4
from .task import new_task


class TaskTemplate:

    """
    The complete description of a Tukio task is made of its registered name and
    its configuration (a dict).
    """

    def __init__(self, name, config=None, uid=None):
        self.name = name
        self.config = config or dict()
        self.uid = uid or str(uuid4())

    def new_task(self, *args, loop=None, **kwargs):
        """
        Create a new task from the current task template.
        """
        inputs = (args, kwargs)
        return new_task(self.name, inputs=inputs,
                        config=self.config, loop=loop)

    @classmethod
    def from_dict(cls, task_dict):
        """
        Create a new task description object from the given dictionary.
        The dictionary takes the form of:
            {
                "uid": <task-uid>,
                "name": <registered-task-name>,
                "config": <config-dict>
            }
        """
        uid = task_dict.get('uid')
        name = task_dict['name']
        config = task_dict.get('config')
        return cls(name, config=config, uid=uid)
