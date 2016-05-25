from uuid import uuid4

from .task import new_task
from tukio.utils import future_state


class TaskTemplate:

    """
    The complete description of a Tukio task is made of its registered name and
    its configuration (a dict).
    A task template can be linked to an execution object (inherited from
    `asyncio.Task`) and provide execution report.
    """

    def __init__(self, name, uid=None, config=None, topics=[]):
        self.name = name
        self.config = config
        self.topics = topics
        self.uid = uid or str(uuid4())
        # store an execution object (instance of `asyncio.Task`)
        self._task = None

    @property
    def task(self):
        return self._task

    @task.setter
    def task(self, task):
        if self._task:
            raise AttributeError("attribute 'task' already set")
        self._task = task

    def new_task(self, *args, loop=None, **kwargs):
        """
        Create a new task from the current task template.
        """
        inputs = (args, kwargs)
        task = new_task(self.name, inputs=inputs,
                        config=self.config, loop=loop)
        self.task = task
        return task

    @classmethod
    def from_dict(cls, task_dict):
        """
        Create a new task description object from the given dictionary.
        The dictionary takes the form of:
            {
                "id": <task-template-id>,
                "name": <registered-task-name>,
                "config": <config-dict>,
                "topics": {[<>]|null}
            }

        The parameters 'topics' and 'config' are both optional.
        See below the behavior of a task at runtime according to the value of
        'topics':
            {"topics": None}
            the task will receive ALL data disptached by the broker

            {"topics": []}
            the task will receive NO data from the broker

            {"topics": ["blob", "foo"]}
            the task will receive data dispatched by the broker in topics
            "blob" and "foo" only
        """
        uid = task_dict.get('id')
        name = task_dict['name']
        config = task_dict.get('config')
        topics = task_dict.get('topics', [])
        return cls(name, uid=uid, config=config, topics=topics)

    def as_dict(self):
        """
        Builds a dictionary that represents the task template object. If the
        task template is linked to a task execution object, the dictionary
        contains the execution (stored at key 'exec').
        """
        task_dict = {"name": self.name, "id": self.uid}
        task_dict.update({"config": self.config, "topics": self.topics})
        exec_dict = self._report()
        if exec_dict:
            task_dict['exec'] = exec_dict
        return task_dict

    def __str__(self):
        return "<TaskTemplate name={}, uid={}>".format(self.name, self.uid)

    def _report(self):
        """
        Creates and returns an execution report provided that the task template
        was previously linked to an execution object.
        An execution report is a dict that usually takes the form of:
            {
                "id": <execution-id>,
                "state": <execution-state>
                "info": <execution-details-as-a-dict>
            }
        """
        if not self.task:
            return dict()
        # If the task is linked to a task holder, try to use its own report.
        try:
            return self.task.holder.report()
        except AttributeError:
            if hasattr(self.task, 'uid'):
                uid = self.task.uid
            else:
                uid = None
            state = future_state(self.task).value
            return {"id": uid, "state": state}
