import asyncio
import logging
import sys
from uuid import uuid4
import weakref

from tukio.dag import DAG
from tukio.task import TaskDescription


logger = logging.getLogger(__name__)


class WorkflowError(Exception):
    pass


class WorkflowRootTaskError(WorkflowError):
    pass


class WorkflowDescription(object):
    """
    A workflow description is a DAG (Directed Acyclic Graph) made up of task
    description objects (`TaskDescription`). This class is not a workflow
    execution engine.
    It provides an API to easily build and update a consistent workflow as well
    as created runnable workflow instances.
    """
    def __init__(self, uid=None):
        # Unique execution ID of the workflow
        self._uid = uid or "-".join(['wf', str(uuid4())[:8]])
        self._dag = DAG()
        self._tasks = weakref.WeakValueDictionary()

    @property
    def uid(self):
        return self._uid

    @property
    def dag(self):
        return self._dag

    @property
    def tasks(self):
        return dict(self._tasks)

    def add(self, task_desc):
        """
        Adds a new task description to the workflow. The task will remain
        orphan until it is linked to upstream/downstream tasks.
        This method must be passed a `TaskDescription()` instance.
        """
        if not isinstance(task_desc, TaskDescription):
            raise TypeError("expected a 'TaskDescription' instance")
        self.dag.add_node(task_desc)
        self._tasks[task_desc.uid] = task_desc

    def delete(self, task_desc):
        """
        Remove a task description from the workflow and delete the links to
        upstream/downstream tasks.
        """
        self.dag.delete_node(task_desc)

    def get(self, task_id):
        """
        Returns the task object that has the searched task ID. Returns 'None'
        if the task ID was not found.
        """
        return self._tasks.get(task_id, None)

    def root(self):
        """
        Returns the root task. If no root task or several root tasks were found
        raises `WorkflowValidationError`.
        """
        root_task = self.dag.root_nodes()
        if len(root_task) == 1:
            return root_task[0]
        else:
            raise WorkflowRootTaskError("expected one root task, "
                                        "found {}".format(root_task))

    def link(self, up_task_desc, down_task_desc):
        """
        Create a directed link from an upstream to a downstream task.
        """
        self.dag.add_edge(up_task_desc, down_task_desc)

    def unlink(self, task_desc1, task_desc2):
        """
        Remove the link between two tasks.
        """
        try:
            self.dag.delete_edge(task_desc1, task_desc2)
        except KeyError:
            self.dag.delete_edge(task_desc2, task_desc1)

    @classmethod
    def from_dict(cls, wf_dict):
        """
        Build a new workflow description from the given dictionary.
        The dictionary takes the form of:
            {
                "uid": <workflow-uid>
                "tasks": [
                    {"uid": <task-uid>, "name": <name>, "config": <cfg-dict>},
                    ...
                ],
                "graph": {
                    <t1-uid>: [t2-uid, <t3-uid>],
                    <t2-uid>: [],
                    ...
                }
            }
        """
        uid = wf_dict['uid']
        wf_desc = cls(uid)
        for task_dict in wf_dict['tasks']:
            task_desc = TaskDescription.from_dict(task_dict)
            wf_desc.add(task_desc)
        for up_id, down_ids_set in wf_dict['graph'].items():
            up_desc = wf_desc.get(up_id)
            for down_id in down_ids_set:
                down_desc = wf_desc.get(down_id)
                wf_desc.link(up_desc, down_desc)
        return wf_desc

    def as_dict(self):
        """
        Build a dictionary that represents the current workflow description
        object.
        """
        wf_dict = {"uid": self.uid, "tasks": [], "graph": {}}
        for task_id, task_desc in self.tasks.items():
            task_dict = {"uid": task_id, "name": task_desc.name}
            if task_desc.config:
                task_dict['config'] = task_desc.config
            wf_dict['tasks'].append(task_dict)
        for up_desc, down_descs_set in self.dag.graph.items():
            _record = {up_desc.uid: list(map(lambda x: x.uid, down_descs_set))}
            wf_dict['graph'].update(_record)
        return wf_dict


"""
wfd.add()
wfd.delete()
wfd.get()
wfd.link()
wfd.unlink()
wfd.root()
wfd.validate()
wfd.from_dict()
wfd.as_dict()
"""

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)])
    loop = asyncio.get_event_loop()

    wf_dict = {
        "uid": "my-workflow",
        "tasks": [
            {"uid": "f1", "name": "fake"},
            {"uid": "f2", "name": "fake"},
            {"uid": "f3", "name": "fake"},
            {"uid": "f4", "name": "fake"},
            {"uid": "f5", "name": "fake"},
            {"uid": "f6", "name": "fake"}
        ],
        "graph": {
            "f1": ["f2"],
            "f2": ["f3", "f4"],
            "f3": ["f5"],
            "f4": ["f5"],
            "f5": ["f6"],
            "f6": []
        }
    }
    wf_desc = WorkflowDescription.from_dict(wf_dict)
    print("workflow uid: {}".format(wf_desc.uid))
    print("workflow graph: {}".format(wf_desc.dag.graph))
    print("workflow tasks: {}".format(wf_desc.tasks))
