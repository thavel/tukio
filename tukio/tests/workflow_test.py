from asynctest import TestCase, ignore_loop, patch
from nose.tools import assert_false, assert_equals
from tukio.workflow import WorkflowTemplate, new_workflow, Workflow
from tukio.task import register, TaskHolder


@register("fake", "execute")
class FakeTask(TaskHolder):

    async def execute(self, data):
        return data


TEMPLATE = {
    "tasks": [
        {"topics": None, "id": "fake", "name": "fake",
         "config": {}},
        {"topics": None, "id": "fake_2", "name": "fake",
         "config": {}},
        {"topics": None, "id": "fake_3", "name": "fake",
         "config": {}}
    ],
    "topics": None, "title": "test workflow", "version": 1,
    "tags": ["test"],
    "graph": {"fake": ["fake_2", "fake_3"]}
}


class WorkflowTest(TestCase):

    def setUp(self):
        template = WorkflowTemplate.from_dict(TEMPLATE)
        self.workflow = new_workflow(template)

    @ignore_loop
    def test_001_disinherit(self):
        assert_false(self.workflow._tasks_disinherited)
        self.workflow.disinherit('001002003', ['toto'])
        assert_equals(self.workflow._tasks_disinherited['001002003'], set(['toto']))
        self.workflow.disinherit('001002003', ['toto', 'tata'])
        assert_equals(self.workflow._tasks_disinherited['001002003'], set(['toto', 'tata']))

    @ignore_loop
    @patch.object(Workflow, "_new_task")
    def test_002_start_dishinerited(self, mock):
        future = FakeTask()
        future.result = lambda :'ok'
        self.workflow._tasks_disinherited[future.uid] = ['fake_2']
        template = self.workflow._template
        self.workflow._run_next_tasks(template.root(), future)
        assert_equals(mock.call_count, 1)