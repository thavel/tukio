from asynctest import TestCase, ignore_loop, patch
from nose.tools import assert_false, assert_equals, assert_true, assert_false
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
    def test_001_disable_children(self):
        self.workflow.disable_children('001002003', ['toto'])
        assert_false(self.workflow._children_active['001002003']['toto'])
        self.workflow.disable_children('001002003', ['toto', 'tata'])
        assert_false(self.workflow._children_active['001002003']['toto'])
        assert_false(self.workflow._children_active['001002003']['tata'])
        self.workflow.disable_children('001002003', ['toto'], enable_others=True)
        assert_false(self.workflow._children_active['001002003']['toto'], set(['tata']))
        assert_true(self.workflow._children_active['001002003'].get('tata', True))

    @ignore_loop
    def test_002_enable_children(self):
        self.workflow.enable_children('001002003', ['toto'])
        assert_true(self.workflow._children_active['001002003']['toto'])
        assert_true(self.workflow._children_active['001002003'][None])
        self.workflow.enable_children('001002003', ['tata'], disable_others=True)
        assert_true(self.workflow._children_active['001002003']['tata'])
        assert_true('toto' not in self.workflow._children_active['001002003'])
        assert_false(self.workflow._children_active['001002003'][None])

    @ignore_loop
    @patch.object(Workflow, "_new_task")
    def test_002a_start_deactivated(self, mock):
        future = FakeTask()
        future.result = lambda: 'ok'
        self.workflow.disable_children(future.uid, ['fake_2'])

        template = self.workflow._template
        self.workflow._run_next_tasks(template.root(), future)
        assert_equals(mock.call_count, 1)

    @ignore_loop
    @patch.object(Workflow, "_new_task")
    def test_002b_start_activated(self, mock):
        future = FakeTask()
        future.result = lambda: 'ok'
        self.workflow.enable_children(future.uid, ['fake_2'])

        template = self.workflow._template
        self.workflow._run_next_tasks(template.root(), future)
        assert_equals(mock.call_count, 2)

    @ignore_loop
    @patch.object(Workflow, "_new_task")
    def test_002c_start_activated(self, mock):
        future = FakeTask()
        future.result = lambda: 'ok'
        self.workflow.enable_children(future.uid, ['fake_2'], disable_others=True)

        template = self.workflow._template
        self.workflow._run_next_tasks(template.root(), future)
        assert_equals(mock.call_count, 1)
