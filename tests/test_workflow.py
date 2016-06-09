import asyncio
import unittest
from tukio.workflow import (
    WorkflowTemplate, new_workflow, WorkflowNotFoundError,
    WorkflowInterface
)
from tukio.task import register, TaskHolder, tukio_factory, new_task


@register("fakeroot", "execute")
class FakeRoot(TaskHolder):

    async def execute(self, data):
        self.test_util()
        return data

    def test_util(self):
        pass


@register("fakenext", "execute")
class FakeNext(TaskHolder):

    async def execute(self, data):
        self.test_util()
        return data

    def test_util(self):
        pass


TEMPLATE = {
    "tasks": [
        {"id": "fake1", "name": "fakeroot"},
        {"id": "fake2", "name": "fakenext"},
        {"id": "fake3", "name": "fakenext"},
        {"id": "fake4", "name": "fakenext"}
    ],
    "graph": {"fake1": ["fake2", "fake3", "fake4"]}
}


class TestWorkflowInterface(unittest.TestCase):

    def setUp(self):
        self.template = WorkflowTemplate.from_dict(TEMPLATE)
        self.loop = asyncio.get_event_loop()
        self.loop.set_task_factory(tukio_factory)
        # template = WorkflowTemplate.from_dict(TEMPLATE)
        # self.workflow = new_workflow(template)

    def tearDown(self):
        tasks = asyncio.Task.all_tasks(loop=self.loop)
        if tasks:
            self.loop.run_until_complete(asyncio.wait(tasks))

    def test_get_workflow_interface(self):
        """
        A task (asyncio.Task) should be able to get a `WorkflowTemplate`
        instance to interact with the workflow since it was triggered by
        a workflow and not as a standalone task.
        """
        # Pass the current task object to the constructor
        workflow = new_workflow(self.template)
        root = workflow.run('dummy')
        wf_interface = WorkflowInterface(root)
        self.assertIsInstance(wf_interface, WorkflowInterface)

        # Works also with the classmethod `from_current_task()` without arg.
        def test_from_current_task(_):
            wf_interface = WorkflowInterface.from_current_task()
            self.assertIsInstance(wf_interface, WorkflowInterface)

        FakeRoot.test_util = test_from_current_task
        self.loop.run_until_complete(root)

    def test_cannot_get_workflow_interface(self):
        """
        A task created outside of a workflow won't be able to get an instance
        of `WorkflowInterface`. It should raise a `WorkflowNotFoundError`.
        """
        # Cannot get an interface when the task was not triggered by a workflow
        task = new_task('fakeroot', 'dummy', loop=self.loop)
        with self.assertRaises(WorkflowNotFoundError):
            WorkflowInterface(task)

        # Cannot either use the classmethod when not executed by the event loop
        with self.assertRaisesRegex(AttributeError, '_callbacks'):
            WorkflowInterface.from_current_task()

    def test_set_valid_next_tasks(self):
        """
        A task can select (at runtime) a subset of its downstreams tasks to be
        executed by the workflow. The subset can be all tasks to no task.
        """
        # Single next task
        workflow = new_workflow(self.template)
        root = workflow.run('dummy')

        def single_next_task(_):
            wf_interface = WorkflowInterface.from_current_task()
            wf_interface.set_next_tasks(['fake2'])

        FakeRoot.test_util = single_next_task
        self.loop.run_until_complete(workflow)
        self.assertEqual(len(workflow._tasks_by_id), 2)
        self.assertIn('fake1', workflow._tasks_by_id)
        self.assertIn('fake2', workflow._tasks_by_id)

        # Two next tasks
        workflow = new_workflow(self.template)
        root = workflow.run('dummy')

        def two_next_tasks(_):
            wf_interface = WorkflowInterface.from_current_task()
            wf_interface.set_next_tasks(['fake2', 'fake4'])

        FakeRoot.test_util = two_next_tasks
        self.loop.run_until_complete(workflow)
        self.assertEqual(len(workflow._tasks_by_id), 3)
        self.assertIn('fake1', workflow._tasks_by_id)
        self.assertIn('fake2', workflow._tasks_by_id)
        self.assertIn('fake4', workflow._tasks_by_id)

        # No next task at all
        workflow = new_workflow(self.template)
        root = workflow.run('dummy')

        def no_next_task(_):
            wf_interface = WorkflowInterface.from_current_task()
            wf_interface.set_next_tasks([])

        FakeRoot.test_util = no_next_task
        self.loop.run_until_complete(workflow)
        self.assertEqual(len(workflow._tasks_by_id), 1)
        self.assertIn('fake1', workflow._tasks_by_id)

    def test_set_invalid_next_tasks(self):
        """
        A task cannot set arbitrary task template IDs as its downstream tasks.
        The workflow graph shall remain unmodified at all times!
        """
        workflow = new_workflow(self.template)
        root = workflow.run('dummy')

        def bad_next_task(_):
            wf_interface = WorkflowInterface.from_current_task()
            wf_interface.set_next_tasks(['foo'])

        FakeRoot.test_util = bad_next_task
        with self.assertLogs(level='ERROR') as logs:
            self.loop.run_until_complete(workflow)
        self.assertIn('not in downstream tasks', logs.output[0])
        self.assertEqual(len(workflow._tasks_by_id), 1)
        self.assertIn('fake1', workflow._tasks_by_id)


if __name__ == '__main__':
    unittest.main()
