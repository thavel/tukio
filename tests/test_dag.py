from unittest import TestCase

from tukio.dag import DAG, DAGValidationError


class TestDAG(TestCase):

    def get_valid_dag(self):
        return DAG.from_dict({
            't1': ['t2', 't3'],
            't2': ['t4'],
            't3': [],
            't4': ['t5'],
            't5': [],
            't6': ['t3']
        })

    def test_dag_from_dict(self):
        # Task not defined
        with self.assertRaises(KeyError):
            DAG.from_dict({'t1': ['t2']})

        # Cyclic graph
        with self.assertRaises(DAGValidationError):
            DAG.from_dict({'t1': ['t1']})

        # No list for successors
        with self.assertRaises(TypeError):
            DAG.from_dict({'t1': 't2'})

        dag = self.get_valid_dag()

        # Check leaves
        self.assertEqual(set(dag.leaves()), set(['t3', 't5']))
        # Check root nodes
        self.assertEqual(set(dag.root_nodes()), set(['t1', 't6']))
        # Check edges
        edges = dag.edges()
        expected = [
            ('t1', 't2'),
            ('t1', 't3'),
            ('t2', 't4'),
            ('t4', 't5'),
            ('t6', 't3')
        ]
        self.assertEqual(set(edges), set(expected))

    def test_copy(self):
        dag = self.get_valid_dag()
        copy = dag.copy()

        # Ensure it is a real copy of everything
        self.assertIsNot(copy, dag)
        self.assertEqual(set(copy.root_nodes()), set(dag.root_nodes()))
        self.assertEqual(set(copy.leaves()), set(dag.leaves()))
        self.assertEqual(copy.edges(), dag.edges())

    def test_add_node(self):
        dag = self.get_valid_dag()

        # Node already exist
        with self.assertRaises(ValueError):
            dag.add_node('t1')

        # Add node
        dag.add_node('t7')
        self.assertIn('t7', dag.leaves())

    def test_delete_node(self):
        dag = self.get_valid_dag()

        # Node does not exist
        with self.assertRaises(KeyError):
            dag.delete_node('unknown')

        # Delete node
        dag.delete_node('t1')
        self.assertNotIn('t1', dag.leaves())
        self.assertNotIn(('t1', 't2'), dag.edges())
        self.assertNotIn(('t1', 't3'), dag.edges())
