"""
test_strategy_dag.py — Tests for StrategyDAG
TARE (Tick-Level Algorithmic Research Environment)

Comprehensive test suite for DAG-based strategy specification.
"""

import unittest
from tare.strategy.strategy_dag import StrategyDAG


class TestStrategyDAGBasics(unittest.TestCase):
    """Test basic node and edge operations."""
    
    def setUp(self):
        """Initialize fresh DAG for each test."""
        self.dag = StrategyDAG()
    
    def test_init_empty(self):
        """DAG initializes empty with no nodes."""
        self.assertTrue(self.dag.is_empty())
        self.assertEqual(self.dag.count_nodes(), 0)
        self.assertEqual(self.dag.count_edges(), 0)
    
    def test_add_single_node(self):
        """Add single node to DAG."""
        self.dag.add_node('alpha_signal', 'alpha', lambda x: 100)
        self.assertEqual(self.dag.count_nodes(), 1)
        self.assertFalse(self.dag.is_empty())
    
    def test_add_multiple_nodes(self):
        """Add multiple nodes with different types."""
        self.dag.add_node('signal1', 'alpha', lambda x: 50)
        self.dag.add_node('signal2', 'alpha', lambda x: 75)
        self.dag.add_node('risk', 'risk', lambda x: 0)
        self.dag.add_node('exec', 'execution', lambda x: {})
        
        self.assertEqual(self.dag.count_nodes(), 4)
        nodes = self.dag.get_nodes()
        self.assertIn('signal1', nodes)
        self.assertIn('signal2', nodes)
        self.assertIn('risk', nodes)
        self.assertIn('exec', nodes)
    
    def test_add_node_duplicate_name(self):
        """Adding node with duplicate name raises ValueError."""
        self.dag.add_node('signal', 'alpha', lambda x: 100)
        with self.assertRaises(ValueError):
            self.dag.add_node('signal', 'alpha', lambda x: 200)
    
    def test_add_node_empty_name(self):
        """Adding node with empty name raises ValueError."""
        with self.assertRaises(ValueError):
            self.dag.add_node('', 'alpha', lambda x: 100)
    
    def test_add_node_invalid_func(self):
        """Adding node with non-callable func raises TypeError."""
        with self.assertRaises(TypeError):
            self.dag.add_node('signal', 'alpha', "not callable")
    
    def test_add_edge_basic(self):
        """Add single edge between nodes."""
        self.dag.add_node('signal', 'alpha', lambda x: 100)
        self.dag.add_node('risk', 'risk', lambda x: 0)
        self.dag.add_edge('signal', 'risk')
        
        self.assertEqual(self.dag.count_edges(), 1)
    
    def test_add_edge_nonexistent_from_node(self):
        """Adding edge from non-existent node raises ValueError."""
        self.dag.add_node('risk', 'risk', lambda x: 0)
        with self.assertRaises(ValueError):
            self.dag.add_edge('signal', 'risk')
    
    def test_add_edge_nonexistent_to_node(self):
        """Adding edge to non-existent node raises ValueError."""
        self.dag.add_node('signal', 'alpha', lambda x: 100)
        with self.assertRaises(ValueError):
            self.dag.add_edge('signal', 'risk')
    
    def test_add_edge_self_loop(self):
        """Adding self-loop raises ValueError."""
        self.dag.add_node('signal', 'alpha', lambda x: 100)
        with self.assertRaises(ValueError):
            self.dag.add_edge('signal', 'signal')


class TestStrategyDAGExecution(unittest.TestCase):
    """Test DAG execution and computation."""
    
    def test_execute_single_node(self):
        """Execute DAG with single node."""
        dag = StrategyDAG()
        dag.add_node('output', 'alpha', lambda x: 42)
        
        result = dag.execute({})
        
        self.assertEqual(result['output'], 42)
    
    def test_execute_linear_chain(self):
        """Execute linear chain of nodes."""
        dag = StrategyDAG()
        dag.add_node('input', 'alpha', lambda x: x.get('value', 10))
        dag.add_node('double', 'alpha', lambda x: x * 2)
        dag.add_node('add_ten', 'alpha', lambda x: x + 10)
        
        dag.add_edge('input', 'double')
        dag.add_edge('double', 'add_ten')
        
        result = dag.execute({'input': {'value': 5}})
        
        self.assertEqual(result['input'], 5)
        self.assertEqual(result['double'], 10)
        self.assertEqual(result['add_ten'], 20)
    
    def test_execute_deterministic(self):
        """Execution produces deterministic results."""
        dag1 = StrategyDAG()
        dag1.add_node('n1', 'alpha', lambda x: 100)
        dag1.add_node('n2', 'alpha', lambda x: 200)
        dag1.add_edge('n1', 'n2')
        
        dag2 = StrategyDAG()
        dag2.add_node('n1', 'alpha', lambda x: 100)
        dag2.add_node('n2', 'alpha', lambda x: 200)
        dag2.add_edge('n1', 'n2')
        
        result1 = dag1.execute({})
        result2 = dag2.execute({})
        
        self.assertEqual(result1, result2)


class TestStrategyDAGValidation(unittest.TestCase):
    """Test DAG validation and cycle detection."""
    
    def test_no_cycle_linear(self):
        """Linear DAG has no cycles."""
        dag = StrategyDAG()
        dag.add_node('a', 'alpha', lambda x: 1)
        dag.add_node('b', 'alpha', lambda x: 2)
        dag.add_node('c', 'alpha', lambda x: 3)
        dag.add_edge('a', 'b')
        dag.add_edge('b', 'c')
        
        self.assertTrue(dag.is_valid())
    
    def test_cycle_detection(self):
        """DAG with cycle is detected as invalid."""
        dag = StrategyDAG()
        dag.add_node('a', 'alpha', lambda x: 1)
        dag.add_node('b', 'alpha', lambda x: 2)
        dag.add_edge('a', 'b')
        dag.add_edge('b', 'a')  # Creates cycle
        
        self.assertFalse(dag.is_valid())


if __name__ == '__main__':
    unittest.main()
