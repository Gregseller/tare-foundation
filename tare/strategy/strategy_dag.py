"""
strategy_dag.py — StrategyDAG v1
TARE (Tick-Level Algorithmic Research Environment)

Define strategy as DAG of alpha/risk/execution nodes.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Callable, Dict, List, Any, Optional
from collections import defaultdict, deque


class StrategyDAG:
    """
    Define strategy as directed acyclic graph (DAG) of computation nodes.
    
    Supports alpha signal generation, risk assessment, and execution planning
    as interconnected nodes with data flow between them. All operations are
    deterministic and use integer arithmetic only.
    """
    
    def __init__(self) -> None:
        """
        Initialize empty StrategyDAG with no nodes or edges.
        
        Creates internal structures to track nodes, edges, and execution state.
        All data structures use deterministic initialization.
        """
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._edges: Dict[str, List[str]] = defaultdict(list)
        self._node_order: List[str] = []
        self._execution_results: Dict[str, Any] = {}
        self._is_valid: bool = False
    
    def add_node(
        self,
        name: str,
        node_type: str,
        func: Callable
    ) -> None:
        """
        Add computation node to DAG.
        
        Nodes represent alpha signals, risk calculations, or execution decisions.
        Each node has a function that processes inputs deterministically.
        
        Args:
            name: Unique node identifier (string, non-empty).
            node_type: Node category - 'alpha', 'risk', 'execution', or custom type.
            func: Callable that processes node inputs and returns output.
                  Must be deterministic and use only integer arithmetic.
        
        Raises:
            ValueError: If name is empty, already exists, or node_type is invalid.
            TypeError: If func is not callable.
        
        Example:
            >>> dag = StrategyDAG()
            >>> dag.add_node('signal_1', 'alpha', lambda x: x * 2)
            >>> dag.add_node('risk_check', 'risk', lambda x: x > 1000)
        """
        if not isinstance(name, str) or not name:
            raise ValueError("name must be non-empty string")
        
        if name in self._nodes:
            raise ValueError(f"node '{name}' already exists")
        
        if not isinstance(node_type, str) or not node_type:
            raise ValueError("node_type must be non-empty string")
        
        valid_types = {'alpha', 'risk', 'execution'}
        if node_type not in valid_types and not isinstance(node_type, str):
            raise ValueError(f"node_type must be one of {valid_types} or custom string")
        
        if not callable(func):
            raise TypeError("func must be callable")
        
        self._nodes[name] = {
            'type': node_type,
            'func': func,
            'inputs': [],
            'output': None
        }
        
        self._is_valid = False
    
    def add_edge(self, from_node: str, to_node: str) -> None:
        """
        Add directed edge between nodes.
        
        Creates data flow from source node's output to target node's input.
        Edges must form acyclic graph (no cycles allowed). Cycle detection
        happens during DAG validation.
        
        Args:
            from_node: Source node name (must exist).
            to_node: Target node name (must exist).
        
        Raises:
            ValueError: If either node does not exist or edge would create cycle.
        
        Example:
            >>> dag.add_edge('signal_1', 'risk_check')
            >>> dag.add_edge('risk_check', 'execution_plan')
        """
        if from_node not in self._nodes:
            raise ValueError(f"from_node '{from_node}' does not exist")
        
        if to_node not in self._nodes:
            raise ValueError(f"to_node '{to_node}' does not exist")
        
        if from_node == to_node:
            raise ValueError("self-loops not allowed")
        
        # Check if edge already exists
        if to_node in self._edges[from_node]:
            return
        
        self._edges[from_node].append(to_node)
        self._nodes[to_node]['inputs'].append(from_node)
        
        self._is_valid = False
    
    def _has_cycle(self) -> bool:
        """
        Detect cycles in DAG using depth-first search.
        
        Returns False if graph is acyclic, True if any cycle found.
        Uses deterministic DFS coloring: white (0), gray (1), black (2).
        
        Returns:
            bool: True if cycle detected, False if acyclic.
        """
        colors = {node: 0 for node in self._nodes}
        
        def visit(node: str) -> bool:
            """Visit node and detect back edges."""
            colors[node] = 1
            
            for neighbor in self._edges[node]:
                if colors[neighbor] == 1:
                    return True
                if colors[neighbor] == 0 and visit(neighbor):
                    return True
            
            colors[node] = 2
            return False
        
        for node in self._nodes:
            if colors[node] == 0:
                if visit(node):
                    return True
        
        return False
    
    def _topological_sort(self) -> List[str]:
        """
        Compute topological sort of DAG nodes.
        
        Returns execution order for deterministic processing. Uses
        Kahn's algorithm with in-degree counting for stability.
        
        Returns:
            list: Node names in topological order (sources first).
            
        Raises:
            ValueError: If graph contains cycles.
        """
        if self._has_cycle():
            raise ValueError("DAG contains cycle")
        
        # Count in-degrees
        in_degree = {node: 0 for node in self._nodes}
        for node in self._nodes:
            for neighbor in self._edges[node]:
                in_degree[neighbor] += 1
        
        # Find all sources
        queue = deque()
        for node in self._nodes:
            if in_degree[node] == 0:
                queue.append(node)
        
        # Process in order
        result = []
        in_degree_copy = in_degree.copy()
        
        while queue:
            # Dequeue deterministically (consume in order added)
            node = queue.popleft()
            result.append(node)
            
            for neighbor in self._edges[node]:
                in_degree_copy[neighbor] -= 1
                if in_degree_copy[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(result) != len(self._nodes):
            raise ValueError("DAG contains cycle")
        
        return result
    
    def _validate_dag(self) -> None:
        """
        Validate DAG structure and compute execution order.
        
        Checks for cycles and computes topological sort. Sets _is_valid
        flag to True if successful, raises exception otherwise.
        
        Raises:
            ValueError: If DAG contains cycles or is otherwise invalid.
        """
        if self._has_cycle():
            raise ValueError("DAG contains cycle")
        
        self._node_order = self._topological_sort()
        self._is_valid = True
    
    def execute(self, inputs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute DAG in topological order.
        
        Processes all nodes sequentially according to data dependencies.
        Input nodes (those with no predecessors) can accept initial values.
        Outputs of executed nodes become inputs to dependent nodes.
        All computation is deterministic.
        
        Args:
            inputs: Optional dict mapping input node names to initial values.
                   Nodes without entries use empty dict as input.
        
        Returns:
            dict: Mapping of node name to execution output. Contains results
                  from all nodes in DAG. Structure determined by node functions.
        
        Raises:
            ValueError: If DAG is invalid or execution fails.
            TypeError: If node function returns unexpected type.
        
        Example:
            >>> dag = StrategyDAG()
            >>> dag.add_node('input', 'alpha', lambda x: x['value'] * 2)
            >>> dag.add_node('output', 'risk', lambda x: x + 100)
            >>> dag.add_edge('input', 'output')
            >>> result = dag.execute({'input': {'value': 50}})
            >>> result['output']  # 100 + 100 = 200
            200
        """
        # Validate DAG
        if not self._is_valid:
            self._validate_dag()
        
        # Initialize inputs
        if inputs is None:
            inputs = {}
        
        if not isinstance(inputs, dict):
            raise TypeError("inputs must be dict or None")
        
        # Clear previous results
        self._execution_results = {}
        
        # Execute nodes in topological order
        for node_name in self._node_order:
            node_def = self._nodes[node_name]
            func = node_def['func']
            predecessors = node_def['inputs']
            
            # Gather inputs from predecessors or initial inputs
            if predecessors:
                # Node has predecessors: collect their outputs
                # Один предшественник → передаём значение напрямую
                # Несколько → передаём dict {pred_name: value}
                if len(predecessors) == 1:
                    node_input = self._execution_results[predecessors[0]]
                else:
                    node_input = {}
                    for pred in predecessors:
                        node_input[pred] = self._execution_results[pred]
            else:
                # Source node: use initial inputs if provided
                if node_name in inputs:
                    node_input = inputs[node_name]
                else:
                    node_input = {}
            
            # Execute node function deterministically
            try:
                node_output = func(node_input)
            except Exception as e:
                raise ValueError(
                    f"Node '{node_name}' execution failed: {str(e)}"
                )
            
            # Store result
            self._execution_results[node_name] = node_output
        
        return self._execution_results.copy()
    
    def get_node(self, name: str) -> Dict[str, Any]:
        """
        Retrieve node definition.
        
        Returns copy of node metadata including type, function, and connections.
        
        Args:
            name: Node name.
        
        Returns:
            dict: Node definition (type, func, inputs list, cached output).
                  Returns empty dict if node not found.
        """
        if name not in self._nodes:
            return {}
        
        node = self._nodes[name]
        return {
            'type': node['type'],
            'inputs': node['inputs'].copy(),
            'has_output': node['output'] is not None
        }
    
    def get_nodes(self) -> List[str]:
        """
        List all node names in DAG.
        
        Returns:
            list: Sorted list of node identifiers (deterministic order).
        """
        return sorted(self._nodes.keys())
    
    def get_edges(self) -> List[tuple]:
        """
        List all edges in DAG as (from_node, to_node) tuples.
        
        Returns edges in deterministic order for reproducibility.
        
        Returns:
            list: List of (source, target) node name tuples.
        """
        edges = []
        for from_node in sorted(self._edges.keys()):
            for to_node in sorted(self._edges[from_node]):
                edges.append((from_node, to_node))
        return edges
    
    def get_execution_order(self) -> List[str]:
        """
        Get topological execution order of nodes.
        
        Returns nodes sorted by dependencies for correct execution sequence.
        Must call execute() or validate DAG first.
        
        Returns:
            list: Node names in execution order.
            
        Raises:
            ValueError: If DAG is invalid (contains cycles).
        """
        if not self._is_valid:
            self._validate_dag()
        
        return self._node_order.copy()
    
    def get_result(self, node_name: str) -> Any:
        """
        Retrieve result from most recent execution.
        
        Returns cached output from node after execute() was called.
        
        Args:
            node_name: Node name.
        
        Returns:
            Any: Output from node's last execution, or None if not executed.
        """
        if node_name not in self._execution_results:
            return None
        
        return self._execution_results[node_name]
    
    def count_nodes(self) -> int:
        """
        Count total nodes in DAG.
        
        Returns:
            int: Number of nodes added.
        """
        return len(self._nodes)
    
    def count_edges(self) -> int:
        """
        Count total edges in DAG.
        
        Returns:
            int: Number of directed edges.
        """
        total = 0
        for neighbors in self._edges.values():
            total += len(neighbors)
        return total
    
    def is_empty(self) -> bool:
        """
        Check if DAG has any nodes.
        
        Returns:
            bool: True if no nodes, False otherwise.
        """
        return len(self._nodes) == 0
    
    def is_valid(self) -> bool:
        """
        Check if DAG structure is valid (acyclic).
        
        Returns:
            bool: True if DAG is acyclic and ready for execution.
        """
        try:
            if not self._is_valid:
                self._validate_dag()
            return True
        except ValueError:
            return False
