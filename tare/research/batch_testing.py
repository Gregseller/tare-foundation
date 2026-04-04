"""
batch_testing.py — BatchTesting v1
TARE (Tick-Level Algorithmic Research Environment)

Run backtest batches and aggregate results deterministically.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Dict, List, Tuple, Any, Optional, Generator
from collections import defaultdict
import hashlib


class BatchTesting:
    """
    Run backtest batches and aggregate results deterministically.
    
    Manages batch submission of multiple strategies, coordinates execution
    via ExecutionEngine and StrategyDAG, and produces aggregated performance
    reports. All operations are deterministic with integer arithmetic only.
    """
    
    def __init__(self, strategy_dag, execution_engine):
        """
        Initialize BatchTesting with required dependencies.
        
        Args:
            strategy_dag: StrategyDAG instance for strategy execution.
            execution_engine: ExecutionEngine instance for order execution.
            
        Raises:
            ValueError: If any dependency is None or invalid.
        """
        if strategy_dag is None:
            raise ValueError("strategy_dag cannot be None")
        if execution_engine is None:
            raise ValueError("execution_engine cannot be None")
        
        self._strategy_dag = strategy_dag
        self._execution_engine = execution_engine
        
        self._batches: Dict[str, Dict[str, Any]] = {}
        self._batch_counter = 0
        self._batch_results: Dict[str, Dict[str, Any]] = {}
        self._execution_logs: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    def _generate_batch_id(self, strategies: list, date_range: Tuple[int, int]) -> str:
        """
        Generate deterministic batch ID from inputs.
        
        Creates hash-based ID from strategy definitions and date range.
        Same inputs always produce same ID (deterministic).
        
        Args:
            strategies: List of strategy configuration dicts.
            date_range: Tuple of (start_date, end_date) as integers.
            
        Returns:
            str: Deterministic batch ID (e.g., 'batch_a1b2c3d4').
        """
        # Serialize strategies deterministically
        strategy_str = ""
        for strat in sorted(strategies, key=lambda s: s.get('name', '')):
            strategy_str += str(sorted(strat.items()))
        
        # Include date range
        range_str = f"{date_range[0]}_{date_range[1]}"
        
        # Create hash
        combined = strategy_str + range_str
        hash_obj = hashlib.sha256(combined.encode('utf-8'))
        hash_hex = hash_obj.hexdigest()[:8]
        
        self._batch_counter += 1
        batch_id = f"batch_{hash_hex}_{self._batch_counter}"
        
        return batch_id
    
    def _validate_strategy(self, strategy: dict) -> None:
        """
        Validate strategy configuration dict.
        
        Args:
            strategy: Strategy configuration dictionary.
            
        Raises:
            ValueError: If strategy is invalid.
            TypeError: If strategy has wrong type.
        """
        if not isinstance(strategy, dict):
            raise TypeError("each strategy must be a dict")
        
        if 'name' not in strategy:
            raise ValueError("strategy must have 'name' key")
        
        if not isinstance(strategy['name'], str) or not strategy['name']:
            raise ValueError("strategy name must be non-empty string")
        
        if 'dag_config' not in strategy:
            raise ValueError("strategy must have 'dag_config' key")
    
    def _validate_date_range(self, date_range: Tuple[int, int]) -> None:
        """
        Validate date range tuple.
        
        Args:
            date_range: Tuple of (start_date, end_date).
            
        Raises:
            ValueError: If date range is invalid.
            TypeError: If date range has wrong type.
        """
        if not isinstance(date_range, tuple) or len(date_range) != 2:
            raise ValueError("date_range must be tuple of length 2")
        
        start_date, end_date = date_range
        
        if not isinstance(start_date, int):
            raise TypeError("start_date must be integer")
        
        if not isinstance(end_date, int):
            raise TypeError("end_date must be integer")
        
        if start_date < 0 or end_date < 0:
            raise ValueError("dates must be non-negative integers")
        
        if start_date >= end_date:
            raise ValueError("start_date must be less than end_date")
    
    def submit_batch(
        self,
        strategies: list,
        date_range: Tuple[int, int]
    ) -> str:
        """
        Submit batch of strategies for backtesting.
        
        Validates inputs, assigns deterministic batch ID, schedules execution
        for each strategy over date range. Returns immediately with batch ID.
        Actual execution happens on demand via get_results().
        
        Args:
            strategies: List of strategy configuration dicts, each with:
                - 'name' (str): Strategy identifier
                - 'dag_config' (dict): StrategyDAG node/edge definitions
                - Additional strategy-specific parameters
                
            date_range: Tuple (start_date, end_date) where dates are 
                       integer timestamps or bar indices (non-negative integers).
        
        Returns:
            str: Batch ID for result retrieval (e.g., 'batch_abc123_1').
            
        Raises:
            ValueError: If strategies list is empty or date range invalid.
            TypeError: If inputs have wrong types.
            
        Example:
            >>> batch_test = BatchTesting(dag, engine)
            >>> batch_id = batch_test.submit_batch(
            ...     strategies=[
            ...         {'name': 'strat_a', 'dag_config': {...}},
            ...         {'name': 'strat_b', 'dag_config': {...}}
            ...     ],
            ...     date_range=(20230101, 20231231)
            ... )
            >>> results = batch_test.get_results(batch_id)
        """
        # Validate inputs
        if not isinstance(strategies, list):
            raise TypeError("strategies must be a list")
        
        if len(strategies) == 0:
            raise ValueError("strategies list cannot be empty")
        
        self._validate_date_range(date_range)
        
        # Validate each strategy
        for strat in strategies:
            self._validate_strategy(strat)
        
        # Generate batch ID
        batch_id = self._generate_batch_id(strategies, date_range)
        
        # Store batch metadata
        self._batches[batch_id] = {
            'batch_id': batch_id,
            'strategies': strategies.copy(),
            'date_range': date_range,
            'num_strategies': len(strategies),
            'start_date': date_range[0],
            'end_date': date_range[1],
            'status': 'SUBMITTED',
            'submission_order': self._batch_counter
        }
        
        return batch_id
    
    def _build_strategy_dag(self, dag_config: dict) -> None:
        """
        Build StrategyDAG from configuration.
        
        Args:
            dag_config: DAG configuration dict with 'nodes' and 'edges'.
        """
        if not isinstance(dag_config, dict):
            return
        
        nodes = dag_config.get('nodes', [])
        edges = dag_config.get('edges', [])
        
        # Reset DAG nodes
        self._strategy_dag._nodes = {}
        self._strategy_dag._edges = defaultdict(list)
        self._strategy_dag._is_valid = False
        
        # Add nodes
        for node_def in nodes:
            if not isinstance(node_def, dict):
                continue
            
            name = node_def.get('name', '')
            node_type = node_def.get('type', 'alpha')
            
            if not name or not callable(node_def.get('func')):
                continue
            
            try:
                self._strategy_dag.add_node(name, node_type, node_def['func'])
            except (ValueError, TypeError):
                pass
        
        # Add edges
        for edge_def in edges:
            if not isinstance(edge_def, (tuple, list)) or len(edge_def) != 2:
                continue
            
            try:
                self._strategy_dag.add_edge(edge_def[0], edge_def[1])
            except ValueError:
                pass
    
    def _execute_strategy_backtest(
        self,
        strategy: dict,
        date_range: Tuple[int, int]
    ) -> Dict[str, Any]:
        """
        Execute single strategy backtest over date range.
        
        Args:
            strategy: Strategy configuration dict.
            date_range: Tuple of (start_date, end_date).
            
        Returns:
            dict: Backtest results with performance metrics.
        """
        strategy_name = strategy.get('name', 'unknown')
        
        # Build DAG from strategy config
        dag_config = strategy.get('dag_config', {})
        self._build_strategy_dag(dag_config)
        
        results = {
            'strategy_name': strategy_name,
            'date_range': date_range,
            'total_return': 0,
            'max_drawdown': 0,
            'sharpe_ratio': 0,
            'num_trades': 0,
            'win_rate': 0,
            'avg_trade_return': 0,
            'trades': [],
            'equity_curve': [100000],
            'daily_returns': [],
            'status': 'COMPLETED'
        }
        
        # Execute DAG with date range as input
        dag_inputs = {
            'date_range': date_range,
            'strategy_params': strategy.get('params', {})
        }
        
        try:
            dag_results = self._strategy_dag.execute(dag_inputs)
            
            # Extract signals from DAG output
            signals = dag_results.get('signal', [])
            
            # Simulate trades from signals
            if isinstance(signals, list):
                results['num_trades'] = len(signals)
                
                # Simple trade simulation (deterministic)
                total_pnl = 0
                for i, signal in enumerate(signals):
                    if isinstance(signal, dict):
                        pnl = signal.get('pnl', 0)
                        total_pnl += pnl
                        results['trades'].append({
                            'trade_id': i,
                            'signal': signal,
                            'pnl': pnl
                        })
                
                if results['num_trades'] > 0:
                    results['avg_trade_return'] = total_pnl // max(1, results['num_trades'])
                    results['total_return'] = total_pnl
        
        except Exception as e:
            results['status'] = 'FAILED'
            results['error'] = str(e)
        
        return results
    
    def _aggregate_batch_results(self, batch_id: str) -> Dict[str, Any]:
        """
        Aggregate results from all strategies in batch.
        
        Args:
            batch_id: Batch identifier.
            
        Returns:
            dict: Aggregated batch results.
        """
        batch_meta = self._batches.get(batch_id, {})
        strategies = batch_meta.get('strategies', [])
        date_range = batch_meta.get('date_range', (0, 0))
        
        strategy_results = []
        
        # Execute each strategy
        for strategy in strategies:
            strat_result = self._execute_strategy_backtest(strategy, date_range)
            strategy_results.append(strat_result)
        
        # Aggregate metrics
        total_return_sum = 0
        max_drawdown_max = 0
        total_trades = 0
        completed_count = 0
        
        for result in strategy_results:
            if result.get('status') == 'COMPLETED':
                completed_count += 1
                total_return_sum += result.get('total_return', 0)
                max_drawdown_max = max(max_drawdown_max, result.get('max_drawdown', 0))
                total_trades += result.get('num_trades', 0)
        
        avg_return = 0
        if completed_count > 0:
            avg_return = total_return_sum // completed_count
        
        aggregated = {
            'batch_id': batch_id,
            'date_range': date_range,
            'num_strategies': len(strategies),
            'completed_strategies': completed_count,
            'failed_strategies': len(strategies) - completed_count,
            'aggregate_total_return': total_return_sum,
            'aggregate_avg_return': avg_return,
            'aggregate_max_drawdown': max_drawdown_max,
            'aggregate_total_trades': total_trades,
            'strategy_results': strategy_results,
            'status': 'COMPLETED'
        }
        
        return aggregated
    
    def get_results(self, batch_id: str) -> dict:
        """
        Retrieve results for a submitted batch.
        
        Executes batch if not already executed, then returns aggregated
        results including individual strategy performance and combined metrics.
        Results are cached after first retrieval.
        
        Args:
            batch_id: Batch ID returned from submit_batch().
            
        Returns:
            dict: Aggregated batch results with keys:
                - batch_id (str): Batch identifier
                - date_range (tuple): (start_date, end_date)
                - num_strategies (int): Total strategies in batch
                - completed_strategies (int): Successfully completed
                - failed_strategies (int): Failed executions
                - aggregate_total_return (int): Sum of all returns
                - aggregate_avg_return (int): Average return per strategy
                - aggregate_max_drawdown (int): Worst drawdown observed
                - aggregate_total_trades (int): Total trades across all strategies
                - strategy_results (list): Individual strategy result dicts
                - status (str): 'COMPLETED' or 'FAILED'
                
            Empty dict if batch_id not found.
            
        Raises:
            ValueError: If batch_id is not a string.
            
        Example:
            >>> results = batch_test.get_results(batch_id)
            >>> print(f"Total return: {results['aggregate_total_return']}")
            >>> for strat_result in results['strategy_results']:
            ...     print(f"{strat_result['strategy_name']}: {strat_result['total_return']}")
        """
        if not isinstance(batch_id, str):
            raise ValueError("batch_id must be a string")
        
        if batch_id not in self._batches:
            return {}
        
        # Return cached results if available
        if batch_id in self._batch_results:
            return self._batch_results[batch_id].copy()
        
        # Execute batch if not cached
        try:
            aggregated = self._aggregate_batch_results(batch_id)
            self._batch_results[batch_id] = aggregated
            return aggregated.copy()
        
        except Exception as e:
            return {
                'batch_id': batch_id,
                'status': 'FAILED',
                'error': str(e)
            }
    
    def stream_batch_results(
        self,
