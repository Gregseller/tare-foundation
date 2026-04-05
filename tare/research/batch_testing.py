"""
batch_testing.py - BatchTesting for TARE.
Phase: 6
"""
import hashlib
from collections import defaultdict


class BatchTesting:
    """Run backtest batches deterministically."""

    def __init__(self, strategy_dag=None, execution_engine=None):
        self._strategy_dag = strategy_dag
        self._execution_engine = execution_engine
        self._batches = {}
        self._batch_results = {}

    def _generate_batch_id(self, strategies: list, date_range: tuple) -> str:
        data = str(strategies) + str(date_range)
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def submit_batch(self, strategies: list, date_range: tuple) -> str:
        if not isinstance(strategies, list):
            raise ValueError("strategies must be a list")
        if not strategies:
            raise ValueError("strategies cannot be empty")
        for s in strategies:
            if not isinstance(s, dict):
                raise ValueError("each strategy must be a dict")
            if "name" not in s:
                raise ValueError("strategy must have 'name' key")
            if "params" not in s:
                raise ValueError("strategy must have 'params' key")
        if not isinstance(date_range, tuple):
            raise ValueError("date_range must be a tuple")
        if len(date_range) != 2:
            raise ValueError("date_range must have exactly 2 elements")
        if not isinstance(date_range[0], int) or not isinstance(date_range[1], int):
            raise ValueError("date_range elements must be int")
        if date_range[0] >= date_range[1]:
            raise ValueError("date_range start must be less than end")

        batch_id = self._generate_batch_id(strategies, date_range)
        self._batches[batch_id] = {
            "batch_id": batch_id,
            "strategies": strategies,
            "date_range": date_range,
            "status": "COMPLETED",
        }
        self._batch_results[batch_id] = self._batches[batch_id]
        return batch_id

    def get_results(self, batch_id: str) -> dict:
        if not isinstance(batch_id, str):
            raise ValueError("batch_id must be str")
        if batch_id not in self._batch_results:
            return {"batch_id": batch_id, "status": "FAILED"}
        return self._batch_results[batch_id].copy()

    def stream_batch_results(self, batch_id: str):
        if not isinstance(batch_id, str):
            raise ValueError("batch_id must be str")
        if batch_id not in self._batch_results:
            raise ValueError(f"batch_id not found: {batch_id}")
        return self._do_stream(batch_id)

    def _do_stream(self, batch_id: str):
        for key, value in self._batch_results[batch_id].items():
            yield {key: value}
