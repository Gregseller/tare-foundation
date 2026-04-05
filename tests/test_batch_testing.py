import unittest
from tare.research.batch_testing import BatchTesting


class TestBatchTesting(unittest.TestCase):
    def setUp(self):
        self.batch_testing = BatchTesting(strategy_dag=None, execution_engine=None)

    # ----- __init__ -----
    def test_init_with_none(self):
        bt = BatchTesting(None, None)
        self.assertIsInstance(bt, BatchTesting)

    # ----- submit_batch -----
    def test_submit_batch_valid(self):
        strategies = [
            {"name": "ma_cross", "params": {"fast": 10, "slow": 30}},
            {"name": "rsi", "params": {"period": 14}}
        ]
        date_range = (1000000000, 2000000000)
        batch_id = self.batch_testing.submit_batch(strategies, date_range)
        self.assertIsInstance(batch_id, str)
        self.assertGreater(len(batch_id), 0)

    def test_submit_batch_determinism(self):
        strategies = [{"name": "test", "params": {}}]
        date_range = (0, 1000)
        id1 = self.batch_testing.submit_batch(strategies, date_range)
        id2 = self.batch_testing.submit_batch(strategies, date_range)
        self.assertEqual(id1, id2)

    def test_submit_batch_strategies_not_list(self):
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch("not list", (0, 1000))

    def test_submit_batch_empty_strategies(self):
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch([], (0, 1000))

    def test_submit_batch_strategy_missing_name(self):
        strategies = [{"params": {}}]
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch(strategies, (0, 1000))

    def test_submit_batch_strategy_missing_params(self):
        strategies = [{"name": "test"}]
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch(strategies, (0, 1000))

    def test_submit_batch_date_range_not_tuple(self):
        strategies = [{"name": "test", "params": {}}]
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch(strategies, "not tuple")

    def test_submit_batch_date_range_wrong_length(self):
        strategies = [{"name": "test", "params": {}}]
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch(strategies, (0,))
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch(strategies, (0, 1, 2))

    def test_submit_batch_date_range_not_int(self):
        strategies = [{"name": "test", "params": {}}]
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch(strategies, (0.5, 1000))

    def test_submit_batch_date_range_start_gt_end(self):
        strategies = [{"name": "test", "params": {}}]
        # Не указано, но вероятно, должно быть исключение
        with self.assertRaises(ValueError):
            self.batch_testing.submit_batch(strategies, (2000, 1000))

    # ----- get_results -----
    def test_get_results_existing_batch(self):
        strategies = [{"name": "test", "params": {}}]
        date_range = (0, 1000)
        batch_id = self.batch_testing.submit_batch(strategies, date_range)
        results = self.batch_testing.get_results(batch_id)
        self.assertIsInstance(results, dict)
        self.assertIn("batch_id", results)
        self.assertEqual(results["batch_id"], batch_id)
        # Дополнительные поля могут быть, но не обязательны
        # Например, status, strategies, date_range и т.д.
        # Проверим, что есть ключ "status" или подобное
        self.assertIn("status", results)

    def test_get_results_nonexistent_batch(self):
        results = self.batch_testing.get_results("nonexistent_id")
        self.assertIsInstance(results, dict)
        self.assertEqual(results.get("status"), "FAILED")

    def test_get_results_invalid_id_type(self):
        with self.assertRaises(ValueError):
            self.batch_testing.get_results(123)

    # ----- stream_batch_results -----
    def test_stream_batch_results_existing_batch(self):
        strategies = [{"name": "test", "params": {}}]
        date_range = (0, 1000)
        batch_id = self.batch_testing.submit_batch(strategies, date_range)
        gen = self.batch_testing.stream_batch_results(batch_id)
        self.assertTrue(hasattr(gen, "__iter__"))
        self.assertTrue(hasattr(gen, "__next__"))
        # Можно получить первый элемент, если есть
        try:
            first = next(gen)
            self.assertIsInstance(first, dict)
        except StopIteration:
            pass  # допустимо, если поток пуст

    def test_stream_batch_results_nonexistent_batch(self):
        with self.assertRaises(ValueError):
            self.batch_testing.stream_batch_results("nonexistent_id")

    def test_stream_batch_results_invalid_id_type(self):
        with self.assertRaises(ValueError):
            self.batch_testing.stream_batch_results(123)

    # ----- детерминизм для get_results -----
    def test_get_results_determinism(self):
        strategies = [{"name": "test", "params": {}}]
        date_range = (0, 1000)
        batch_id = self.batch_testing.submit_batch(strategies, date_range)
        res1 = self.batch_testing.get_results(batch_id)
        res2 = self.batch_testing.get_results(batch_id)
        self.assertEqual(res1, res2)


if __name__ == "__main__":
    unittest.main()