import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline import run, validate

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / 'orders.db'
        self.rejects = Path(self.tmp.name) / 'rejects.jsonl'

    def tearDown(self):
        self.tmp.cleanup()

    def run_batch(self, filename):
        return run(ROOT / 'data' / filename, self.db, self.rejects)

    def test_first_batch_validation_and_deduplication(self):
        metrics = self.run_batch('orders_day1.csv')
        self.assertEqual(metrics['rows_read'], 5)
        self.assertEqual(metrics['invalid'], 1)
        self.assertEqual(metrics['duplicates_in_file'], 1)
        self.assertEqual(metrics['inserted'], 3)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 3)
            self.assertEqual(conn.execute('SELECT amount_cents FROM orders WHERE order_id=?', ('A102',)).fetchone()[0], 9999)

    def test_incremental_update_and_replay(self):
        self.run_batch('orders_day1.csv')
        metrics = self.run_batch('orders_day2.csv')
        self.assertEqual(metrics['inserted'], 2)
        self.assertEqual(metrics['updated'], 1)
        self.assertEqual(metrics['unchanged'], 1)
        repeat = self.run_batch('orders_day2.csv')
        self.assertEqual(repeat['inserted'], 0)
        self.assertEqual(repeat['updated'], 0)
        self.assertEqual(repeat['unchanged'], 4)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute('SELECT status FROM orders WHERE order_id=?', ('A101',)).fetchone()[0], 'completed')
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 5)

    def test_rejects_invalid_amount_precision(self):
        sample = {'order_id': 'A1', 'customer_id': 'C1', 'order_date': '2026-11-01',
                  'amount': '2.345', 'status': 'completed', 'updated_at': '2026-11-01T01:00:00'}
        with self.assertRaises(ValueError):
            validate(sample)

if __name__ == '__main__':
    unittest.main()
