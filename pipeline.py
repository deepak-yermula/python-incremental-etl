"""A small but genuine, incremental and idempotent CSV -> SQLite ETL."""
import argparse
import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import json
import logging
from pathlib import Path
import sqlite3

LOGGER = logging.getLogger('orders_etl')
COLUMNS = {'order_id', 'customer_id', 'order_date', 'amount', 'status', 'updated_at'}
STATUSES = {'pending', 'completed', 'cancelled'}
SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_date TEXT NOT NULL,
    amount_cents INTEGER NOT NULL CHECK(amount_cents >= 0),
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def validate(row):
    """Return a normalized record or raise ValueError for an invalid row."""
    try:
        record = {key: str(row.get(key) or '').strip() for key in COLUMNS}
        if not record['order_id'] or not record['customer_id']:
            raise ValueError('order_id and customer_id are required')
        date.fromisoformat(record['order_date'])
        updated = datetime.fromisoformat(record['updated_at'])
        if updated.tzinfo is not None:
            raise ValueError('updated_at must be timezone-naive UTC in this demo')
        if record['status'] not in STATUSES:
            raise ValueError('invalid status')
        amount = Decimal(record['amount'])
        cents = amount * 100
        if not amount.is_finite() or amount < 0 or cents != cents.to_integral_value():
            raise ValueError('amount must be a nonnegative number with at most two decimals')
        record['amount_cents'] = int(cents)
        record['updated_at'] = updated.isoformat(timespec='seconds')
        del record['amount']
        return record
    except (TypeError, InvalidOperation, OverflowError, ValueError) as exc:
        raise ValueError(str(exc)) from exc


def run(input_path, database, rejects):
    input_path = Path(input_path)
    database = Path(database)
    rejects = Path(rejects)
    metrics = {'rows_read': 0, 'invalid': 0, 'duplicates_in_file': 0,
               'inserted': 0, 'updated': 0, 'unchanged': 0}
    latest = {}
    rejected = []

    with input_path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or not COLUMNS.issubset(reader.fieldnames):
            raise ValueError(f'CSV must include columns: {sorted(COLUMNS)}')
        for row in reader:
            metrics['rows_read'] += 1
            try:
                clean = validate(row)
            except ValueError as exc:
                metrics['invalid'] += 1
                rejected.append({'line': metrics['rows_read'] + 1,
                                 'reason': str(exc), 'source_row': row})
                continue
            previous = latest.get(clean['order_id'])
            if previous is not None:
                metrics['duplicates_in_file'] += 1
            if previous is None or clean['updated_at'] > previous['updated_at']:
                latest[clean['order_id']] = clean
            elif clean['updated_at'] == previous['updated_at'] and clean != previous:
                # A timestamp tie with conflicting content has no well-defined winner.
                raise ValueError(f"Conflicting rows for {clean['order_id']} have the same updated_at")

    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as conn:
        conn.execute(SCHEMA)
        with conn:  # commits as a unit; on error, roll back DML changes
            for order_id, row in sorted(latest.items()):
                prior = conn.execute('SELECT updated_at FROM orders WHERE order_id = ?', (order_id,)).fetchone()
                if prior is None:
                    metric = 'inserted'
                elif row['updated_at'] > prior[0]:
                    metric = 'updated'
                else:
                    metric = 'unchanged'
                if metric != 'unchanged':
                    conn.execute("""
                      INSERT INTO orders (order_id, customer_id, order_date, amount_cents, status, updated_at)
                      VALUES (?, ?, ?, ?, ?, ?)
                      ON CONFLICT(order_id) DO UPDATE SET
                        customer_id=excluded.customer_id,
                        order_date=excluded.order_date,
                        amount_cents=excluded.amount_cents,
                        status=excluded.status,
                        updated_at=excluded.updated_at
                      WHERE excluded.updated_at > orders.updated_at
                    """, (row['order_id'], row['customer_id'], row['order_date'],
                          row['amount_cents'], row['status'], row['updated_at']))
                metrics[metric] += 1

    rejects.parent.mkdir(parents=True, exist_ok=True)
    with rejects.open('w', encoding='utf-8') as stream:
        for item in rejected:
            stream.write(json.dumps(item) + '\n')
    LOGGER.info('ETL metrics: %s', json.dumps(metrics, sort_keys=True))
    return metrics


def main():
    parser = argparse.ArgumentParser(description='Incremental CSV-to-SQLite orders loader')
    parser.add_argument('--input', required=True)
    parser.add_argument('--database', default='orders.db')
    parser.add_argument('--rejects', default='rejects.jsonl')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    run(args.input, args.database, args.rejects)

if __name__ == '__main__':
    main()
