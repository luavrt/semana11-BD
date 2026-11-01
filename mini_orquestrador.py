from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
import csv, time, traceback

REPORTS = Path('reports')
REPORTS.mkdir(exist_ok=True)

@dataclass
class Task:
    task_id: str
    func: callable
    retries: int = 0
    retry_delay_seconds: float = 0.0
    upstream: set[str] = field(default_factory=set)
    downstream: set[str] = field(default_factory=set)

class DAG:
    def __init__(self, dag_id: str, schedule: str = '@daily'):
        self.dag_id = dag_id
        self.schedule = schedule
        self.tasks: dict[str, Task] = {}
        self.xcom: dict[str, object] = {}

    def task(self, task_id=None, retries=0, retry_delay_seconds=0.0):
        def decorator(func):
            tid = task_id or func.__name__
            self.tasks[tid] = Task(tid, func, retries, retry_delay_seconds)
            return tid
        return decorator

    def chain(self, *task_ids):
        for a, b in zip(task_ids, task_ids[1:]):
            self.tasks[a].downstream.add(b)
            self.tasks[b].upstream.add(a)

    def _order(self):
        done, order = set(), []
        while len(done) < len(self.tasks):
            progressed = False
            for tid, task in self.tasks.items():
                if tid in done:
                    continue
                if task.upstream.issubset(done):
                    order.append(tid)
                    done.add(tid)
                    progressed = True
            if not progressed:
                raise RuntimeError('Ciclo detectado na DAG. DAGs não podem ter A -> B -> A.')
        return order

    def run(self, logical_date=None):
        logical_date = logical_date or datetime.now().date().isoformat()
        log_path = REPORTS / f'{self.dag_id}_run_log.csv'
        new_file = not log_path.exists()
        with log_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(['timestamp','dag_id','logical_date','task_id','attempt','status','message'])
            print(f'\n=== DAG {self.dag_id} | logical_date={logical_date} | schedule={self.schedule} ===')
            for tid in self._order():
                task = self.tasks[tid]
                max_attempts = 1 + task.retries
                for attempt in range(1, max_attempts + 1):
                    context = {'dag_id': self.dag_id, 'task_id': tid, 'logical_date': logical_date, 'attempt': attempt, 'xcom': self.xcom}
                    try:
                        print(f'→ {tid} | tentativa {attempt}/{max_attempts}')
                        result = task.func(context)
                        self.xcom[tid] = result
                        writer.writerow([datetime.now().isoformat(), self.dag_id, logical_date, tid, attempt, 'success', str(result)[:120]])
                        print(f'✓ {tid} concluída')
                        break
                    except Exception as e:
                        msg = f'{type(e).__name__}: {e}'
                        writer.writerow([datetime.now().isoformat(), self.dag_id, logical_date, tid, attempt, 'failed', msg])
                        print(f'✗ {tid} falhou: {msg}')
                        if attempt >= max_attempts:
                            print('\nDAG interrompida. Leia o log em:', log_path)
                            raise
                        if task.retry_delay_seconds:
                            time.sleep(task.retry_delay_seconds)
        print('=== DAG concluída com sucesso ===')
        return self.xcom
