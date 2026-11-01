from mini_orquestrador import DAG
from pathlib import Path
from datetime import date, timedelta

dag = DAG('dag_backfill_demo', schedule='@daily')

@dag.task('processar_data')
def processar_data(context):
    d = context['logical_date']
    out = Path('reports') / f'backfill_{d}.txt'
    out.write_text(f'Data lógica processada: {d}\n', encoding='utf-8')
    print('processada data lógica:', d)
    return str(out)

# Sem dependências extras: uma única task por data.
inicio = date(2025,1,10)
for i in range(3):
    dag.run((inicio + timedelta(days=i)).isoformat())
print('\nBackfill concluído para 3 datas. Confira reports/backfill_*.txt')
