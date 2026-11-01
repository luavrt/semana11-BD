from mini_orquestrador import DAG
from utils_dados import DB_PATH, gerar_dados, ler_pedidos, write_csv, read_csv, append_json_log
from pathlib import Path
from datetime import datetime
import statistics

if not DB_PATH.exists():
    gerar_dados()

dag = DAG('pipeline_lakehouse_local', schedule='0 2 * * *')

@dag.task('extrair_raw')
def extrair_raw(context):
    rows = ler_pedidos()
    write_csv('lake/raw/pedidos.csv', rows)
    append_json_log('governance/lineage_log.json', {'ts': datetime.now().isoformat(), 'de':'sqlite/pedidos', 'para':'lake/raw/pedidos.csv', 'ops':['extração fiel sem limpeza']})
    print('raw:', len(rows), 'linhas')
    return len(rows)

@dag.task('transformar_silver')
def transformar_silver(context):
    rows = read_csv('lake/raw/pedidos.csv')
    clean = []
    for r in rows:
        total = float(r['total'])
        if total <= 0:
            continue
        r['status'] = r['status'].lower().strip()
        r['ano'] = r['criado_em'][:4]
        r['mes'] = r['criado_em'][5:7]
        r['total'] = f'{total:.2f}'
        clean.append(r)
    write_csv('lake/silver/pedidos.csv', clean)
    append_json_log('governance/lineage_log.json', {'ts': datetime.now().isoformat(), 'de':'lake/raw/pedidos.csv', 'para':'lake/silver/pedidos.csv', 'ops':['status normalizado','total > 0','ano e mes criados']})
    print('silver:', len(clean), 'linhas')
    return len(clean)

@dag.task('validar_qualidade')
def validar_qualidade(context):
    rows = read_csv('lake/silver/pedidos.csv')
    ids = [r['id'] for r in rows]
    erros = []
    if len(ids) != len(set(ids)): erros.append('id duplicado')
    if any(float(r['total']) <= 0 for r in rows): erros.append('total não positivo')
    if any(not r['cliente_id'] for r in rows): erros.append('cliente_id nulo')
    if erros:
        raise ValueError(erros)
    print('✓ qualidade aprovada:', len(rows), 'registros')
    return {'linhas': len(rows), 'erros': 0}

@dag.task('gerar_gold')
def gerar_gold(context):
    rows = read_csv('lake/silver/pedidos.csv')
    grupos = {}
    for r in rows:
        cid = r['cliente_id']; grupos.setdefault(cid, []).append(float(r['total']))
    gold=[]
    for cid, vals in sorted(grupos.items(), key=lambda kv:int(kv[0])):
        gold.append({'cliente_id': cid, 'total_pedidos': len(vals), 'ticket_medio': f'{statistics.mean(vals):.2f}', 'total_gasto': f'{sum(vals):.2f}'})
    write_csv('lake/gold/features_cliente.csv', gold)
    append_json_log('governance/lineage_log.json', {'ts': datetime.now().isoformat(), 'de':'lake/silver/pedidos.csv', 'para':'lake/gold/features_cliente.csv', 'ops':['groupby cliente_id','count/mean/sum']})
    print('gold:', len(gold), 'clientes')
    return len(gold)

dag.chain('extrair_raw','transformar_silver','validar_qualidade','gerar_gold')
dag.run('2025-01-15')
