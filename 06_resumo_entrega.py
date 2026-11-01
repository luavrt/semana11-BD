from pathlib import Path
import json
print('=== RESUMO DA ENTREGA ===')
paths = ['dados/loja_demo.sqlite','lake/raw/pedidos.csv','lake/silver/pedidos.csv','lake/gold/features_cliente.csv','governance/lineage_log.json']
linhas=[]
linhas.append('# Resumo da prática — Semana 11\n')
for p in paths:
    path = Path(p)
    status = 'OK' if path.exists() else 'FALTANDO'
    size = path.stat().st_size if path.exists() else 0
    msg = f'- {status}: `{p}` ({size} bytes)'
    print(msg); linhas.append(msg)
logs = sorted(Path('reports').glob('*'))
linhas.append('\n## Arquivos em reports/\n')
for l in logs:
    linhas.append(f'- `{l}`')
Path('reports/resumo_entrega.md').write_text('\n'.join(linhas), encoding='utf-8')
print('\nResumo gravado em reports/resumo_entrega.md')
print('Anexe prints no caderno e gere o PDF final.')
