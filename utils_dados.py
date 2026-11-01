from pathlib import Path
from datetime import datetime, timedelta
import sqlite3, csv, random, json

ROOT = Path('.')
DADOS = ROOT/'dados'
LAKE = ROOT/'lake'
REPORTS = ROOT/'reports'
for p in [DADOS, LAKE/'raw', LAKE/'silver', LAKE/'gold', REPORTS, ROOT/'governance']:
    p.mkdir(parents=True, exist_ok=True)

DB_PATH = DADOS/'loja_demo.sqlite'

def gerar_dados(seed=11, clientes=60, pedidos=360):
    random.seed(seed)
    if DB_PATH.exists():
        DB_PATH.unlink()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute('CREATE TABLE clientes (id INTEGER PRIMARY KEY, nome TEXT, email TEXT, cidade TEXT)')
    cur.execute('CREATE TABLE pedidos (id INTEGER PRIMARY KEY, cliente_id INTEGER, total REAL, status TEXT, criado_em TEXT)')
    cidades = ['Teresina','São Luís','Fortaleza','Recife','Parnaíba']
    status = ['entregue','pendente','cancelado']
    for i in range(1, clientes+1):
        cur.execute('INSERT INTO clientes VALUES (?,?,?,?)', (i, f'Cliente {i:03d}', f'cliente{i:03d}@email.com', random.choice(cidades)))
    inicio = datetime(2025, 1, 1)
    for i in range(1, pedidos+1):
        cid = random.randint(1, clientes)
        total = round(random.uniform(20, 950), 2)
        if i % 71 == 0:
            total = -abs(total)  # problema de qualidade proposital
        st = random.choice(status)
        data = inicio + timedelta(days=random.randint(0, 20))
        cur.execute('INSERT INTO pedidos VALUES (?,?,?,?,?)', (i, cid, total, st, data.date().isoformat()))
    con.commit(); con.close()
    return DB_PATH

def ler_pedidos():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute('SELECT * FROM pedidos ORDER BY id')]
    con.close(); return rows

def write_csv(path, rows, fieldnames=None):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    if not rows:
        return
    fieldnames = fieldnames or list(rows[0].keys())
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader(); w.writerows(rows)

def read_csv(path):
    with Path(path).open(newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def append_json_log(path, item):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    data = []
    if path.exists():
        data = json.loads(path.read_text(encoding='utf-8'))
    data.append(item)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
