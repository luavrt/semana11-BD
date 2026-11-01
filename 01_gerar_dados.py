from utils_dados import gerar_dados, DB_PATH
p = gerar_dados()
print('=== DADOS GERADOS ===')
print('Banco SQLite:', p)
print('Clientes: 60')
print('Pedidos : 360')
print('Problemas de qualidade propositalmente inseridos: alguns totais negativos')
