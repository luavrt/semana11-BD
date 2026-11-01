from mini_orquestrador import DAG

dag = DAG('dag_hello_world', schedule='@daily')

@dag.task('inicio')
def inicio(context):
    print('Pipeline iniciado para', context['logical_date'])
    return 'ok_inicio'

@dag.task('processar')
def processar(context):
    anterior = context['xcom'].get('inicio')
    print('Recebi do XCom:', anterior)
    return 'dados_processados'

@dag.task('fim')
def fim(context):
    print('Pipeline finalizado com sucesso')
    return 'ok_fim'

dag.chain('inicio','processar','fim')
dag.run('2025-01-15')
