from mini_orquestrador import DAG
from pathlib import Path

dag = DAG('dag_retry_quality_gate', schedule=None)

@dag.task('fonte_instavel', retries=1, retry_delay_seconds=0)
def fonte_instavel(context):
    if context['attempt'] == 1:
        raise ConnectionError('fonte temporariamente indisponível')
    print('Fonte respondeu na segunda tentativa')
    return 'arquivo_recebido'

@dag.task('quality_gate')
def quality_gate(context):
    recebido = context['xcom'].get('fonte_instavel')
    if recebido != 'arquivo_recebido':
        raise ValueError('entrada inválida')
    print('✓ Quality gate aprovado. A próxima etapa pode continuar.')
    return 'aprovado'

@dag.task('publicar')
def publicar(context):
    Path('reports/publicacao_ok.txt').write_text('dados publicados após retry e validação\n', encoding='utf-8')
    print('Publicação simulada concluída')

dag.chain('fonte_instavel','quality_gate','publicar')
dag.run('2025-01-15')
