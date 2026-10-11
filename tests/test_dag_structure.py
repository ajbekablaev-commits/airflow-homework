# tests/test_dag_structure.py
import pytest, sys, os
from datetime import timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

def test_dag_import():
    '''DAG файлы синтаксис қатесізіне импортталуы тиіс'''
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'dag_module',
        os.path.join(os.path.dirname(__file__), '..', 'dags', 'popular_people_pipeline.py')
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)   # ImportError болса тест сәтсіз

def test_dag_exists():
    '''popular_people_pipeline DAG-і бар болуы тиіс'''
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert 'popular_people_pipeline' in dagbag.dags, \
        f'DAG табылмады. Қателер: {dagbag.import_errors}'

def test_dag_no_import_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dagbag.import_errors) == 0, \
        f'DAG импорт қателері: {dagbag.import_errors}'

def test_dag_schedule():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['popular_people_pipeline']
    assert dag.schedule_interval in ['@daily', timedelta(days=1), '0 0 * * *'], \
        'schedule @daily болуы тиіс'

def test_dag_catchup_false():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['popular_people_pipeline']
    assert dag.catchup == False

def test_required_tasks_exist():
    '''Барлық міндетті тапсырмалар бар болуы тиіс'''
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['popular_people_pipeline']
    task_ids = dag.task_ids
    required = [
        'start', 'end',
        'validate_data', 'check_schema',
        'spark_load', 'spark_clean',
        'spark_sql_analysis', 'spark_partition_write',
        'generate_report', 'check_report_quality',
        'notify_success', 'notify_warning',
    ]
    for t in required:
        assert t in task_ids, f"'{t}' тапсырмасы DAG-та жоқ"

def test_task_dependencies():
    '''Тапсырма тізбегі дұрыс болуы тиіс'''
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['popular_people_pipeline']

    def upstream_ids(task_id):
        return {t.task_id for t in dag.get_task(task_id).upstream_list}

    # spark_load алдында validate_data немесе check_schema болуы тиіс
    assert upstream_ids('spark_load') & {'validate_data', 'check_schema'}
    # spark_clean алдында spark_load болуы тиіс
    assert 'spark_load' in upstream_ids('spark_clean')
    # generate_report: spark_sql_analysis немесе spark_partition_write кейін
    assert upstream_ids('generate_report') & {'spark_sql_analysis', 'spark_partition_write'}

def test_dag_tags():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['popular_people_pipeline']
    tag_names = [t.name if hasattr(t,'name') else t for t in dag.tags]
    assert 'spark' in tag_names or 'pyspark' in tag_names
