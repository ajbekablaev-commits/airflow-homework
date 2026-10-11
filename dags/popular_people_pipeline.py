from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag as dag_decorator, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

import csv
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from spark_jobs.transformations import (
    load_data,
    clean_data,
    decode_gender,
    add_popularity_tier,
)

DATA_PATH = os.path.abspath(os.path.join(ROOT_DIR, 'data', 'popular_people.csv'))
OUTPUT_PATH = os.path.abspath(os.path.join(ROOT_DIR, 'output'))

REQUIRED_COLUMNS = ['name', 'gender', 'known_for_department', 'original_name', 'popularity']


default_args = {
    'owner': 'student',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'sla': timedelta(hours=1),
}


def _get_spark(app_name='AirflowPopularPeople'):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .master('local[*]')
        .config('spark.ui.showConsoleProgress', 'false')
        .getOrCreate()
    )


def _count_rows(path):
    with open(path, newline='', encoding='utf-8') as f:
        return sum(1 for _ in csv.reader(f)) - 1


def _do_validate():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f'Деректер файлы табылмады: {DATA_PATH}')
    row_count = _count_rows(DATA_PATH)
    if row_count <= 9000:
        raise ValueError(f'Жол саны жеткіліксіз: {row_count} (>9000 болуы тиіс)')
    return {'row_count': row_count, 'status': 'OK'}


def _do_spark_load():
    spark = _get_spark()
    try:
        df = load_data(spark, DATA_PATH)
        return {
            'row_count': df.count(),
            'columns': df.columns,
            'spark_version': spark.version,
        }
    finally:
        spark.stop()


def _do_spark_clean():
    spark = _get_spark()
    try:
        df = load_data(spark, DATA_PATH)
        df = add_popularity_tier(decode_gender(clean_data(df)))
        cleaned_path = os.path.join(OUTPUT_PATH, 'cleaned')
        df.write.mode('overwrite').parquet(cleaned_path)
        return spark.read.parquet(cleaned_path).count()
    finally:
        spark.stop()


def _validate_data(**context):
    return _do_validate()


def _check_schema(**context):
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f'Деректер файлы табылмады: {DATA_PATH}')
    with open(DATA_PATH, newline='', encoding='utf-8') as f:
        header = next(csv.reader(f), [])
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ValueError(f'Міндетті бағандар жетіспейді: {missing}')
    return header


def _spark_load(**context):
    return _do_spark_load()


def _spark_clean(**context):
    return _do_spark_clean()


def _spark_sql_analysis(**context):
    spark = _get_spark()
    try:
        cleaned_path = os.path.join(OUTPUT_PATH, 'cleaned')
        reports_path = os.path.join(OUTPUT_PATH, 'reports')
        spark.read.parquet(cleaned_path).createOrReplaceTempView('people')

        dept_stats = spark.sql('''
            SELECT known_for_department,
                   COUNT(*) AS person_count,
                   ROUND(AVG(popularity), 2) AS avg_popularity,
                   SUM(popularity) AS total_popularity
            FROM people
            GROUP BY known_for_department
            ORDER BY person_count DESC
        ''')

        gender_dist = spark.sql('''
            SELECT gender_label,
                   COUNT(*) AS person_count,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
            FROM people
            GROUP BY gender_label
            ORDER BY person_count DESC
        ''')

        top3 = spark.sql('''
            SELECT name, known_for_department, popularity, row_num
            FROM (
                SELECT name, known_for_department, popularity,
                       ROW_NUMBER() OVER (
                           PARTITION BY known_for_department
                           ORDER BY popularity DESC
                       ) AS row_num
                FROM people
            ) t
            WHERE row_num <= 3
            ORDER BY known_for_department, row_num
        ''')

        dept_stats.write.mode('overwrite').parquet(os.path.join(reports_path, 'dept_stats'))
        gender_dist.write.mode('overwrite').parquet(os.path.join(reports_path, 'gender_distribution'))
        top3.write.mode('overwrite').parquet(os.path.join(reports_path, 'top3_by_dept'))
    finally:
        spark.stop()


def _spark_partition_write(**context):
    spark = _get_spark()
    try:
        df = spark.read.parquet(os.path.join(OUTPUT_PATH, 'cleaned'))
        df.repartition(4).write.mode('overwrite').partitionBy('known_for_department').parquet(
            os.path.join(OUTPUT_PATH, 'partitioned')
        )
    finally:
        spark.stop()


def _generate_report(**context):
    ti = context['ti']
    cleaned_rows = ti.xcom_pull(task_ids='spark_clean', key='return_value')
    spark = _get_spark()
    try:
        stats = spark.read.parquet(os.path.join(OUTPUT_PATH, 'reports', 'dept_stats')).collect()
    finally:
        spark.stop()

    total_rows = sum(r['person_count'] for r in stats)
    total_pop = sum(r['total_popularity'] for r in stats)
    avg_pop = total_pop / total_rows if total_rows else 0.0
    top5 = sorted(stats, key=lambda r: r['person_count'], reverse=True)[:5]

    lines = [
        'POPULAR PEOPLE PIPELINE REPORT',
        '=' * 40,
        f'Құрылған уақыты: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'Тазаланған жолдар: {cleaned_rows if cleaned_rows is not None else "N/A"}',
        f'Жалпы жол саны: {total_rows}',
        f'Орташа танымалдылық: {avg_pop:.2f}',
        '',
        'ТОП-5 сала (адам саны бойынша):',
    ]
    for i, r in enumerate(top5, 1):
        lines.append(f"  {i}. {r['known_for_department']}: {r['person_count']}")

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    with open(os.path.join(OUTPUT_PATH, 'final_report.txt'), 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    return total_rows


def _check_report_quality(**context):
    row_count = context['ti'].xcom_pull(task_ids='generate_report', key='return_value')
    if row_count is not None and row_count > 100:
        return 'notify_success'
    return 'notify_warning'


def _notify_success(**context):
    n = context['ti'].xcom_pull(task_ids='generate_report', key='return_value')
    print(f'Pipeline сәтті аяқталды: {n} жол өңделді')


def _notify_warning(**context):
    n = context['ti'].xcom_pull(task_ids='generate_report', key='return_value')
    print(f'ЕСКЕРТУ: жол саны күтілгеннен аз — {n}')


DOC_MD = '''
## Popular People Pipeline

**Не істейді:** TMDB танымал адамдар датасетін тексереді, PySpark арқылы
тазалайды (gender кодын мәтінге айналдыру, popularity деңгейі), Spark SQL
талдауын жасайды, деректерді салалар бойынша партициялап жазады және мәтіндік
есеп шығарады.

**Кіріс деректері:** `data/popular_people.csv` (name, gender,
known_for_department, original_name, popularity; ~9 980 жол).

**Шығыс деректері:**
- `output/cleaned/` - тазаланған Parquet
- `output/reports/` - 3 талдау нәтижесі (Parquet)
- `output/partitioned/` - `known_for_department` бойынша бөлінген Parquet
- `output/final_report.txt` - қорытынды есеп

**Іске қосылу кестесі:** `@daily` (күн сайын), `catchup=False`.
'''

with DAG(
    dag_id='popular_people_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['spark', 'etl', 'popular_people'],
    doc_md=DOC_MD,
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    validate_data = PythonOperator(task_id='validate_data', python_callable=_validate_data)
    check_schema = PythonOperator(task_id='check_schema', python_callable=_check_schema)

    spark_load = PythonOperator(
        task_id='spark_load',
        python_callable=_spark_load,
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )

    spark_clean = PythonOperator(task_id='spark_clean', python_callable=_spark_clean, sla=timedelta(minutes=30))
    spark_sql_analysis = PythonOperator(task_id='spark_sql_analysis', python_callable=_spark_sql_analysis)
    spark_partition_write = PythonOperator(task_id='spark_partition_write', python_callable=_spark_partition_write)
    generate_report = PythonOperator(task_id='generate_report', python_callable=_generate_report)

    check_quality = BranchPythonOperator(task_id='check_report_quality', python_callable=_check_report_quality)
    notify_success = PythonOperator(task_id='notify_success', python_callable=_notify_success)
    notify_warning = PythonOperator(task_id='notify_warning', python_callable=_notify_warning)

    (start
     >> [validate_data, check_schema]
     >> spark_load
     >> spark_clean
     >> [spark_sql_analysis, spark_partition_write]
     >> generate_report
     >> check_quality
     >> [notify_success, notify_warning]
     >> end)


@dag_decorator(
    dag_id='popular_people_taskflow',
    default_args={'owner': 'student', 'retries': 2, 'retry_delay': timedelta(minutes=3)},
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['spark', 'etl', 'popular_people', 'taskflow'],
)
def popular_people_taskflow():
    @task
    def validate_data() -> dict:
        return _do_validate()

    @task
    def spark_load(validation: dict) -> dict:
        print(f'Валидация: {validation}')
        return _do_spark_load()

    @task
    def spark_clean(load_info: dict) -> int:
        print(f'Жүктелген жол: {load_info["row_count"]}')
        return _do_spark_clean()

    spark_clean(spark_load(validate_data()))


popular_people_taskflow()
