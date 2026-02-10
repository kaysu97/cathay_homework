import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from common.utils import send_failure_email, validate_file, SOURCE_DIR, SUCCESS_DIR, LOCAL_TZ


default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False, # 使用 callback 自定義
    'retries': 0, # 失敗直接告警，不重試 (依需求可調整)
    'on_failure_callback': send_failure_email, # 綁定失敗告警
    'email': ['admin@example.com']
}



NEXT_EXECUTION_DATE = "{{ data_interval_end.in_timezone('Asia/Taipei').strftime('%Y%m%d') }}"

with DAG(
    dag_id='daily_file_processor',
    default_args=default_args,
    description='Process daily record files with validation',
    schedule_interval='0 0 * * *', # 每天 00:00
    start_date=datetime(2026, 2, 5, 0, 0, tzinfo=LOCAL_TZ), # 如果今天是 2/8，前兩天的資料檔名是 2/7 和 2/6，，但實際上 logical date 會是 2/5, 2/6，故設定為 2/5 開始(觸發回溯)，也可設定用 days_ago(3)，但不建議。
    catchup=True, # 開啟backfill
    tags=['etl', 'validation'],
) as dag:

    # 1. 等待檔案
    # timeout: 30分鐘 (1800秒)，若未收到則失敗
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=os.path.join(SOURCE_DIR, f'record_{NEXT_EXECUTION_DATE}.txt'),
        poke_interval=600, # 每 10 分鐘檢查一次
        timeout=30 * 60,  # 30 分鐘超時
        mode='reschedule',
        soft_fail=False   # 超時視為失敗 (觸發告警)
    )

    # 2. 驗證資料
    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_file,
        templates_dict={
            'file_date': NEXT_EXECUTION_DATE
        },
        provide_context=True
    )

    # 3. 移動檔案
    # 移動成功後檔案進入 /tmp/success/
    move_file = BashOperator(
        task_id='move_file',
        bash_command="mv " + os.path.join(SOURCE_DIR, f'record_{NEXT_EXECUTION_DATE}.txt') + " " + SUCCESS_DIR,
    )


    wait_for_file >> validate_data >> move_file