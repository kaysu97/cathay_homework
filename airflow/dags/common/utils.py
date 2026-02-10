import os
import pendulum
import logging
import pandas as pd
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

# 設定時區
LOCAL_TZ = pendulum.timezone("Asia/Taipei")

# 設定路徑
SOURCE_DIR = "/tmp/test"
SUCCESS_DIR = "/tmp/success"

def send_failure_email(context):
    """
    任務失敗時觸發的 callback
    """
    ti = context['task_instance']

    task_id = ti.task_id
    email = ti.task.email[0]
    execution_date = context['data_interval_end'].in_timezone(LOCAL_TZ).strftime('%Y%m%d')
    exception = context.get('exception')

    subject = f"[Alert] Airflow Task Failed: {task_id}"
    html_content = f"""
    <h3>Task Failure Alert</h3>
    <ul>
        <li><b>Task ID:</b> {task_id}</li>
        <li><b>Run Date:</b> {execution_date}</li>
        <li><b>Exception:</b> {exception}</li>
    </ul>
    """
    # 這裡的收件人可以設為環境變數或固定值
    send_email(to=[email], subject=subject, html_content=html_content)
    logger.info(f"Email sent to: {email}")


def validate_file(**context):
    """
    驗證檔案內容：大小、筆數、ID唯一性
    """
    # 取得當天日期的檔名
    file_date = context.get('templates_dict').get('file_date')
    file_name = f"record_{file_date}.txt"
    file_path = os.path.join(SOURCE_DIR, file_name)

    logger.info(f"Validating file: {file_path}")

    # 1. 檢查檔案大小 > 50 bytes
    file_size = os.path.getsize(file_path)
    if file_size <= 50:
        raise ValueError(f"File size too small ({file_size} bytes). Must be > 50 bytes.")

    # 讀取檔案內容進行檢查
    try:
        # 假設第一行是 Header
        df = pd.read_csv(file_path, header=0)
    except Exception as e:
        raise ValueError(f"Failed to read file with pandas: {str(e)}")

    # 2. 檢查 Header 以外資料筆數 > 0
    if df.empty:
        raise ValueError("File contains no data rows (only header or empty).")

    # 3. 檢查 ID 是否重複
    id_df = df.id
    if id_df.duplicated().any():
        duplicated_ids = df[id_df.duplicated()].id.unique()
        raise ValueError(f"Duplicate ID found: {duplicated_ids}")

    logger.info("Validation passed successfully.")
