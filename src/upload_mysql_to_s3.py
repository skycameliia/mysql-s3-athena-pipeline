from utils.mysql_client import MySQLClient
from utils.s3_client import S3Client

def upload_mysql_to_s3(table_name: str):
    """
    MySQL → S3 (Parquet) の統合処理
    """
    mysql_client = MySQLClient()
    s3_client = S3Client()
    try:
        df = mysql_client.fetch_table(table_name)
        s3_key = s3_client.upload_dataframe_as_parquet(df, table_name)
        if s3_key:
            print(f"✓ 完了: s3://{s3_client.bucket}/{s3_key}")
        else:
            print("✗ アップロードに失敗しました")
    finally:
        mysql_client.close()
