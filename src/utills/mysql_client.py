import os
import pymysql
import pandas as pd
from dotenv import load_dotenv

class MySQLClient:
    """
    MySQLデータベースとの接続・データ取得を担当するクラス
    """

    def __init__(self):
        load_dotenv()
        self.connection = pymysql.connect(
            host=os.getenv('MYSQL_HOST'),
            port=int(os.getenv('MYSQL_PORT', 3306)),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        """
        指定テーブルをDataFrameとして取得
        """
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, self.connection)
        print(f"✓ MySQLから{len(df)}行を取得")
        return df

    def close(self):
        """
        接続をクローズ
        """
        if self.connection:
            self.connection.close()
            print("✓ MySQL接続をクローズ")
