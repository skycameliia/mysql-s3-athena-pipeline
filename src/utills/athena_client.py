import boto3
import time
import pandas as pd
from io import BytesIO
from typing import Optional


class AthenaClient:
    """
    Amazon Athenaとの連携を行うクライアントクラス
    
    Attributes:
        client: boto3のAthenaクライアント
        s3_client: boto3のS3クライアント
        database: デフォルトのデータベース名
        output_location: クエリ結果の出力先S3パス
    """
    
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str,
                 region_name: str, database: str, output_location: str):
        """
        AthenaClientの初期化
        
        Args:
            aws_access_key_id: AWSアクセスキーID
            aws_secret_access_key: AWSシークレットアクセスキー
            region_name: AWSリージョン
            database: Athenaデータベース名
            output_location: クエリ結果の出力先S3パス
        """
        self.client = boto3.client(
            'athena',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.database = database
        self.output_location = output_location
    
    def execute_query(self, query: str, database: Optional[str] = None) -> Optional[str]:
        """
        Athenaクエリを実行し、完了まで待機
        
        Args:
            query: 実行するSQLクエリ
            database: データベース名（Noneの場合はデフォルト使用）
        
        Returns:
            str: クエリ実行ID（成功時）
            None: 失敗時
        """
        db_name = database or self.database
        
        try:
            response = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': db_name},
                ResultConfiguration={'OutputLocation': self.output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            print(f"✓ クエリ実行開始: {query_execution_id}")
            
            return self.wait_for_query_completion(query_execution_id)
            
        except Exception as e:
            print(f"✗ クエリ実行エラー: {str(e)}")
            return None
    
    def wait_for_query_completion(self, query_execution_id: str) -> Optional[str]:
        """
        クエリの完了を待機
        
        Args:
            query_execution_id: クエリ実行ID
        
        Returns:
            str: クエリ実行ID（成功時）
            None: 失敗時
        """
        while True:
            status = self.client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            state = status['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                print("✓ クエリ実行成功")
                return query_execution_id
                
            elif state in ['FAILED', 'CANCELLED']:
                reason = status['QueryExecution']['Status'].get(
                    'StateChangeReason', '不明'
                )
                print(f"✗ クエリ実行失敗: {reason}")
                return None
            
            time.sleep(1)
    
    def get_query_results_as_parquet(self, query_execution_id: str,
                                     s3_bucket: str, s3_key: str) -> bool:
        """
        クエリ結果をParquet形式でS3に保存
        
        Args:
            query_execution_id: クエリ実行ID
            s3_bucket: 保存先S3バケット名
            s3_key: 保存先S3キー
        
        Returns:
            bool: 保存成功時True、失敗時False
        """
        try:
            result_response = self.client.get_query_results(
                QueryExecutionId=query_execution_id,
                MaxResults=1000
            )
            
            columns = [
                col['Label']
                for col in result_response['ResultSet']['ResultSetMetadata']['ColumnInfo']
            ]
            
            rows = result_response['ResultSet']['Rows'][1:]
            data = [
                [col.get('VarCharValue', None) for col in row['Data']]
                for row in rows
            ]
            
            df = pd.DataFrame(data, columns=columns)
            print(f"✓ {len(df)}行のデータを取得")
            
            parquet_buffer = BytesIO()
            df.to_parquet(
                parquet_buffer,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            parquet_buffer.seek(0)
            self.s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            
            print(f"✓ Parquetファイル保存完了: s3://{s3_bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"✗ 結果取得エラー: {str(e)}")
            return False
