import os
import boto3
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd

class S3Client:
    """
    AWS S3へのアップロード処理を担当するクラス
    """

    def __init__(self):
        load_dotenv()
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'ap-northeast-1')
        )
        self.bucket = os.getenv('S3_BUCKET_NAME')

    def upload_dataframe_as_parquet(self, df: pd.DataFrame, table_name: str) -> str:
        """
        DataFrameをParquet形式に変換してS3にアップロード
        """
        try:
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"data/{table_name}/{timestamp}.parquet"

            response = self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/x-parquet'
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"✓ アップロード成功: s3://{self.bucket}/{s3_key}")
                return s3_key
            else:
                raise Exception("S3アップロード失敗")

        except NoCredentialsError:
            print("✗ AWS認証情報が見つかりません")
        except ClientError as e:
            print(f"✗ AWS接続エラー: {e}")
        except Exception as e:
            print(f"✗ アップロードエラー: {e}")
        return ""
