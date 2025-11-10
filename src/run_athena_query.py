import os
from dotenv import load_dotenv
from utils.athena_client import AthenaClient


def load_sql_file(sql_file_path: str) -> str:
    """
    SQLファイルを読み込む
    
    Args:
        sql_file_path: SQLファイルのパス
    
    Returns:
        str: SQLクエリ文字列
    
    Raises:
        FileNotFoundError: SQLファイルが見つからない場合
        Exception: その他の読み込みエラー
    """
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        query = f.read()
    
    print(f"✓ SQLファイル読み込み完了: {sql_file_path}")
    return query


def run_query(sql_file: str, output_key: str, database: str = None) -> bool:
    """
    Athenaクエリを実行し、結果をParquet形式で保存
    
    Args:
        sql_file: 実行するSQLファイルのパス
        output_key: 出力先のS3キー
        database: データベース名（Noneの場合は環境変数から取得）
    
    Returns:
        bool: 実行成功時True、失敗時False
    
    Raises:
        ValueError: 必要な環境変数が設定されていない場合
    """
    load_dotenv()
    
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'ATHENA_DATABASE',
        'ATHENA_OUTPUT_LOCATION',
        'S3_BUCKET_NAME'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"必須環境変数が設定されていません: {', '.join(missing_vars)}")
    
    print("=" * 60)
    print("Athenaクエリ実行開始")
    print("=" * 60)
    
    athena_client = AthenaClient(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'ap-northeast-1'),
        database=database or os.getenv('ATHENA_DATABASE'),
        output_location=os.getenv('ATHENA_OUTPUT_LOCATION')
    )
    
    query = load_sql_file(sql_file)
    
    print("\n▶ クエリ実行中...")
    query_execution_id = athena_client.execute_query(query)
    
    if not query_execution_id:
        print("\n✗ クエリ実行失敗")
        return False
    
    print("\n▶ 結果をParquet形式で保存中...")
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    
    success = athena_client.get_query_results_as_parquet(
        query_execution_id=query_execution_id,
        s3_bucket=s3_bucket,
        s3_key=output_key
    )
    
    if success:
        print("\n" + "=" * 60)
        print("✓ クエリ実行処理が完了しました")
        print("=" * 60)
        return True
    else:
        print("\n✗ 結果の保存に失敗しました")
        return False
