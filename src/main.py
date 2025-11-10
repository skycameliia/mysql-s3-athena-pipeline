#!/usr/bin/env python3
import sys
import os
import argparse
from pathlib import Path
from run_athena_query import run_query
from upload_mysql_to_s3 import upload_mysql_to_s3
from dotenv import load_dotenv

def validate_sql_file(sql_path: str) -> str:
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQLファイルが見つかりません: {sql_path}")
    if not sql_path.endswith('.sql'):
        raise ValueError(f"SQLファイル（.sql）を指定してください: {sql_path}")
    return os.path.abspath(sql_path)

def generate_output_key(sql_path: str) -> str:
    filename = Path(sql_path).stem
    return f"outputs/query_results/{filename}.parquet"

def main():
    load_dotenv()
    s3_bucket = os.getenv("S3_BUCKET_NAME", "<bucket>")

    parser = argparse.ArgumentParser(
        description='MySQL→S3アップロード & Athenaクエリ実行 CLI'
    )

    parser.add_argument('--mysql-table', type=str, help='S3にアップロードするMySQLテーブル名')
    parser.add_argument('--sql-file', type=str, help='実行するAthena SQLファイルのパス')
    parser.add_argument('--output', type=str, help='Athenaクエリ結果の出力先S3キー')
    parser.add_argument('--database', type=str, help='Athenaデータベース名')

    args = parser.parse_args()
    success = True

    try:
        if args.mysql_table:
            print(f"\n▶ MySQLテーブル '{args.mysql_table}' のS3アップロード開始...")
            upload_success = upload_mysql_to_s3(args.mysql_table)
            success = success and upload_success

        if args.sql_file:
            sql_path = validate_sql_file(args.sql_file)
            print(f"\n▶ Athena SQLファイル: {sql_path}")
            output_key = args.output or generate_output_key(sql_path)
            print(f"▶ 出力先: s3://{s3_bucket}/{output_key}")
            if args.database:
                print(f"▶ データベース: {args.database}")

            query_success = run_query(sql_file=sql_path, output_key=output_key, database=args.database)
            success = success and query_success

        if not args.mysql_table and not args.sql_file:
            print("\n✗ 実行対象が指定されていません。")
            parser.print_help()
            sys.exit(1)

        sys.exit(0 if success else 1)

    except FileNotFoundError as e:
        print(f"\n✗ エラー: {str(e)}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"\n✗ エラー: {str(e)}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ 予期しないエラーが発生しました: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
