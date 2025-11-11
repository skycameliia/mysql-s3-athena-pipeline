# mysql-to-s3-athena

MySQLデータをS3にアップロードし、Amazon Athenaで分析するためのPythonパイプラインです。

## 概要

このプロジェクトは、MySQLデータベースからデータを抽出し、S3バケットに保存した後、Amazon Athenaを使用してSQLクエリを実行できるデータパイプラインを提供します。

## 主な機能

- MySQLデータベースからのデータ抽出
- S3へのデータアップロード（CSV/Parquet形式）
- Athenaテーブルの自動作成
- Athenaクエリの実行と結果取得
- クエリ結果のローカル保存

## ディレクトリ構成

```
mysql-to-s3-athena/
├── README.md
├── LICENSE
├── .gitignore
├── requirements.txt
├── .env.example          # 接続情報のサンプル
├── src/
│   ├── upload_mysql_to_s3.py     # MySQL→S3アップロード
│   ├── run_athena_query.py       # Athenaクエリ実行
│   ├── utils/
│   │   ├── s3_client.py          # S3操作用クライアント
│   │   ├── athena_client.py      # Athena操作用クライアント
│   │   └── mysql_client.py       # MySQL接続用クライアント
│   │   └── file_loader.py        # ファイルの読み込みを行うユーティリティ
│   └── main.py                   # メインエントリーポイント
├── sql/
│   └── queries/
│       └── example_query.sql     # サンプルクエリ
├── outputs/
│   └── query_results/            # クエリ結果の保存先
└── docs/
    └── architecture.png          # アーキテクチャ図
```

## 必要要件

- Python 3.8以上
- AWS アカウント（S3、Athena使用可能）
- MySQL データベースへのアクセス権限

## セットアップ

### 1. リポジトリのクローン

```bash
git clone https://github.com/yourusername/mysql-to-s3-athena.git
cd mysql-to-s3-athena
```

### 2. 仮想環境の作成と有効化

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### 3. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 4. 環境変数の設定

`.env.example`をコピーして`.env`ファイルを作成し、必要な情報を入力します。

```bash
cp .env.example .env
```

`.env`ファイルの内容例：

```env
# MySQL接続情報
MYSQL_HOST=your-mysql-host.com
MYSQL_PORT=3306
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=your_database

# AWS接続情報
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-northeast-1

# S3設定
S3_BUCKET_NAME=your-bucket-name
S3_PREFIX=mysql-data/

# Athena設定
ATHENA_DATABASE=your_athena_database
ATHENA_OUTPUT_LOCATION=s3://your-bucket-name/athena-results/
```

## 使用方法

### 基本的な使い方

#### 1. MySQLデータをS3にアップロード

```bash
python src/upload_mysql_to_s3.py --table your_table_name
```

オプション：
- `--table`: アップロードするテーブル名（必須）
- `--format`: 出力形式（csv または parquet、デフォルト: csv）
- `--batch-size`: バッチサイズ（デフォルト: 10000）

#### 2. Athenaクエリの実行

```bash
python src/run_athena_query.py --query-file sql/queries/example_query.sql
```

オプション：
- `--query-file`: 実行するSQLファイルのパス
- `--query`: 直接SQLクエリを指定
- `--output`: 結果の保存先（デフォルト: outputs/query_results/）

#### 3. 統合実行（main.py使用）

```bash
python src/main.py --table users --query-file sql/queries/example_query.sql
```

### SQLクエリの例

`sql/queries/example_query.sql`:

```sql
SELECT 
    CASE 
        WHEN personal_training = 1 THEN 'あり'
        ELSE 'なし'
    END as personal_training_status,
    COUNT(*) as member_count,
    AVG(visit_per_week) as avg_weekly_visits,
    AVG(avg_time_in_gym) as avg_gym_time,
    COUNT(CASE WHEN uses_sauna = 1 THEN 1 END) as sauna_users
FROM gym_membership
GROUP BY personal_training
ORDER BY personal_training DESC;
```

## 主要モジュールの説明

### src/utils/mysql_client.py

MySQL接続とデータ抽出を管理するクライアント。

```python
from src.utils.mysql_client import MySQLClient

client = MySQLClient()
data = client.fetch_table_data('users')
```

### src/utils/s3_client.py

S3へのデータアップロードを管理するクライアント。

```python
from src.utils.s3_client import S3Client

client = S3Client()
client.upload_dataframe(df, 'users.csv')
```

### src/utils/athena_client.py

Athenaクエリの実行と結果取得を管理するクライアント。

```python
from src.utils.athena_client import AthenaClient

client = AthenaClient()
results = client.execute_query('SELECT * FROM users LIMIT 10')
```

## トラブルシューティング

### MySQL接続エラー

- ホスト名、ポート、認証情報が正しいか確認してください
- ファイアウォールの設定を確認してください
- MySQLサーバーが起動しているか確認してください

### AWS認証エラー

- AWS認証情報が正しく設定されているか確認してください
- IAMユーザーに必要な権限（S3、Athena）があるか確認してください

### Athenaクエリがタイムアウトする

- クエリ結果の出力先S3バケットが存在するか確認してください
- Athenaデータベースとテーブルが正しく作成されているか確認してください

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。詳細は[LICENSE](LICENSE)ファイルを参照してください。

## 貢献

プルリクエストを歓迎します。大きな変更の場合は、まずissueを開いて変更内容について議論してください。

## 作成者

qiita --- https://qiita.com/sky_camellia
