class FileLoader:
    """
    各種ファイル読み込みを扱うユーティリティ
    """

    @staticmethod
    def load_sql_file(sql_file_path: str) -> str:
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                query = f.read()
            print(f"✓ SQLファイル読み込み完了: {sql_file_path}")
            return query
        except FileNotFoundError:
            raise FileNotFoundError(f"✗ SQLファイルが見つかりません: {sql_file_path}")
        except Exception as e:
            raise Exception(f"✗ SQLファイルの読み込みに失敗: {e}")
