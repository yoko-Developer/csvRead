import os
import re
import shutil # ファイルコピーのためにshutilモジュールを使用します

# --- 設定項目（ここだけ、くまちゃんの環境に合わせて修正してね！） ---
# 検索対象となるCSVファイルがあるルートフォルダ (池上, 中島, 唐木フォルダがある場所)
# 新しいパスに修正済み！
INPUT_BASE_DIR = r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv' 

# アプリのルートフォルダ (GitHubリポジトリのルート)
# C:\Users\User26\yoko\dev\csvRead を指定
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'

# 検索条件に合致したオリジナルファイルをコピーして保存するルートフォルダ
# APP_ROOT_DIR の下の filtered_originals フォルダ
OUTPUT_COPY_DIR = os.path.join(APP_ROOT_DIR, 'filtered_originals')

# 検索するファイル名のパターン (正規表現)
# 'B' で始まり、任意の文字が続き、'020.csv' で終わる (大文字小文字を区別しない)
SEARCH_PATTERN = r'^B.*020\.csv$'

# --- メイン処理（この部分は変更しないでね！） ---
if __name__ == "__main__":
    print(f"--- ファイル検索とコピー処理 開始 ---")

    # 出力フォルダがなければ作成
    os.makedirs(OUTPUT_COPY_DIR, exist_ok=True) 

    found_files_count = 0

    # INPUT_BASE_DIR内の全てのCSVファイルを検索
    for root, dirs, files in os.walk(INPUT_BASE_DIR):
        for filename in files:
            # ファイル名が検索パターンに合致するかチェック (大文字小文字を区別しない)
            if re.match(SEARCH_PATTERN, filename, re.IGNORECASE): 
                input_filepath = os.path.join(root, filename)
                
                # コピー先のパスを決定 (元のフォルダ構造を維持)
                # 例: G:\...Import\中島\B001020.csv の相対パス \中島\B001020.csv を
                # C:\Users\User26\yoko\dev\csvRead\filtered_originals\中島\B001020.csv にコピー
                relative_path_from_input = os.path.relpath(input_filepath, INPUT_BASE_DIR)
                output_copy_filepath = os.path.join(OUTPUT_COPY_DIR, relative_path_from_input)
                
                # コピー先のディレクトリが存在しない場合は作成
                os.makedirs(os.path.dirname(output_copy_filepath), exist_ok=True)
                
                try:
                    shutil.copy2(input_filepath, output_copy_filepath) # ファイルをコピー
                    print(f"✅ コピー成功: {input_filepath} -> {output_copy_filepath}")
                    found_files_count += 1
                except Exception as e:
                    print(f"❌ コピー失敗 ({input_filepath}): {e}")
                
    print(f"\n--- ファイル検索とコピー処理 完了 ---")
    print(f"合計 {found_files_count} 個のファイルがコピーされました。")
    