import os
import psycopg2
import glob

APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'
MERGED_OUTPUT_DIR = os.path.join(APP_ROOT_DIR, 'merged_output')
LOG_FILE = os.path.join(APP_ROOT_DIR, 'scripts', 'imported_notes_files.log')

DB_HOST = "localhost"
DB_NAME = "nagashin"
DB_USER = "postgres"
DB_PASSWORD = "x5WU7Xb3"  # ← 本番では .env に移してね🐻

def load_imported_files():
    if not os.path.exists(LOG_FILE):
        return set()
    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_imported_file(filename):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{filename}\n")

def save_csvs_to_postgres():
    imported_files = load_imported_files()

    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    csv_files = glob.glob(os.path.join(MERGED_OUTPUT_DIR, '*_merged.csv'))
    if not csv_files:
        print("📂 マージ済みCSVファイルが見つかりません。")
        return

    print(f"📥 {len(csv_files)} 件のファイルを確認中...")

    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        if filename in imported_files:
            print(f"  ⏭️ スキップ: {filename}（既に取り込み済み）")
            continue

        try:
            print(f"  ⏳ インポート中: {filename}")
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                cur.copy_expert(
                    sql="COPY notes_receivable FROM STDIN WITH CSV",
                    file=f
                )
            conn.commit()
            print(f"  ✅ インポート成功: {filename}")
            save_imported_file(filename)
        except Exception as e:
            conn.rollback()
            print(f"  ❌ エラー: {filename} のインポートに失敗しました。エラー内容: {e}")

    cur.close()
    conn.close()
    print("🎉 全CSVのインポート処理が完了しました。")

if __name__ == "__main__":
    save_csvs_to_postgres()
