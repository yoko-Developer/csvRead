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

def clear_imported_files_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write('')  # ファイル中身を空にする

def save_csvs_to_postgres():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    print("🧹 テーブルとインデックスを初期化中...")
    cur.execute("DROP INDEX IF EXISTS idx_jgroupid_string;")
    cur.execute("DROP TABLE IF EXISTS notes_receivable;")

    cur.execute("""
CREATE TABLE notes_receivable (
    ocr_result_id CHAR(18) NOT NULL,
    page_no INTEGER NOT NULL,
    id INTEGER NOT NULL,
    jgroupid_string VARCHAR(3),
    cif_number VARCHAR(7),
    settlement_at VARCHAR(6),
    registration_number_original TEXT,
    registration_number TEXT,
    maker_name_original TEXT,
    maker_name TEXT,
    maker_com_code TEXT,
    maker_com_code_status_id INTEGER,
    maker_comcd_relation_source_type_id INTEGER,
    maker_exist_comcd_relation_history_id INTEGER,
    issue_date_original TEXT,
    issue_date TEXT,
    due_date_original TEXT,
    due_date TEXT,
    paying_bank_name_original TEXT,
    paying_bank_name TEXT,
    paying_bank_code TEXT,
    paying_bank_branch_name_original TEXT,
    paying_bank_branch_name TEXT,
    balance_original NUMERIC,
    balance NUMERIC,
    discount_bank_name_original TEXT,
    discount_bank_name TEXT,
    discount_bank_code TEXT,
    description_original TEXT,
    description TEXT,
    conf_registration_number INTEGER,
    conf_maker_name INTEGER,
    conf_issue_date INTEGER,
    conf_due_date INTEGER,
    conf_balance INTEGER,
    conf_paying_bank_name INTEGER,
    conf_paying_bank_branch_name INTEGER,
    conf_discount_bank_name INTEGER,
    conf_description INTEGER,
    coord_x_registration_number NUMERIC,
    coord_y_registration_number NUMERIC,
    coord_h_registration_number NUMERIC,
    coord_w_registration_number NUMERIC,
    coord_x_maker_name NUMERIC,
    coord_y_maker_name NUMERIC,
    coord_h_maker_name NUMERIC,
    coord_w_maker_name NUMERIC,
    coord_x_issue_date NUMERIC,
    coord_y_issue_date NUMERIC,
    coord_h_issue_date NUMERIC,
    coord_w_issue_date NUMERIC,
    coord_x_due_date NUMERIC,
    coord_y_due_date NUMERIC,
    coord_h_due_date NUMERIC,
    coord_w_due_date NUMERIC,
    coord_x_balance NUMERIC,
    coord_y_balance NUMERIC,
    coord_h_balance NUMERIC,
    coord_w_balance NUMERIC,
    coord_x_paying_bank_name NUMERIC,
    coord_y_paying_bank_name NUMERIC,
    coord_h_paying_bank_name NUMERIC,
    coord_w_paying_bank_name NUMERIC,
    coord_x_paying_bank_branch_name NUMERIC,
    coord_y_paying_bank_branch_name NUMERIC,
    coord_h_paying_bank_branch_name NUMERIC,
    coord_w_paying_bank_branch_name NUMERIC,
    coord_x_discount_bank_name NUMERIC,
    coord_y_discount_bank_name NUMERIC,
    coord_h_discount_bank_name NUMERIC,
    coord_w_discount_bank_name NUMERIC,
    coord_x_description NUMERIC,
    coord_y_description NUMERIC,
    coord_h_description NUMERIC,
    coord_w_description NUMERIC,
    row_no SMALLINT,
    insertdatetime TIMESTAMP,
    updatedatetime TIMESTAMP,
    updateuser TEXT,
    PRIMARY KEY (ocr_result_id, page_no, id)
);
""")

    cur.execute("CREATE INDEX idx_jgroupid_string ON notes_receivable(jgroupid_string);")
    conn.commit()
    print("✅ テーブルとインデックスの初期化が完了しました。")

    # ここでログファイルを空にしちゃう
    clear_imported_files_log()
    print("🧹 取り込みログファイルをクリアしました。")

    imported_files = load_imported_files()
    csv_files = glob.glob(os.path.join(MERGED_OUTPUT_DIR, '*_merged.csv'))

    if not csv_files:
        print("📂 マージ済みCSVファイルが見つかりません。")
        cur.close()
        conn.close()
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
