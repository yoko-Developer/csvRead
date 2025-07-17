import pandas as pd
import os
import re
from datetime import datetime
import random 

# --- 設定項目（ここだけ、くまちゃんの環境に合わせて修正してね！） ---
# AIReadが出力したCSVファイルが保存されているルートフォルダ (池上, 中島, 唐木フォルダがある場所)
# 例: r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv'
INPUT_BASE_DIR = r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv' 

# 加工後のCSVファイルを保存するルートフォルダ
# 例: r'C:\Users\User26\yoko\dev\csvRead\output'
OUTPUT_BASE_DIR = r'C:\Users\User26\yoko\dev\csvRead\output' 

# マスタデータファイルが保存されているフォルダ
# 例: r'C:\AIRead\master_data'
MASTER_DATA_DIR = r'C:\AIRead\master_data' # <--- ここをくまちゃんがマスタを保存したパスに合わせてね！

# PostgreSQLの最終形に必要な全てのカラム名をリストで定義
FINAL_POSTGRE_COLUMNS = [
    'ocr_result_id', 'page_no', 'id', 'jgroupid_string', 'cif_number', 'settlement_at',
    'maker_name_original', 'maker_name', 'maker_com_code',
    'issue_date_rightside_date', 'issue_date',
    'due_date_rightside_date', 'due_date',
    'balance_rightside', 'balance',
    'payment_bank_name_rightside', 'payment_bank_name',
    'payment_bank_branch_name_rightside', 'payment_bank_branch_name',
    'description_rightside', 'description'
]

# --- 各CSVファイル形式ごとのマッピングルールを定義 ---
# これがExcelが内部的に持つ「変換レシピ」をPythonで明示的に定義する部分

# 各マッピング辞書は (PostgreSQLの目標カラム名 : 元のCSVのヘッダー名 または 列インデックス) の形式
# maker_name, issue_date, due_date, balance, payment_bank_name, payment_bank_branch_name, description_rightside, description
# を中心にマッピングを定義し、これらの派生カラムも適切に埋める。

# 1. 手形情報形式のCSV (例: "振出人", "振出年月日", "金額" など)
HAND_BILL_MAPPING_DICT = {
    'maker_name': '振出人',
    'issue_date': '振出年月日',
    'due_date': '支払期日',
    'payment_bank_name': '支払銀行名称',
    'payment_bank_branch_name': '支払銀行支店名',
    'balance': '金額',
    'description_rightside': '割引銀行名及び支店名等', 
    'description': '摘要' 
}

# 2. 財務諸表 (勘定科目と金額) 形式のCSV (例: "account", "amount_0", "amount_1" など)
#    - PostgreSQLのカラムに意味的に合わないデータが入ることを許容し、可能な限り埋める
FINANCIAL_STATEMENT_MAPPING_DICT = {
    'maker_name': 'account', # 勘定科目をmaker_nameに
    'issue_date': 'amount_0', # amount_0をissue_dateに
    'balance': 'amount_0',    # amount_0をbalanceに
    'due_date': 'amount_1',   # amount_1をdue_dateに
    'description': 'amount_2' # amount_2をdescriptionに
}

# 3. 借入金明細形式のCSV (例: "借入先名称(氏名)", "期末現在高" など)
LOAN_DETAILS_MAPPING_DICT = {
    'maker_name': '借入先名称(氏名)',
    'issue_date': '借入先所在地(住所)', # 住所を日付にマッピング
    'balance': '期末現在高',           
    'description_rightside': '期中の支払利子額', 
    'description': '利率',            
}

# 4. ヘッダーなしのCSV (最初の行からデータが始まる)
#    - マッピング元は列インデックス (0始まり)
#    - このマッピングは、特定のヘッダーが見つからない場合の「汎用」マッピング
#    - これは、くまちゃんが手動で貼り付けた「振出人」で始まるヘッダーなしCSV例をベース
NO_HEADER_MAPPING_DICT = {
    'maker_name': 0, 
    'issue_date': 1, 
    'due_date': 2,   
    'payment_bank_name': 3, 
    'payment_bank_branch_name': 4, 
    'balance': 5,    
    'description_rightside': 6, 
    'description': 7, 
}


# --- 関数定義（この部分は変更しないでね！） ---
# ocr_result_id の通し番号を保持するためのグローバル変数
# 実行ごとに初期化される。複数回実行する場合は、この値をファイルに保存して読み込むロジックを追加検討。
current_ocr_id_sequence = 0 

def get_next_ocr_id():
    """ocr_result_idを1からの自動採番で取得する（4桁で終わる）"""
    global current_ocr_id_sequence
    current_ocr_id_sequence += 1
    # 「4桁で終わる」という要件を満たすために、連番を4桁でゼロ埋め。
    # 全体のIDの長さは固定ではないが、一意性は担保される。
    # Excelの例のIDは17桁固定だが、Excelのコメント「ただ採番」と「4桁で終わる」を優先。
    return f"{datetime.now().strftime('%Y%m%d%H%M%S')}{str(current_ocr_id_sequence).zfill(4)}"


def process_universal_csv(input_filepath, output_base_dir, input_base_dir, 
                          maker_master_df, jgroupid_master_df, max_source_cols, 
                          final_postgre_columns_list, hand_bill_map, financial_map, loan_map, no_header_map): # 引数追加
    """
    全てのAIRead出力CSVファイルを読み込み、統一されたPostgreSQL向けカラム形式に変換して出力する関数。
    CSVの種類（ヘッダー内容）を判別し、それぞれに応じたマッピングを適用する。
    """
    df_original = None
    file_type = "不明" # ファイルタイプを判別するための変数

    try:
        # 1. ファイルの最初の数行を読み込み、ヘッダーの有無と内容を判別
        first_line_content = ""
        
        # 試行するエンコーディングリスト (UTF-8を優先)
        encodings_to_try = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']

        for enc in encodings_to_try:
            try:
                # header=0 (ヘッダーあり)で読み込みを試す
                df_temp = pd.read_csv(input_filepath, encoding=enc, header=0, nrows=1) # ヘッダー判定用に1行だけ読み込む
                headers_str = " ".join(df_temp.columns.fillna('').astype(str).values)
                
                # ヘッダーを自動判別し、適切な設定でpd.read_csvを呼び出す
                if ('"振出人"' in headers_str) or ('振出人,' in headers_str and not headers_str.startswith('account') and not headers_str.startswith('借入先名称(氏名)')):
                    df_original = pd.read_csv(input_filepath, encoding=enc, header=0)
                    file_type = "手形情報"
                elif ('"account"' in headers_str) or ('account,' in headers_str):
                    df_original = pd.read_csv(input_filepath, encoding=enc, header=0)
                    file_type = "財務諸表"
                elif ('"借入先名称(氏名)"' in headers_str) or ('借入先名称(氏名),' in headers_str):
                    df_original = pd.read_csv(input_filepath, encoding=enc, header=0)
                    file_type = "借入金明細"
                else:
                    # 明確なヘッダーが見つからない場合、header=None (ヘッダーなし)で読み込み直す
                    df_original = pd.read_csv(input_filepath, encoding=enc, header=None)
                    file_type = "汎用データ_ヘッダーなし"
                
                print(f"  ファイル {os.path.basename(input_filepath)} を {enc} ({file_type}) で読み込み成功。")
                break # 読み込みに成功したらループを抜ける
            except Exception as e_inner: # 読み込み失敗時は次のエンコーディングを試す
                print(f"  ファイル {os.path.basename(input_filepath)} を {enc} で読み込み失敗。別のエンコーディング/ヘッダー設定を試します。エラー: {e_inner}")
                df_original = None 
                continue 

        if df_original is None or df_original.empty:
            raise ValueError(f"ファイル {os.path.basename(input_filepath)} をどのエンコーディングとヘッダー設定でも読み込めませんでした。")
        
        print(f"  ファイル {os.path.basename(input_filepath)} は '{file_type}' として処理します。")

    except Exception as e:
        print(f"❌ エラー発生（{input_filepath}）: CSV読み込みまたはファイルタイプ判別で問題が発生しました。エラー: {e}")
        import traceback
        traceback.print_exc()
        return

    # --- データ加工処理 ---
    df_processed = pd.DataFrame() 
    
    # --- 共通項目 (PostgreSQLのグリーンの表の左側に来る、自動生成項目) の生成 ---
    # ocr_result_id: 1からの自動採番で、4桁で終わる
    ocr_result_id = get_next_ocr_id() 
    df_processed['ocr_result_id'] = ocr_result_id


    # page_no: 何でもよい（1で固定）の要件に従う
    df_processed['page_no'] = 1 

    # id: ファイルの中でカウントアップ (各行にユニークなID)
    df_processed['id'] = range(1, len(df_original) + 1)

    # jgroupid_string: jgroupid_masterからランダムに1つ選択
    jgroupid_string = "000" # デフォルト値
    if not jgroupid_master_df.empty and 'jgroupid' in jgroupid_master_df.columns:
        jgroupid_string = random.choice(jgroupid_master_df['jgroupid'].tolist())
    df_processed['jgroupid_string'] = jgroupid_string

    # cif_number: ランダムな数字列（6桁の例）
    cif_number = str(random.randint(100000, 999999))
    df_processed['cif_number'] = cif_number

    # settlement_at: yyyyMM形式で何でもよい
    settlement_at = datetime.now().strftime('%Y%m') # YYYYMM形式
    df_processed['settlement_at'] = settlement_at

    # PostgreSQLの最終形に必要な全てのカラムを空で初期化
    # final_postgre_columns_list は引数として渡される
    
    # 自動生成された6項目以外のPostgreSQLカラムを空で初期化
    for pg_col in final_postgre_columns_list[6:]: 
        df_processed[pg_col] = '' 

    # --- 各ファイルタイプに応じたマッピングルールを適用 ---
    
    # 使用するマッピング辞書を決定
    mapping_to_use = {}
    if file_type == "手形情報":
        mapping_to_use = hand_bill_map
    elif file_type == "財務諸表": 
        mapping_to_use = financial_map
    elif file_type == "借入金明細": 
        mapping_to_use = loan_map
    else: # "汎用データ_ヘッダーなし"
        mapping_to_use = no_header_map


    # df_processed にマッピングされたデータを格納
    for pg_col_name, src_ref in mapping_to_use.items():
        if isinstance(src_ref, str): # 元がヘッダー名の場合
            if src_ref in df_original.columns:
                df_processed[pg_col_name] = df_original[src_ref].fillna('').astype(str)
            else: # 元のCSVにヘッダーが存在しない場合は空に (初期化されているが念のため)
                df_processed[pg_col_name] = ''
        elif isinstance(src_ref, int): # 元が列インデックスの場合
            if src_ref < df_original.shape[1]:
                df_processed[pg_col_name] = df_original.iloc[:, src_ref].fillna('').astype(str)
            else: # 元のCSVに列が存在しない場合は空に (初期化されているが念のため)
                df_processed[pg_col_name] = ''
        else: # マッピングルールが不正な場合など
            df_processed[pg_col_name] = ''


    # --- Excel関数相当のロジックを適用（派生カラムの生成） ---

    # maker_name_original は maker_name と同じ
    df_processed['maker_name_original'] = df_processed['maker_name'].fillna('').astype(str)
    
    # maker_com_code (VLOOKUP相当)
    if 'maker_name' in df_processed.columns and not maker_master_df.empty:
        temp_maker_df = df_processed[['maker_name']].copy() 
        temp_maker_df = pd.merge(temp_maker_df, maker_master_df[['会社名', '会社コード']],
                                   left_on='maker_name', right_on='会社名', how='left', suffixes=('', '_master'))
        df_processed['maker_com_code'] = temp_maker_df['会社コード'].fillna('').astype(str) # 会社コードに修正
    else:
        df_processed['maker_com_code'] = ''

    # issue_date_rightside_date, due_date_rightside_date, balance_rightside など
    # Excelの例だとそれぞれ対応するカラムと同じ値
    df_processed['issue_date_rightside_date'] = df_processed['issue_date'].fillna('').astype(str)
    df_processed['due_date_rightside_date'] = df_processed['due_date'].fillna('').astype(str)
    df_processed['balance_rightside'] = df_processed['balance'].fillna('').astype(str)
    df_processed['payment_bank_name_rightside'] = df_processed['payment_bank_name'].fillna('').astype(str)
    df_processed['payment_bank_branch_name_rightside'] = df_processed['payment_bank_branch_name'].fillna('').astype(str)
    # description_rightside は description と同じ (もしExcelでそうなら)
    # description_rightsideとdescriptionは別々にマッピングされたので、そのまま。

    # 最終的な列の順序をPostgreSQLの目標形式に合わせる
    df_processed = df_processed.reindex(columns=final_postgre_columns_list) # final_postgre_columns_list は引数として渡される

    # --- 保存処理 ---
    # 出力先のサブフォルダを元のフォルダ構造に合わせて作成
    relative_path_to_file = os.path.relpath(input_filepath, input_base_dir)
    relative_dir_to_file = os.path.dirname(relative_path_to_file)
    output_sub_dir = os.path.join(output_base_dir, relative_dir_to_file)
    os.makedirs(output_sub_dir, exist_ok=True)

    # 加工後のCSVを保存
    output_filename = os.path.basename(input_filepath).replace('.csv', '_processed.csv')
    output_filepath = os.path.join(output_sub_dir, output_filename)
    df_processed.to_csv(output_filepath, index=False, encoding='utf-8-sig')

    print(f"✅ 加工完了: {input_filepath} -> {output_filepath}")

# --- メイン処理（この部分も変更しないでね！） ---
if __name__ == "__main__":
    print(f"--- 処理開始: {datetime.now()} ---")

    # 出力フォルダがなければ作成
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True) 

    # マスタデータ読み込み
    MASTER_DATA_DIR = r'C:\AIRead\master_data' # <--- ここをくまちゃんがマスタを保存したパスに合わせてね！

    # maker_master.csv を読み込む
    maker_master_filepath = os.path.join(MASTER_DATA_DIR, 'master.csv') # <<-- ファイル名を 'master.csv' に修正済み！
    maker_master_df = pd.DataFrame() 
    if os.path.exists(maker_master_filepath):
        try:
            maker_master_df = pd.read_csv(maker_master_filepath, encoding='utf-8') # <<-- エンコーディングをUTF-8に修正！
        except Exception as e:
            print(f"❌ エラー: master.csv の読み込みに失敗しました。エンコーディングを確認してください。エラー: {e}")
            maker_master_df = pd.DataFrame({'会社名': [], '会社コード': []}) # 空のDataFrameで継続
    else:
        print(f"⚠️ 警告: master.csv が見つかりません。パスを確認してください: {maker_master_filepath}")
        maker_master_data = {
            '会社名': ['(株)双文社印刷', '(株)太平印刷社', '(株)リーブルテック', '日本ハイコム(株)', '(株)新寿堂', '手持手形計', '割引手形計', '(株)シーフォース'],
            '会社コード': ['4380946945', '9138429316', '2578916640', '5408006886', '0668992415', '9443492307', '4417864013', '7398659210']
        }
        maker_master_df = pd.DataFrame(maker_master_data)

    # jgroupid_master.csv を読み込む
    jgroupid_master_filepath = os.path.join(MASTER_DATA_DIR, 'jgroupid_master.csv')
    jgroupid_master_df = pd.DataFrame() 
    if os.path.exists(jgroupid_master_filepath): 
        try:
            jgroupid_master_df = pd.read_csv(jgroupid_master_filepath, encoding='utf-8') # <<-- エンコーディングをUTF-8に修正！
        except Exception as e:
            print(f"❌ エラー: jgroupid_master.csv の読み込みに失敗しました。エンコーディングを確認してください。エラー: {e}")
            jgroupid_master_df = pd.DataFrame({'jgroupid': []}) # 空のDataFrameで継続
    else:
        print(f"⚠️ 警告: jgroupid_master.csv が見つかりません。パスを確認してください: {jgroupid_master_filepath}")
        jgroupids = [f"{i:03d}" for i in range(1, 94)] 
        jgroupid_master_df = pd.DataFrame({'jgroupid': jgroupids})


    # INPUT_BASE_DIR内の全てのCSVファイルを処理
    for root, dirs, files in os.walk(INPUT_BASE_DIR):
        for filename in files:
            if filename.lower().endswith('.csv'): 
                input_filepath = os.path.join(root, filename)
                # 全てのファイルを共通のプロセス関数で処理
                process_universal_csv(input_filepath, OUTPUT_BASE_DIR, INPUT_BASE_DIR, 
                                      maker_master_df, jgroupid_master_df, MAX_SOURCE_COLUMNS, 
                                      FINAL_POSTGRE_COLUMNS, NO_HEADER_MAPPING_DICT, HAND_BILL_MAPPING_DICT, 
                                      FINANCIAL_STATEMENT_MAPPING_DICT, LOAN_DETAILS_MAPPING_DICT) # 引数名修正
                #                               ^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    