import pandas as pd
import os
import re
from datetime import datetime
import random 
import shutil

# --- 設定項目（ここだけ、くまちゃんの環境に合わせて修正してね！） ---
# AIReadが出力したCSVファイルがあるルートフォルダ (池上, 中島, 唐木フォルダがある場所)
# 例: r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv'
INPUT_BASE_DIR = r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv' 

# アプリのルートフォルダ (GitHubリポジトリのルート)
# 例: r'C:\Users\User26\yoko\dev\csvRead'
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'

# 検索結果（B*020.csv）のオリジナルファイルを保存するルートフォルダ
# 例: C:\Users\User26\yoko\dev\csvRead\filtered_originals
SEARCH_RESULT_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'filtered_originals')

# 加工後のCSVファイルを保存するルートフォルダ
# 例: C:\Users\User26\yoko\dev\csvRead\processed_output
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 

# マスタデータファイルが保存されているフォルダ
# 例: C:\Users\User26\yoko\dev\csvRead\master_data
MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data')

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
    'issue_date': 'amount_0', 
    'balance': 'amount_0',    
    'due_date': 'amount_1',   
    'description': 'amount_2' 
}

# 3. 借入金明細形式のCSV (例: "借入先名称(氏名)", "期末現在高" など)
#    - このマッピングは、元のCSVにヘッダーがあることを前提とする
LOAN_DETAILS_MAPPING_DICT = {
    'maker_name': '借入先名称(氏名)',
    'issue_date': '借入先所在地(住所)', 
    'balance': '期末現在高',           
    'description_rightside': '期中の支払利子額', 
    'description': '利率',            
}

# 4. ヘッダーなしのCSV (最初の行からデータが始まる)
#    - マッピング元は列インデックス (0始まり)
#    - このマッピングは、特定のヘッダーが見つからない場合の「汎用」マッピング
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


# --- 関数定義 ---
# ocr_result_id の通し番号を保持するためのグローバル変数
current_ocr_id_sequence = 0 

# maker_com_code の採番を保持するためのグローバル変数
maker_name_to_com_code_map = {}
# ★★★ next_maker_com_code_val は100から開始することで、必ず3桁のコードを生成 ★★★
# 001から099は2桁なので避ける。000は使用しない。
next_maker_com_code_val = 100 

# jgroupid_string の採番を保持するためのグローバル変数
current_jgroupid_index = 0 
jgroupid_values_from_master = [] 


def get_next_ocr_id():
    """ocr_result_idを1から9999までの自動採番で取得する（4桁で終わる）"""
    global current_ocr_id_sequence 
    current_ocr_id_sequence += 1
    return str(current_ocr_id_sequence % 10000).zfill(4)

def get_next_jgroupid_string():
    """jgroupid_stringをjgroupidマスタから連番で取得する（1から93をループ）"""
    global current_jgroupid_index 
    global jgroupid_values_from_master 

    if jgroupid_values_from_master:
        jgroupid_val = jgroupid_values_from_master[current_jgroupid_index % len(jgroupid_values_from_master)]
        current_jgroupid_index += 1
        return str(jgroupid_val).zfill(3) 
    else:
        return "000" # マスタが読めない場合はデフォルト値

def get_maker_com_code_for_name(maker_name):
    """
    maker_nameに基づいて3桁の会社コードを採番・取得する。
    同じmaker_nameには同じコードを割り当てる。
    常に3桁のランダムな数字が生成されるようにする。
    """
    global maker_name_to_com_code_map 
    global next_maker_com_code_val 

    maker_name_str = str(maker_name).strip() 
    if not maker_name_str: 
        return "" 

    if maker_name_str in maker_name_to_com_code_map:
        return maker_name_to_com_code_map[maker_name_str]
    else:
        # ★★★ 新しい3桁コードを生成 (常に3桁になるように next_maker_com_code_val を100から開始し、999まで) ★★★
        # 100から999までの範囲で連番を振り、それ以降は100に戻ることで常に3桁を維持
        new_code_int = next_maker_com_code_val % 1000 
        if new_code_int < 100: # 1000で割った余りが2桁以下になった場合、100から始める
            new_code_int = 100 + new_code_int 
        new_code = str(new_code_int).zfill(3) # 念のためゼロ埋め

        # もしランダムな数字で良いのであれば、以下の行に差し替えも可能
        # new_code = str(random.randint(100, 999)) 
        
        maker_name_to_com_code_map[maker_name_str] = new_code
        next_maker_com_code_val += 1
        return new_code


def process_universal_csv(input_filepath, processed_output_base_dir, input_base_dir, 
                        maker_master_df, 
                        final_postgre_columns_list, no_header_map, hand_bill_map, financial_map, loan_map):
    """
    全てのAIRead出力CSVファイルを読み込み、統一されたPostgreSQL向けカラム形式に変換して出力する関数。
    CSVの種類（ヘッダー内容）を判別し、それぞれに応じたマッピングを適用する。
    """
    df_original = None
    file_type = "不明" 

    try:
        first_line_content = ""
        encodings_to_try = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']

        for enc in encodings_to_try:
            try:
                with open(input_filepath, 'r', encoding=enc, newline='') as f_read_all:
                    all_lines_from_file = f_read_all.readlines()
                
                if not all_lines_from_file:
                    raise ValueError("ファイルが空です。")
                
                first_line_content = all_lines_from_file[0].strip()

                read_header = 0 
                
                if ('"振出人"' in first_line_content) or ('振出人,' in first_line_content):
                    file_type = "手形情報"
                elif ('"account"' in first_line_content) or ('account,' in first_line_content):
                    file_type = "財務諸表"
                elif ('"借入先名称(氏名)"' in first_line_content) or ('借入名称(氏名),' in first_line_content):
                    file_type = "借入金明細"
                else:
                    file_type = "汎用データ_ヘッダーなし"
                    read_header = None 


                df_original = pd.read_csv(input_filepath, encoding=enc, header=read_header)
                print(f"  ファイル {os.path.basename(input_filepath)} を {enc} ({file_type}, header={read_header}) で読み込み成功。")
                break 
            except Exception as e_inner: 
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
    df_processed = pd.DataFrame(columns=final_postgre_columns_list)
    
    df_data_rows = None
    if file_type == "汎用データ_ヘッダーなし":
        df_data_rows = df_original.iloc[1:].copy() 
        df_data_rows.columns = range(df_data_rows.shape[1]) 
    else: 
        df_data_rows = df_original.iloc[0:].copy() 

    if df_data_rows.empty:
        print(f"  警告: ファイル {os.path.basename(input_filepath)} に有効なデータ行が見つからなかったため、加工をスキップします。")
        return 

    num_rows_to_process = len(df_data_rows) 

    # --- 共通項目 (PostgreSQLのグリーンの表の左側に来る、自動生成項目) の生成 ---
    ocr_result_id_val = get_next_ocr_id() 
    df_processed['ocr_result_id'] = [ocr_result_id_val] * num_rows_to_process 

    df_processed['page_no'] = [1] * num_rows_to_process 

    df_processed['id'] = range(1, num_rows_to_process + 1)

    jgroupid_string_val = get_next_jgroupid_string() 
    df_processed['jgroupid_string'] = [jgroupid_string_val] * num_rows_to_process

    cif_number_val = str(random.randint(100000, 999999))
    df_processed['cif_number'] = [cif_number_val] * num_rows_to_process

    settlement_at_val = datetime.now().strftime('%Y%m') 
    df_processed['settlement_at'] = [settlement_at_val] * num_rows_to_process

    for pg_col in final_postgre_columns_list[6:]: 
        df_processed[pg_col] = '' 

    # --- 各ファイルタイプに応じたマッピングルールを適用 ---
    
    mapping_to_use = {}
    if file_type == "手形情報":
        mapping_to_use = hand_bill_map
    elif file_type == "財務諸表": 
        mapping_to_use = financial_map
    elif file_type == "借入金明細": 
        mapping_to_use = loan_map
    else: 
        mapping_to_use = no_header_map

    for pg_col_name, src_ref in mapping_to_use.items():
        if isinstance(src_ref, str): 
            if src_ref in df_data_rows.columns: 
                df_processed[pg_col_name] = df_data_rows[src_ref].fillna('').astype(str).values 
        elif isinstance(src_ref, int): 
            if src_ref < df_data_rows.shape[1]:
                df_processed[pg_col_name] = df_data_rows.iloc[:, src_ref].fillna('').astype(str).values 

    # --- Excel関数相当のロジックを適用（派生カラムの生成） ---

    df_processed['maker_name_original'] = df_processed['maker_name'].fillna('').astype(str)
    
    # maker_com_code の採番ロジック (常に3桁保証)
    df_processed['maker_com_code'] = df_processed['maker_name'].apply(get_maker_com_code_for_name)

    df_processed['issue_date_rightside_date'] = df_processed['issue_date'].fillna('').astype(str)
    df_processed['due_date_rightside_date'] = df_processed['due_date'].fillna('').astype(str)
    df_processed['balance_rightside'] = df_processed['balance'].fillna('').astype(str)
    df_processed['payment_bank_name_rightside'] = df_processed['payment_bank_name'].fillna('').astype(str)
    df_processed['payment_bank_branch_name_rightside'] = df_processed['payment_bank_branch_name'].fillna('').astype(str)

    # --- 保存処理 ---
    relative_path_to_file = os.path.relpath(input_filepath, input_base_dir)
    relative_dir_to_file = os.path.dirname(relative_path_to_file)
    processed_output_sub_dir = os.path.join(processed_output_base_dir, relative_dir_to_file)
    os.makedirs(processed_output_sub_dir, exist_ok=True)

    processed_output_filename = os.path.basename(input_filepath).replace('.csv', '_processed.csv')
    processed_output_filepath = os.path.join(processed_output_sub_dir, processed_output_filename)
    df_processed.to_csv(processed_output_filepath, index=False, encoding='utf-8-sig')

    print(f"✅ 加工完了: {input_filepath} -> {processed_output_filepath}")

# --- メイン処理（この部分も変更しないでね！） ---
if __name__ == "__main__":
    print(f"--- 処理開始: {datetime.now()} ---")

    os.makedirs(PROCESSED_OUTPUT_BASE_DIR, exist_ok=True) 

    MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data') 

    # master.csv の読み込みはmaker_com_codeの生成には使用しないが、読み込み部分は残す
    maker_master_filepath = os.path.join(MASTER_DATA_DIR, 'master.csv') 
    maker_master_df = pd.DataFrame() 
    if os.path.exists(maker_master_filepath):
        try:
            maker_master_df = pd.read_csv(maker_master_filepath, encoding='utf-8')
            print(f"  ℹ️ {maker_master_filepath} を読み込みました (このデータは現在のmaker_com_code生成には使用されません)。")
        except Exception as e:
            print(f"❌ エラー: {maker_master_filepath} の読み込みに失敗しました。エラー: {e}")
            maker_master_df = pd.DataFrame() 
    else:
        print(f"⚠️ 警告: {maker_master_filepath} が見つかりません (現在のmaker_com_code生成には影響ありません)。")
        maker_master_df = pd.DataFrame() 


    jgroupid_master_filepath = os.path.join(MASTER_DATA_DIR, 'jgroupid_master.csv')
    
    if os.path.exists(jgroupid_master_filepath): 
        try:
            df_jgroupid_temp = pd.read_csv(jgroupid_master_filepath, encoding='utf-8', header=None)
            
            if not df_jgroupid_temp.empty and df_jgroupid_temp.shape[1] > 0:
                jgroupid_values_from_master = df_jgroupid_temp.iloc[:, 0].astype(str).tolist()
                if not jgroupid_values_from_master:
                    raise ValueError("jgroupid_master.csv からデータを読み込めましたが、リストが空です。")
            else:
                raise ValueError("jgroupid_master.csv が空またはデータがありません。")
            
        except Exception as e:
            print(f"❌ エラー: jgroupid_master.csv の読み込みに失敗しました。エンコーディングまたはフォーマットを確認してください。エラー: {e}")
            jgroupid_values_from_master = [str(i).zfill(3) for i in range(1, 94)] 
    else:
        print(f"⚠️ 警告: jgroupid_master.csv が見つかりません。パスを確認してください: {jgroupid_master_filepath}")
        jgroupid_values_from_master = [str(i).zfill(3) for i in range(1, 94)] 

    INPUT_PROCESSED_DIR = os.path.join(APP_ROOT_DIR, 'filtered_originals') 

    for root, dirs, files in os.walk(INPUT_PROCESSED_DIR):
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'): 
                input_filepath = os.path.join(root, filename)
                print(f"\n--- 処理対象ファイル: {input_filepath} ---")

                process_universal_csv(input_filepath, PROCESSED_OUTPUT_BASE_DIR, INPUT_PROCESSED_DIR, 
                                    maker_master_df, 
                                    FINAL_POSTGRE_COLUMNS, NO_HEADER_MAPPING_DICT, HAND_BILL_MAPPING_DICT, 
                                    FINANCIAL_STATEMENT_MAPPING_DICT, LOAN_DETAILS_MAPPING_DICT)

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    