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


# --- 関数定義（この部分は変更しないでね！） ---
# ocr_result_id の通し番号を保持するためのグローバル変数
# この変数はスクリプトの実行開始時に一度だけ初期化される
current_ocr_id_sequence = 0 

# maker_com_code の採番を保持するためのグローバル変数
maker_name_to_com_code_map = {}
next_maker_com_code_val = 1 # 001から開始

# jgroupid_string の採番を保持するためのグローバル変数
current_jgroupid_index = 0 # jgroupidマスタリストのインデックスとして使用
jgroupid_values_from_master = [] # jgroupidマスタから読み込んだ値を格納


def get_next_ocr_id():
    """ocr_result_idを1から9999までの自動採番で取得する（4桁で終わる）"""
    global current_ocr_id_sequence 
    current_ocr_id_sequence += 1
    # 1から9999までの連番を4桁でゼロ埋め。9999を超えたら0001に戻る（Excelの「超えない」要件解釈）
    # ID全体を4桁にする（Excelの「4桁で終わる」と「1から採番」を優先）
    return str(current_ocr_id_sequence % 10000).zfill(4)

def get_next_jgroupid_string():
    """jgroupid_stringをjgroupidマスタから連番で取得する（1から93をループ）"""
    global current_jgroupid_index 
    global jgroupid_values_from_master 

    # jgroupidマスタが空でなければ、そのリストから取得
    if jgroupid_values_from_master:
        # リストの長さに応じてループし、連番で取得
        jgroupid_val = jgroupid_values_from_master[current_jgroupid_index % len(jgroupid_values_from_master)]
        current_jgroupid_index += 1
        return str(jgroupid_val).zfill(3) # 3桁でゼロ埋めを保証
    else:
        # マスタが読み込めなかった場合のフォールバック（以前の動作）
        return "000" # マスタが読めない場合はデフォルト値

def get_maker_com_code_for_name(maker_name):
    """
    maker_nameに基づいて3桁の会社コードを採番・取得する。
    同じmaker_nameには同じコードを割り当てる。
    """
    global maker_name_to_com_code_map 
    global next_maker_com_code_val 

    if maker_name in maker_name_to_com_code_map:
        return maker_name_to_com_code_map[maker_name]
    else:
        # 新しい3桁コードを生成
        new_code = str(next_maker_com_code_val).zfill(3)
        maker_name_to_com_code_map[maker_name] = new_code
        next_maker_com_code_val += 1
        return new_code


def process_universal_csv(input_filepath, processed_output_base_dir, input_base_dir, 
                        maker_master_df, # jgroupid_master_df_param は使わないので削除
                        final_postgre_columns_list, no_header_map, hand_bill_map, financial_map, loan_map): # 引数からjgroupid_master_df_paramを削除
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
                # ファイル全体を文字列として読み込み、ヘッダー判定とデータ読み込みを行う
                with open(input_filepath, 'r', encoding=enc, newline='') as f_read_all:
                    all_lines_from_file = f_read_all.readlines()
                
                if not all_lines_from_file:
                    raise ValueError("ファイルが空です。")
                
                first_line_content = all_lines_from_file[0].strip()

                # ヘッダーを自動判別
                # ヘッダー行をpandasに渡すかどうかを決定
                read_header = 0 # default for header exists
                
                if ('"振出人"' in first_line_content) or ('振出人,' in first_line_content):
                    file_type = "手形情報"
                elif ('"account"' in first_line_content) or ('account,' in first_line_content):
                    file_type = "財務諸表"
                elif ('"借入先名称(氏名)"' in first_line_content) or ('借入名称(氏名),' in first_line_content): # '借入名称(氏名)'の後にカンマがないケースに対応
                    file_type = "借入金明細"
                else:
                    file_type = "汎用データ_ヘッダーなし"
                    read_header = None # ヘッダーがないので、最初の行からデータとして読み込む


                df_original = pd.read_csv(input_filepath, encoding=enc, header=read_header)
                print(f"  ファイル {os.path.basename(input_filepath)} を {enc} ({file_type}, header={read_header}) で読み込み成功。")
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
    # df_processed を先に初期化し、PostgreSQLの最終カラム構造を持つようにする
    df_processed = pd.DataFrame(columns=final_postgre_columns_list)
    
    # 実際のデータ行のみを処理対象とする
    # header=0で読み込んだ場合は df_originalは既にヘッダー行が除かれている。
    # header=Noneで読み込んだ場合（汎用データ_ヘッダーなし）は、df_originalの0行目がヘッダーデータなので、それをスキップする。
    df_data_rows = None
    if file_type == "汎用データ_ヘッダーなし":
        df_data_rows = df_original.iloc[1:].copy() # 最初の行をヘッダーデータとしてスキップ (header=Noneで読んだ場合)
    else: # 手形情報, 財務諸表, 借入金明細 (header=0で読み込まれたもの)
        df_data_rows = df_original.iloc[0:].copy() # df_original自体が既にヘッダー行が除かれたデータなので、全てをコピー

    # df_data_rows が空でないことを確認
    if df_data_rows.empty:
        print(f"  警告: ファイル {os.path.basename(input_filepath)} に有効なデータ行が見つからなかったため、加工をスキップします。")
        return # このファイルの処理を中断

    num_rows_to_process = len(df_data_rows) # 処理するデータ行数

    # --- 共通項目 (PostgreSQLのグリーンの表の左側に来る、自動生成項目) の生成 ---
    # ocr_result_id: 1からの自動採番で、4桁で終わる
    ocr_result_id_val = get_next_ocr_id() 
    df_processed['ocr_result_id'] = [ocr_result_id_val] * num_rows_to_process 


    # page_no: 何でもよい（1で固定）の要件に従う
    df_processed['page_no'] = [1] * num_rows_to_process 

    # id: ファイルの中でカウントアップ (各行にユニークなID)
    df_processed['id'] = range(1, num_rows_to_process + 1)

    # jgroupid_string: jgroupid_masterから連番で取得
    jgroupid_string_val = get_next_jgroupid_string() # グローバル変数から取得
    df_processed['jgroupid_string'] = [jgroupid_string_val] * num_rows_to_process

    # cif_number: ランダムな数字列（6桁の例）
    cif_number_val = str(random.randint(100000, 999999))
    df_processed['cif_number'] = [cif_number_val] * num_rows_to_process

    # settlement_at: yyyyMM形式で何でもよい
    settlement_at_val = datetime.now().strftime('%Y%m') # YYYYMM形式
    df_processed['settlement_at'] = [settlement_at_val] * num_rows_to_process

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
            if src_ref in df_data_rows.columns: # df_data_rows のカラム名をチェック
                df_processed[pg_col_name] = df_data_rows[src_ref].fillna('').astype(str).values 
            # else: 元のCSVにヘッダーが存在しない場合は空に (初期化されているので何もしない)
        elif isinstance(src_ref, int): # 元が列インデックスの場合
            if src_ref < df_data_rows.shape[1]:
                df_processed[pg_col_name] = df_data_rows.iloc[:, src_ref].fillna('').astype(str).values 
            # else: 元のCSVに列が存在しない場合は空に (初期化されているので何もしない)
        # else: マッピングルールが不正な場合も、すでに初期化済みなので何もしない


    # --- Excel関数相当のロジックを適用（派生カラムの生成） ---

    # maker_name_original は maker_name と同じ
    df_processed['maker_name_original'] = df_processed['maker_name'].fillna('').astype(str)
    
    # maker_com_code (新しい3桁自動採番ロジック)
    # 各行のmaker_nameに対してコードを生成・取得
    df_processed['maker_com_code'] = df_processed['maker_name'].apply(get_maker_com_code_for_name)

    # issue_date_rightside_date, due_date_rightside_date, balance_rightside など
    # Excelの例だとそれぞれ対応するカラムと同じ値
    df_processed['issue_date_rightside_date'] = df_processed['issue_date'].fillna('').astype(str)
    df_processed['due_date_rightside_date'] = df_processed['due_date'].fillna('').astype(str)
    df_processed['balance_rightside'] = df_processed['balance'].fillna('').astype(str)
    df_processed['payment_bank_name_rightside'] = df_processed['payment_bank_name'].fillna('').astype(str)
    df_processed['payment_bank_branch_name_rightside'] = df_processed['payment_bank_branch_name'].fillna('').astype(str)
    # description_rightside は description と同じ (もしExcelでそうなら)
    # description_rightsideとdescriptionは別々にマッピングされたので、そのまま。

    # 最終的な列の順序はPostgreSQLの目標形式に合わせる (df_processedは既にこのカラム順で作成されている)
    # reindexは不要。初期化でカラム順は確保済み。

    # --- 保存処理 ---
    # 出力先のサブフォルダを元のフォルダ構造に合わせて作成
    relative_path_to_file = os.path.relpath(input_filepath, input_base_dir)
    relative_dir_to_file = os.path.dirname(relative_path_to_file)
    processed_output_sub_dir = os.path.join(processed_output_base_dir, relative_dir_to_file)
    os.makedirs(processed_output_sub_dir, exist_ok=True)

    # 加工後のCSVを保存
    processed_output_filename = os.path.basename(input_filepath).replace('.csv', '_processed.csv')
    processed_output_filepath = os.path.join(processed_output_sub_dir, processed_output_filename)
    df_processed.to_csv(processed_output_filepath, index=False, encoding='utf-8-sig')

    print(f"✅ 加工完了: {input_filepath} -> {processed_output_filepath}")

# --- メイン処理（この部分も変更しないでね！） ---
if __name__ == "__main__":
    print(f"--- 処理開始: {datetime.now()} ---")

    # 出力フォルダがなければ作成
    os.makedirs(PROCESSED_OUTPUT_BASE_DIR, exist_ok=True) 

    # マスタデータ読み込み
    MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data') # APP_ROOT_DIR からパスを構築

    # maker_master.csv を読み込む
    maker_master_filepath = os.path.join(MASTER_DATA_DIR, 'master.csv') # <<-- ファイル名を 'master.csv' に修正済み！
    maker_master_df = pd.DataFrame() 
    if os.path.exists(maker_master_filepath):
        try:
            maker_master_df = pd.read_csv(maker_master_filepath, encoding='utf-8')
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
    
    if os.path.exists(jgroupid_master_filepath): 
        try:
            # jgroupid_master.csv はヘッダーなしで、1行目からデータが始まることを想定
            df_jgroupid_temp = pd.read_csv(jgroupid_master_filepath, encoding='utf-8', header=None)
            
            # 0列目（最初の列）のデータをリストとして取得し、グローバル変数に格納
            if not df_jgroupid_temp.empty and df_jgroupid_temp.shape[1] > 0:
                jgroupid_values_from_master = df_jgroupid_temp.iloc[:, 0].astype(str).tolist()
                # リストが空でないか確認
                if not jgroupid_values_from_master:
                    raise ValueError("jgroupid_master.csv からデータを読み込めましたが、リストが空です。")
            else:
                raise ValueError("jgroupid_master.csv が空またはデータがありません。")
            
        except Exception as e:
            print(f"❌ エラー: jgroupid_master.csv の読み込みに失敗しました。エンコーディングまたはフォーマットを確認してください。エラー: {e}")
            # エラー発生時はデフォルトの1-93リストで継続
            jgroupid_values_from_master = [str(i).zfill(3) for i in range(1, 94)] 
    else:
        print(f"⚠️ 警告: jgroupid_master.csv が見つかりません。パスを確認してください: {jgroupid_master_filepath}")
        # 見つからない場合のデフォルトデータ（例示）
        jgroupid_values_from_master = [str(i).zfill(3) for i in range(1, 94)] 


    # INPUT_PROCESSED_DIR は filter_and_copy_csv.py が出力したフォルダ
    INPUT_PROCESSED_DIR = os.path.join(APP_ROOT_DIR, 'filtered_originals') 

    # INPUT_PROCESSED_DIR内の全てのCSVファイルを処理
    for root, dirs, files in os.walk(INPUT_PROCESSED_DIR):
        for filename in files:
            # _processed.csv が付いていないCSVファイルのみを処理対象とする
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'): 
                input_filepath = os.path.join(root, filename)
                print(f"\n--- 処理対象ファイル: {input_filepath} ---")

                # 加工処理を実行
                process_universal_csv(input_filepath, PROCESSED_OUTPUT_BASE_DIR, INPUT_PROCESSED_DIR, 
                                    maker_master_df, 
                                    FINAL_POSTGRE_COLUMNS, NO_HEADER_MAPPING_DICT, HAND_BILL_MAPPING_DICT, 
                                    FINANCIAL_STATEMENT_MAPPING_DICT, LOAN_DETAILS_MAPPING_DICT)

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    