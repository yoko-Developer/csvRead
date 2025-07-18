import pandas as pd
import os
import re
from datetime import datetime
import random 
import shutil
import numpy as np 

# 設定項目
INPUT_BASE_DIR = r'G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\10_受取手形\Import'
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'
SEARCH_RESULT_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'filtered_originals')
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data')

# 23カラムの定義
FINAL_POSTGRE_COLUMNS = [
    'ocr_result_id', 'page_no', 'id', 'jgroupid_string', 'cif_number', 'settlement_at',
    'maker_name_original', 'maker_name', 'maker_com_code',
    'issue_date_original',      
    'issue_date',
    'due_date_original',        
    'due_date',
    'balance_original',         
    'balance',
    'paying_bank_name_original',
    'paying_bank_name',         
    'paying_bank_branch_name_original', 
    'paying_bank_branch_name',  
    'discount_bank_name_original',
    'discount_bank_name',       
    'description_original',     
    'description'               
]


# 各CSVファイル形式ごとのマッピングルールを定義
HAND_BILL_MAPPING_DICT = {
    'maker_name': '振出人',
    'issue_date': '振出年月日',                    
    'due_date': '支払期日',                        
    'balance': '金額',                            
    'paying_bank_name': '支払銀行名称',            
    'paying_bank_branch_name': '支払銀行支店名',   
    'discount_bank_name': '割引銀行名及び支店名等', 
    'description': '摘要'                       
}

FINANCIAL_STATEMENT_MAPPING_DICT = {
    'maker_name': 'account', 
    'issue_date': 'amount_0', 
    'balance': 'amount_0',    
    'due_date': 'amount_1',   
    'description': 'amount_2' 
}

LOAN_DETAILS_MAPPING_DICT = {
    'maker_name': '借入先名称(氏名)',
    'issue_date': '借入先所在地(住所)', 
    'balance': '期末現在高',           
    'description_rightside': '期中の支払利子額', 
    'description': '利率',            
}

NO_HEADER_MAPPING_DICT = {
    'maker_name': 0, 
    'issue_date': 1, 
    'due_date': 2,   
    'paying_bank_name': 3, 
    'paying_bank_branch_name': 4, 
    'balance': 5,    
    'discount_bank_name': 6, 
    'description': 7, 
}


# ocr_id_mapping と _ocr_id_sequence_counter はメイン処理で初期化し、グローバル変数化
ocr_id_mapping = {}
_ocr_id_sequence_counter = 0 

def get_ocr_result_id_for_group(file_group_root_name): 
    """
    ファイルグループ名（例: B000001）に基づいて、yyyymmddhhmmsssss0 形式のocr_result_idを生成または取得する。
    """
    global ocr_id_mapping
    global _ocr_id_sequence_counter

    if file_group_root_name not in ocr_id_mapping:
        current_time_str = datetime.now().strftime('%Y%m%d%H%M') # yyyymmddhhmm
        
        sequence_part_int = _ocr_id_sequence_counter * 10
        if sequence_part_int > 99999: 
            sequence_part_int = sequence_part_int % 100000 
        
        sequence_part_str = str(sequence_part_int).zfill(5) 
        
        new_ocr_id = f"{current_time_str}{sequence_part_str}" # 18桁形式 yyyymmddhhmmsssss0

        ocr_id_mapping[file_group_root_name] = new_ocr_id
        _ocr_id_sequence_counter += 1
    
    return ocr_id_mapping[file_group_root_name]

maker_name_to_com_code_map = {}
next_maker_com_code_val = 100 
current_jgroupid_index = 0 
jgroupid_values_from_master = [] 


def get_next_jgroupid_string():
    global current_jgroupid_index 
    global jgroupid_values_from_master 

    if jgroupid_values_from_master:
        jgroupid_val = jgroupid_values_from_master[current_jgroupid_index % len(jgroupid_values_from_master)]
        current_jgroupid_index += 1
        return str(jgroupid_val).zfill(3) 
    else:
        return "000" 

def get_maker_com_code_for_name(maker_name):
    global maker_name_to_com_code_map 
    global next_maker_com_code_val 

    maker_name_str = str(maker_name).strip() 
    
    if not maker_name_str: 
        return "" 

    if maker_name_str in maker_name_to_com_code_map:
        return maker_name_to_com_code_map[maker_name_str]
    else:
        new_code_int = next_maker_com_code_val % 1000 
        if new_code_int < 100: 
            new_code_int = 100 + new_code_int 
        new_code = str(new_code_int).zfill(3) 
        
        maker_name_to_com_code_map[maker_name_str] = new_code
        next_maker_com_code_val += 1
        return new_code


def process_universal_csv(input_filepath, processed_output_base_dir, input_base_dir, 
                        maker_master_df, ocr_id_map_for_groups, current_file_group_root_name, 
                        final_postgre_columns_list, no_header_map, hand_bill_map, financial_map, loan_map):
    """
    全てのAIRead出力CSVファイルを読み込み、統一されたPostgreSQL向けカラム形式に変換して出力する関数
    CSVの種類（ヘッダー内容）を判別し、それぞれに応じたマッピングを適用する
    """
    df_original = None
    file_type = "不明" 
    
    try:
        encodings_to_try = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
        
        for enc in encodings_to_try:
            try:
                df_original = pd.read_csv(input_filepath, encoding=enc, header=0, sep=',', quotechar='"', 
                                        dtype=str, na_values=['〃'], keep_default_na=False)
                
                df_original.columns = df_original.columns.str.strip() 
                
                current_headers = df_original.columns.tolist()

                is_hand_bill = ('振出人' in current_headers) and ('金額' in current_headers)
                is_financial = ('account' in current_headers)
                is_loan = ('借入先名称(氏名)' in current_headers)

                if is_hand_bill:
                    file_type = "手形情報"
                elif is_financial:
                    file_type = "財務諸表"
                elif is_loan:
                    file_type = "借入金明細"
                else:
                    file_type = "汎用データ_ヘッダーなし"
                    df_original = pd.read_csv(input_filepath, encoding=enc, header=None, sep=',', quotechar='"', 
                                            dtype=str, na_values=['〃'], keep_default_na=False)
                    df_original.columns = df_original.columns.astype(str).str.strip() 
                
                print(f"  デバッグ: ファイル {os.path.basename(input_filepath)} の判定結果: '{file_type}'")
                print(f"  デバッグ: 読み込んだ df_original のカラム:\n{df_original.columns.tolist()}")
                print(f"  デバッグ: 読み込んだ df_original の最初の3行:\n{df_original.head(3).to_string()}") 
                print(f"  デバッグ: df_original内の欠損値 (NaN) の数:\n{df_original.isnull().sum().to_string()}") 
                    
                break 
            except Exception as e_inner: 
                print(f"  ファイル {os.path.basename(input_filepath)} を {enc} で読み込み失敗。別のエンコーディングを試します。エラー: {e_inner}")
                df_original = None 
                continue 

        if df_original is None or df_original.empty:
            print(f"  警告: ファイル {os.path.basename(input_filepath)} をどのエンコーディングとヘッダー設定でも読み込めませんでした。処理をスキップします。")
            return 
        
        print(f"  ファイル {os.path.basename(input_filepath)} は '{file_type}' として処理します。")

    except Exception as e:
        print(f"❌ エラー発生（{input_filepath}）: CSV読み込みまたはファイルタイプ判別で問題が発生しました。エラー: {e}")
        import traceback
        traceback.print_exc()
        return

    # データ加工処理
    df_data_rows = df_original.copy() 

    if df_data_rows.empty:
        print(f"  警告: ファイル {os.path.basename(input_filepath)} に有効なデータ行が見つからなかったため、加工をスキップします。")
        return 

    # 「〃」のみをffillで埋め、空文字列はそのまま維持
    df_data_rows = df_data_rows.ffill() 
    df_data_rows = df_data_rows.fillna('') 
    print(f"  ℹ️ 「〃」マークを直上データで埋め、元々ブランクだった箇所は維持しました。")

    # 合計行の削除ロジック
    keywords_to_delete = ["合計", "小計", "計", "手持手形計", "割引手形計"] 
    
    filter_conditions = []
    # str.contains() を使用し、正規表現を適用
    keywords_regex = r'|'.join([re.escape(k) for k in keywords_to_delete]) 
    
    if file_type == "手形情報":
        if '振出人' in df_data_rows.columns:
            filter_conditions.append(df_data_rows['振出人'].str.contains(keywords_regex, regex=True, na=False))
    elif file_type == "財務諸表":
        if 'account' in df_data_rows.columns:
            filter_conditions.append(df_data_rows['account'].str.contains(keywords_regex, regex=True, na=False))
    elif file_type == "借入金明細":
        if '借入先名称(氏名)' in df_data_rows.columns:
            filter_conditions.append(df_data_rows['借入先名称(氏名)'].str.contains(keywords_regex, regex=True, na=False))
    elif file_type == "汎用データ_ヘッダーなし":
        if '0' in df_data_rows.columns: 
            filter_conditions.append(df_data_rows['0'].str.contains(keywords_regex, regex=True, na=False))

    if filter_conditions:
        combined_filter = pd.concat(filter_conditions, axis=1).any(axis=1)
        rows_deleted_count = combined_filter.sum()
        df_data_rows = df_data_rows[~combined_filter].reset_index(drop=True)
        if rows_deleted_count > 0:
            print(f"  ℹ️ 合計行（キーワードパターン: {keywords_regex}）を {rows_deleted_count} 行削除しました。")
    
    num_rows_to_process = len(df_data_rows) 
    
    # df_processed の初期化
    df_processed = pd.DataFrame('', index=range(num_rows_to_process), columns=final_postgre_columns_list)


    # 共通項目 (PostgreSQLのグリーンの表の左側、自動生成項目) を生成
    # ocr_result_id: ファイルグループ名から取得
    df_processed['ocr_result_id'] = [get_ocr_result_id_for_group(current_file_group_root_name)] * num_rows_to_process 

    df_processed['page_no'] = [1] * num_rows_to_process 

    df_processed['id'] = range(1, num_rows_to_process + 1)

    # jgroupid_string: 固定値 '001'
    df_processed['jgroupid_string'] = ['001'] * num_rows_to_process

    # cif_number: ファイル名から抽出した6桁の数値
    # current_file_group_root_name は "B000001" の形式なので、先頭の'B'を除いた6桁を取得
    cif_number_val = current_file_group_root_name[1:] # 'B'を除いた部分
    df_processed['cif_number'] = [cif_number_val] * num_rows_to_process

    settlement_at_val = datetime.now().strftime('%Y%m') 
    df_processed['settlement_at'] = [settlement_at_val] * num_rows_to_process

    # 各ファイルタイプに応じたマッピングルールを適用
    mapping_to_use = {}
    if file_type == "手形情報":
        mapping_to_use = hand_bill_map
    elif file_type == "財務諸表": 
        mapping_to_use = financial_map
    elif file_type == "借入金明細": 
        mapping_to_use = loan_map
    else: 
        mapping_to_use = no_header_map

    df_data_rows.columns = df_data_rows.columns.astype(str)
    
    for pg_col_name, src_ref in mapping_to_use.items():
        source_data_series = None
        if isinstance(src_ref, str): 
            if src_ref in df_data_rows.columns: 
                source_data_series = df_data_rows[src_ref]
            else:
                print(f"  ⚠️ 警告: マッピング元のカラム '{src_ref}' が元のCSVファイルに見つかりませんでした（PostgreSQLカラム: {pg_col_name}）このカラムはブランク")
        elif isinstance(src_ref, int): 
            if str(src_ref) in df_data_rows.columns: 
                source_data_series = df_data_rows[str(src_ref)]
            elif src_ref < df_data_rows.shape[1]: 
                source_data_series = df_data_rows.iloc[:, src_ref]
            else:
                print(f"  ⚠️ 警告: マッピング元の列インデックス '{src_ref}' が元のCSVファイルに存在しません（PostgreSQLカラム: {pg_col_name}）このカラムはブランク")

        if source_data_series is not None:
            df_processed[pg_col_name] = source_data_series.astype(str).values 
        else:
            pass 


    # --- Excel関数相当のロジックを適用（派生カラムの生成） ---
    df_processed['maker_name_original'] = df_processed['maker_name'].copy() 
    
    df_processed['maker_com_code'] = df_processed['maker_name'].apply(get_maker_com_code_for_name)

    df_processed['issue_date_original'] = df_processed['issue_date'].copy() 
    df_processed['due_date_original'] = df_processed['due_date'].copy()   

    def clean_balance_no_comma(value):
        try:
            cleaned_value = str(value).replace(',', '').strip()
            if not cleaned_value:
                return '' 
            numeric_value = float(cleaned_value)
            return str(int(numeric_value)) 
        except ValueError:
            return '' 
    
    df_processed['balance'] = df_processed['balance'].apply(clean_balance_no_comma)
    df_processed['balance_original'] = df_processed['balance'].copy() 

    df_processed['paying_bank_name_original'] = df_processed['paying_bank_name'].copy() 
    df_processed['paying_bank_branch_name_original'] = df_processed['paying_bank_branch_name'].copy() 
    df_processed['discount_bank_name_original'] = df_processed['discount_bank_name'].copy() 
    df_processed['description_original'] = df_processed['description'].copy() 
    
    
    # 保存処理
    relative_path_to_file = os.path.relpath(input_filepath, input_base_dir)
    relative_dir_to_file = os.path.dirname(relative_path_to_file)
    processed_output_sub_dir = os.path.join(processed_output_base_dir, relative_dir_to_file)
    os.makedirs(processed_output_sub_dir, exist_ok=True)

    processed_output_filename = os.path.basename(input_filepath).replace('.csv', '_processed.csv')
    processed_output_filepath = os.path.join(processed_output_sub_dir, processed_output_filename)
    df_processed.to_csv(processed_output_filepath, index=False, encoding='utf-8-sig')

    print(f"✅ 加工完了: {input_filepath} -> {processed_output_filepath}")

# メイン処理 
if __name__ == "__main__":
    print(f"--- 処理開始: {datetime.now()} ---")

    os.makedirs(PROCESSED_OUTPUT_BASE_DIR, exist_ok=True) 

    MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data') 

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
        print(f"⚠️ 警告: {jgroupid_master_filepath} が見つかりません。パスを確認してください: {jgroupid_master_filepath}")
        jgroupid_values_from_master = [str(i).zfill(3) for i in range(1, 94)] 

    INPUT_PROCESSED_DIR = os.path.join(APP_ROOT_DIR, 'filtered_originals') 

    # ocr_result_id のマッピングを事前に生成するロジック（ファイルグループのルート名抽出）
    print("\n--- ocr_result_id マッピング事前生成開始 ---")
    ocr_id_mapping = {}
    _ocr_id_sequence_counter = 0 
    
    all_target_file_groups_root = set() 
    for root, dirs, files in os.walk(INPUT_PROCESSED_DIR):
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'):
                # ファイル名から「ファイルグループのルート名」を抽出 (例: B000001)
                # B000001_2.jpg_020.csv -> B000001
                match = re.match(r'^(B\d{6})_.*\.jpg_020\.csv$', filename, re.IGNORECASE)
                if match:
                    all_target_file_groups_root.add(match.group(1)) 
                else:
                    print(f"  ℹ️ ファイル名パターンに合致しないファイル: {filename} はocr_result_id生成対象外です。")
                    
    sorted_file_groups_root = sorted(list(all_target_file_groups_root)) 
    
    for group_root_name in sorted_file_groups_root:
        get_ocr_result_id_for_group(group_root_name) 
    
    print("--- ocr_result_id マッピング事前生成完了 ---")
    print(f"生成された ocr_result_id マッピング (最初の5つ): {list(ocr_id_mapping.items())[:5]}...")

    # メインのファイル処理ループ
    for root, dirs, files in os.walk(INPUT_PROCESSED_DIR):
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'): 
                input_filepath = os.path.join(root, filename)
                print(f"\n--- 処理対象ファイル: {input_filepath} ---")

                # 現在のファイルのファイルグループのルート名を抽出
                current_file_group_root_name = None
                match = re.match(r'^(B\d{6})_.*\.jpg_020\.csv$', filename, re.IGNORECASE)
                if match:
                    current_file_group_root_name = match.group(1) 
                
                if current_file_group_root_name is None:
                    print(f"  ⚠️ 警告: ファイル {filename} のファイルグループのルート名を特定できませんでした。このファイルはスキップします。")
                    continue 

                process_universal_csv(input_filepath, PROCESSED_OUTPUT_BASE_DIR, INPUT_PROCESSED_DIR, 
                                    maker_master_df, ocr_id_mapping, current_file_group_root_name, 
                                    FINAL_POSTGRE_COLUMNS, NO_HEADER_MAPPING_DICT, HAND_BILL_MAPPING_DICT, 
                                    FINANCIAL_STATEMENT_MAPPING_DICT, LOAN_DETAILS_MAPPING_DICT)

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    