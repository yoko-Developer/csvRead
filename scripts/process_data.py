import pandas as pd
import os
import re
from datetime import datetime
import random 
import shutil
import numpy as np 

# --- 設定項目 ---
INPUT_BASE_DIR = r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv' 
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'
SEARCH_RESULT_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'filtered_originals')
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data')

# ★★★ FINAL_POSTGRE_COLUMNS はお客様が提示した23カラムのリストに完全に一致！これが真の最終形！ ★★★
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


# --- 各CSVファイル形式ごとのマッピングルールを定義 ---
# ★★★ HAND_BILL_MAPPING_DICT はFINAL_POSTGRE_COLUMNSの23カラムに合わせて調整！ ★★★
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


# --- 関数定義 ---
# get_ocr_result_id_for_group はファイルグループ名を引数として受け取る
# ocr_id_mapping と _ocr_id_sequence_counter はメイン処理で初期化し、グローバル変数化
ocr_id_mapping = {}
_ocr_id_sequence_counter = 0 

def get_ocr_result_id_for_group(file_group_root_name): # 引数名を修正 (例: B000001)
    """
    ファイルグループ名（例: B000001）に基づいて、yyyymmddhhmmsssss0 形式のocr_result_idを生成または取得する。
    """
    global ocr_id_mapping
    global _ocr_id_sequence_counter

    if file_group_root_name not in ocr_id_mapping:
        # 新しいファイルグループの場合、現在日時と新しいシーケンス番号でIDを生成
        current_time_str = datetime.now().strftime('%Y%m%d%H%M') # yyyymmddhhmm
        
        # sssss (5桁シーケンス番号)。末尾1桁は0固定、残り4桁を通し番号 (0000, 0010, 0020...)
        # 「ファイルごとに2桁目を0から昇順に」 -> 00000, 00010, 00020, ... の形式
        # _ocr_id_sequence_counter * 10 で 0, 10, 20, ... を生成し、5桁ゼロ埋め
        sequence_part_int = _ocr_id_sequence_counter * 10
        # 5桁に収めるため、最大99990までを許容（00000から99990）
        if sequence_part_int > 99999: # 万が一5桁を超えた場合、リセットやエラーハンドリングも検討可能
            sequence_part_int = sequence_part_int % 100000 # 5桁に丸める
        
        sequence_part_str = str(sequence_part_int).zfill(5) 
        
        # new_ocr_id = f"{current_time_str}{sequence_part_str[:-1]}0" # 最後の1桁を0で固定 (これは正しい)
        new_ocr_id = f"{current_time_str}{sequence_part_str}" # ocr_result_id は sssss0 形式なので、sssss の部分を生成し、その末尾は0にする。

        ocr_id_mapping[file_group_root_name] = new_ocr_id
        _ocr_id_sequence_counter += 1
    
    return ocr_id_mapping[file_group_root_name]

# 他の関数は変更なし
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
                        maker_master_df, ocr_id_map_for_groups, current_file_group_root_name, # 引数名を修正 (例: B000001)
                        final_postgre_columns_list, no_header_map, hand_bill_map, financial_map, loan_map):
    """
    全てのAIRead出力CSVファイルを読み込み、統一されたPostgreSQL向けカラム形式に変換して出力する関数。
    CSVの種類（ヘッダー内容）を判別し、それぞれに応じたマッピングを適用する。
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

    # --- データ加工処理 ---
    df_data_rows = df_original.copy() 

    if df_data_rows.empty:
        print(f"  警告: ファイル {os.path.basename(input_filepath)} に有効なデータ行が見つからなかったため、加工をスキップします。")
        return 

    # 「〃」マークのみをffillで埋め、空文字列はそのまま維持
    df_data_rows = df_data_rows.ffill() 
    df_data_rows = df_data_rows.fillna('') 
    print(f"  ℹ️ 「〃」マークを直上データで埋め、元々ブランクだった箇所は維持しました。")

    # 合計行の削除ロジック
    keywords_to_delete = ["合計", "小計", "計", "手持手形計", "割引手形計"] # 手形計も追加
    
    filter_conditions = []
    # str.contains() を使用し、正規表現を適用
    keywords_regex = r'|'.join([re.escape(k) for k in keywords_to_delete]) # リストの各要素をエスケープしてOR結合
    
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
    
    # df_processed の初期化を最終調整
    df_processed = pd.DataFrame('', index=range(num_rows_to_process), columns=final_postgre_columns_list)


    # --- 共通項目 (PostgreSQLのグリーンの表の左側に来る、自動生成項目) を生成 ---
    # ★★★ ocr_result_id はファイルグループ名から取得するように変更 ★★★
    df_processed['ocr_result_id'] = [get_ocr_result_id_for_group(current_file_group_root_name)] * num_rows_to_process 

    df_processed['page_no'] = [1] * num_rows_to_process 

    df_processed['id'] = range(1, num_rows_to_process + 1)

    jgroupid_string_val = get_next_jgroupid_string() 
    df_processed['jgroupid_string'] = [jgroupid_string_val] * num_rows_to_process

    cif_number_val = str(random.randint(100000, 999999))
    df_processed['cif_number'] = [cif_number_val] * num_rows_to_process

    settlement_at_val = datetime.now().strftime('%Y%m') 
    df_processed['settlement_at'] = [settlement_at_val] * num_rows_to_process

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

    df_data_rows.columns = df_data_rows.columns.astype(str) # 念のためここでもstrに変換
    
    for pg_col_name, src_ref in mapping_to_use.items():
        source_data_series = None
        if isinstance(src_ref, str): 
            if src_ref in df_data_rows.columns: 
                source_data_series = df_data_rows[src_ref]
            else:
                print(f"  ⚠️ 警告: マッピング元のカラム '{src_ref}' が元のCSVファイルに見つかりませんでした（PostgreSQLカラム: {pg_col_name}）。このカラムはブランクになります。")
        elif isinstance(src_ref, int): 
            if str(src_ref) in df_data_rows.columns: 
                source_data_series = df_data_rows[str(src_ref)]
            elif src_ref < df_data_rows.shape[1]: 
                source_data_series = df_data_rows.iloc[:, src_ref]
            else:
                print(f"  ⚠️ 警告: マッピング元の列インデックス '{src_ref}' が元のCSVファイルに存在しません（PostgreSQLカラム: {pg_col_name}）。このカラムはブランクになります。")

        if source_data_series is not None:
            df_processed[pg_col_name] = source_data_series.astype(str).values 
        else:
            pass 


    # --- Excel関数相当のロジックを適用（派生カラムの生成） ---
    # ★★★ 各カラムの生成ロジックをお客様が提示した23カラムのリストに忠実に再現する！ ★★★
    
    df_processed['maker_name_original'] = df_processed['maker_name'].copy() 
    
    df_processed['maker_com_code'] = df_processed['maker_name'].apply(get_maker_com_code_for_name)

    # issue_date_original, issue_date, due_date_original, due_date, balance_original, balance
    # issue_date, due_date, balance は HAND_BILL_MAPPING_DICT または NO_HEADER_MAPPING_DICT で直接マッピングされている
    # issue_date_original, due_date_original, balance_original はそれらからのコピー

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


    # paying_bank_name_original, paying_bank_name, paying_bank_branch_name_original, paying_bank_branch_name
    # discount_bank_name_original, discount_bank_name, description_original, description
    # これらのカラムは HAND_BILL_MAPPING_DICT または NO_HEADER_MAPPING_DICT で直接マッピングされている
    
    df_processed['paying_bank_name_original'] = df_processed['paying_bank_name'].copy() 
    df_processed['paying_bank_branch_name_original'] = df_processed['paying_bank_branch_name'].copy() 
    df_processed['discount_bank_name_original'] = df_processed['discount_bank_name'].copy() 
    df_processed['description_original'] = df_processed['description'].copy() 
    
    # ★★★ 修正ここまで ★★★
    
    # --- 保存処理 ---
    relative_path_to_file = os.path.relpath(input_filepath, input_base_dir)
    relative_dir_to_file = os.path.dirname(relative_path_to_file)
    processed_output_sub_dir = os.path.join(processed_output_base_dir, relative_dir_to_file)
    os.makedirs(processed_output_sub_dir, exist_ok=True)

    processed_output_filename = os.path.basename(input_filepath).replace('.csv', '_processed.csv')
    processed_output_filepath = os.path.join(processed_output_sub_dir, processed_output_filename)
    df_processed.to_csv(processed_output_filepath, index=False, encoding='utf-8-sig')

    print(f"✅ 加工完了: {input_filepath} -> {processed_output_filepath}")

# --- メイン処理 ---
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

    # ★★★ ocr_result_id のマッピングを事前に生成するロジック ★★★
    print("\n--- ocr_result_id マッピング事前生成開始 ---")
    ocr_id_mapping = {}
    _ocr_id_sequence_counter = 0 
    
    # 対象となる全ファイルをリストアップし、ファイルグループでソート
    all_target_file_groups_root = set() # ファイルグループのルート名 (例: B000001) を格納するセット
    for root, dirs, files in os.walk(INPUT_PROCESSED_DIR):
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'):
                # ファイル名から「ファイルグループのルート名」を抽出 (例: B000001)
                # B000001_2.jpg_020.csv -> B000001
                match = re.match(r'^(B\d{6})_(\d)\.jpg_020\.csv$', filename, re.IGNORECASE)
                if match:
                    all_target_file_groups_root.add(match.group(1)) # B000001 の部分をセットに追加
                else:
                    print(f"  ℹ️ ファイル名パターンに合致しないファイル: {filename} はocr_result_id生成対象外です。")
                    
    sorted_file_groups_root = sorted(list(all_target_file_groups_root)) # ソートする
    
    # 昇順にソートされたファイルグループに対して ocr_result_id を割り当てる
    for group_root_name in sorted_file_groups_root:
        get_ocr_result_id_for_group(group_root_name) # この関数が ocr_id_mapping を更新する
    
    print("--- ocr_result_id マッピング事前生成完了 ---")
    print(f"生成された ocr_result_id マッピング (最初の5つ): {list(ocr_id_mapping.items())[:5]}...")

    # ★★★ メインのファイル処理ループ ★★★
    for root, dirs, files in os.walk(INPUT_PROCESSED_DIR):
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'): 
                input_filepath = os.path.join(root, filename)
                print(f"\n--- 処理対象ファイル: {input_filepath} ---")

                # 現在のファイルのファイルグループのルート名を抽出
                current_file_group_root_name = None
                match = re.match(r'^(B\d{6})_(\d)\.jpg_020\.csv$', filename, re.IGNORECASE)
                if match:
                    current_file_group_root_name = match.group(1) # B000001 の部分
                
                if current_file_group_root_name is None:
                    print(f"  ⚠️ 警告: ファイル {filename} のファイルグループのルート名を特定できませんでした。このファイルはスキップします。")
                    continue 

                process_universal_csv(input_filepath, PROCESSED_OUTPUT_BASE_DIR, INPUT_PROCESSED_DIR, 
                                    maker_master_df, ocr_id_mapping, current_file_group_root_name, # 追加引数を渡す
                                    FINAL_POSTGRE_COLUMNS, NO_HEADER_MAPPING_DICT, HAND_BILL_MAPPING_DICT, 
                                    FINANCIAL_STATEMENT_MAPPING_DICT, LOAN_DETAILS_MAPPING_DICT)

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    