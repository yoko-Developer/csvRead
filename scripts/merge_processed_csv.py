import pandas as pd
import os
import re
import shutil 
from datetime import datetime 

# 設定項目
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'

PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
MERGED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'merged_output') 

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

# ocr_id_mapping と _ocr_id_sequence_counter: merge_processed_csv_files のローカル変数に移動
# ocr_result_id の時刻部分を固定するための変数をメイン処理開始時に設定
_merge_ocr_id_fixed_timestamp_str = "" 

# _generate_expected_ocr_id_for_merge 関数を修正し、固定時刻を使用するように変更
def _generate_expected_ocr_id_for_merge(group_root_name_local):
    global _merge_ocr_id_fixed_timestamp_str
    # ocr_id_mapping はこの関数スコープ外で管理されるか、関数内で一時的に生成される
    # process_data.py と同じロジックでIDを「再計算」する
    # process_data.py での ocr_id_sequence_counter の状態を知ることはできないため、
    # 各グループのIDが本当に process_data.py と同じになるか確認が必要。

    # 最も確実な方法は、process_data.py が ocr_id_mapping をファイルに保存し、merge_processed_csv.py がそれを読み込むこと。
    # ocr_id_mapping を _merge_processed_csv_files の中で再生成する。

    # この関数はもはやグローバルな _ocr_id_sequence_counter を使わず、
    # _merge_ocr_id_fixed_timestamp_str のみを使うようにする。
    # シーケンス番号の部分は、ocr_result_id_mapping_actual という形で、実際にファイルから読み取ったIDを信頼する
    pass # この関数は後で削除か、別の使い方に変更

# ファイルから読み込んだ ocr_result_idをキーにマージする

def merge_processed_csv_files():
    """
    processed_output フォルダ内の加工済みCSVファイルをファイルグループごとに結合し、
    merged_output フォルダに保存する関数
    """
    global _merge_ocr_id_fixed_timestamp_str # グローバル変数を使用
    
    print(f"--- ファイルグループごとの結合処理開始 ---")
    print(f"加工済みファイルフォルダ: {PROCESSED_OUTPUT_BASE_DIR}")
    print(f"結合済みファイル出力フォルダ: {MERGED_OUTPUT_BASE_DIR}")

    # 結合済みファイル出力フォルダが存在しない場合は作成
    os.makedirs(MERGED_OUTPUT_BASE_DIR, exist_ok=True)

    files_to_merge_by_group = {}
    
    for root, dirs, files in os.walk(PROCESSED_OUTPUT_BASE_DIR): 
        for filename in files:
            if filename.lower().endswith('_processed.csv'):
                match = re.match(r'^(B\d{6})_(\d+)\.jpg_020_processed\.csv$', filename, re.IGNORECASE)
                if match:
                    group_root_name = match.group(1) 
                    page_num = int(match.group(2))   
                    filepath = os.path.join(root, filename)

                    if group_root_name not in files_to_merge_by_group:
                        files_to_merge_by_group[group_root_name] = []
                    files_to_merge_by_group[group_root_name].append((page_num, filepath))
                else:
                    print(f"  ℹ️ マージ対象外のファイル形式 (パターン不一致): {filename}")

    merged_files_count = 0
    sorted_merged_groups = sorted(files_to_merge_by_group.keys())

    # ocr_result_id の時刻部分を、実際に読み込んだファイルから取得する
    # 各グループの最初のファイルを読み込んで、ocr_result_id の yyyymmddhhmm 部分を取得する
    group_actual_ocr_ids = {}

    for group_root_name in sorted_merged_groups:
        page_files = files_to_merge_by_group[group_root_name]
        page_files.sort(key=lambda x: x[0]) # ページ番号でソート
        
        if page_files: # 少なくとも1つのファイルがある場合
            first_filepath_in_group = page_files[0][1] # 最初のファイルのパス
            try:
                df_first_page = pd.read_csv(first_filepath_in_group, encoding='utf-8-sig', dtype=str, nrows=1) # 1行だけ読み込む
                if not df_first_page.empty and 'ocr_result_id' in df_first_page.columns:
                    group_actual_ocr_ids[group_root_name] = df_first_page.iloc[0]['ocr_result_id']
                else:
                    print(f"  ⚠️ 警告: グループ '{group_root_name}' の最初のファイルからocr_result_idを読み込めませんでした。このグループは結合されません。")
                    files_to_merge_by_group[group_root_name] = [] # 結合対象から除外
            except Exception as e:
                print(f"  ❌ エラー: グループ '{group_root_name}' の最初のファイル ({os.path.basename(first_filepath_in_group)}) 読み込み中にエラー。エラー: {e}")
                files_to_merge_by_group[group_root_name] = [] # 結合対象から除外


    for group_root_name in sorted_merged_groups: 
        page_files = files_to_merge_by_group[group_root_name]

        if not page_files: # 最初のファイルでエラーがあったなどで結合対象から除外された場合
            continue 
        
        expected_ocr_id_for_group = group_actual_ocr_ids.get(group_root_name)
        if not expected_ocr_id_for_group: # IDが取得できなかったグループはスキップ
            continue
        
        # Bを除いた6桁
        expected_cif_number_for_group = group_root_name[1:]
        expected_jgroupid_string_for_group = '001'
        
        combined_df = pd.DataFrame(columns=FINAL_POSTGRE_COLUMNS) 
        
        print(f"  → グループ '{group_root_name}' のファイルを結合中 (期待ID: {expected_ocr_id_for_group})...")
        
        global_id_counter = 1 

        for page_index, (page_num, filepath) in enumerate(page_files):
            try:
                df_page = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str)
                
                # ファイルが空の場合
                if df_page.empty: 
                    print(f"    ℹ️ {os.path.basename(filepath)} は空のためスキップします。")
                    continue

                # 読み込んだ df_page の ID データを厳密にチェックするロジックを強化
                current_file_ocr_id = df_page.iloc[0]['ocr_result_id'] if 'ocr_result_id' in df_page.columns else None
                current_file_jgroupid = df_page.iloc[0]['jgroupid_string'] if 'jgroupid_string' in df_page.columns else None
                current_file_cif = df_page.iloc[0]['cif_number'] if 'cif_number' in df_page.columns else None

                # ocr_result_id のチェック
                if current_file_ocr_id != expected_ocr_id_for_group:
                    print(f"  ❌ エラー: グループ '{group_root_name}' のファイル '{os.path.basename(filepath)}' でocr_result_idの不一致を検出しました。期待: {expected_ocr_id_for_group}, 実際: {current_file_ocr_id}。このファイルは結合しません。")
                    continue 

                # cif_number のチェック
                if current_file_cif != expected_cif_number_for_group:
                    print(f"  ❌ エラー: グループ '{group_root_name}' のファイル '{os.path.basename(filepath)}' でcif_numberの不一致を検出しました。期待: {expected_cif_number_for_group}, 実際: {current_file_cif}。このファイルは結合しません。")
                    continue 

                # jgroupid_string のチェックと修正
                if current_file_jgroupid != expected_jgroupid_string_for_group:
                    print(f"  ⚠️ 警告: グループ '{group_root_name}' のファイル '{os.path.basename(filepath)}' でjgroupid_stringの不一致を検出しました。期待: {expected_jgroupid_string_for_group}, 実際: {current_file_jgroupid}。結合後、強制的に'{expected_jgroupid_string_for_group}'に修正します。")
                    df_page['jgroupid_string'] = expected_jgroupid_string_for_group # 強制修正

                # 結合前にカラム順をFINAL_POSTGRE_COLUMNSに合わせる
                df_page = df_page[FINAL_POSTGRE_COLUMNS] 

                # df_page: 'id' はマージされる各ファイル内で1からの連番に振り直す
                df_page['id'] = range(global_id_counter, global_id_counter + len(df_page))
                global_id_counter += len(df_page) 

                # page_no: 元の値を維持する
                df_page['page_no'] = 1 
                
                combined_df = pd.concat([combined_df, df_page], ignore_index=True)
                print(f"    - ページ {page_num} ({os.path.basename(filepath)}) を結合しました。")
            except Exception as e:
                print(f"  ❌ エラー: ページ {page_num} のファイル {os.path.basename(filepath)} の読み込み/結合中に問題が発生しました。エラー: {e}")
                import traceback 
                traceback.print_exc() # デバッグのため詳細なトレースバックを出力
                # エラーが発生した場合でも、他のファイルを結合し続けるために空のDataFrameを追加
                combined_df = pd.concat([combined_df, pd.DataFrame(columns=FINAL_POSTGRE_COLUMNS)], ignore_index=True)


        # 結合されたDataFrameをmerged_output_filenameに保存
        merged_output_filename = f"{group_root_name}_processed_merged.csv"
        merged_output_filepath = os.path.join(MERGED_OUTPUT_BASE_DIR, merged_output_filename)
        
        try:
            if not combined_df.empty: 
                combined_df.to_csv(merged_output_filepath, index=False, encoding='utf-8-sig')
                merged_files_count += 1
                print(f"  ✅ グループ '{group_root_name}' の結合ファイルを保存しました: {merged_output_filepath}")
            else:
                print(f"  ⚠️ 警告: グループ '{group_root_name}' に結合対象の有効なデータが見つからなかったため、ファイルは保存されません。")
        except Exception as e:
            print(f"  ❌ エラー: グループ '{group_root_name}' の結合ファイルの保存中に問題が発生しました。エラー: {e}")

    print(f"\n--- ファイルグループごとの結合処理完了 ---")
    print(f"🎉 結合されたファイルグループ数: {merged_files_count} 🎉")

if __name__ == "__main__":
    print(f"--- 結合処理スクリプト開始: {datetime.now()} ---")
    merge_processed_csv_files()
    print(f"\n🎉 全ての結合処理が完了しました！ ({datetime.now()}) 🎉")
    