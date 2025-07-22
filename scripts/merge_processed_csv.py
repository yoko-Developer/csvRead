import pandas as pd
import os
import re
import shutil 
from datetime import datetime 

# 設定項目
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'

# 加工済みファイルがあるフォルダ
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
# マージ済みファイルを保存するフォルダ
MERGED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'merged_output') 

# このリストは process_data.py の FINAL_POSTGRE_COLUMNS と完全に一致している必要がある
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


def merge_processed_csv_files():
    """
    processed_output フォルダ内の加工済みCSVファイルをファイルグループごとに結合し、
    merged_output フォルダに保存する関数。
    """
    print(f"--- ファイルグループごとの結合処理開始 ---")
    print(f"加工済みファイルフォルダ: {PROCESSED_OUTPUT_BASE_DIR}")
    print(f"結合済みファイル出力フォルダ: {MERGED_OUTPUT_BASE_DIR}")

    # 結合済みファイル出力フォルダが存在しない場合は作成
    os.makedirs(MERGED_OUTPUT_BASE_DIR, exist_ok=True)

    files_to_merge_by_group = {}
    
    # processed_output フォルダ内を再帰的に検索
    for root, dirs, files in os.walk(PROCESSED_OUTPUT_BASE_DIR): 
        for filename in files:
            # '_processed.csv' で終わるファイルのみを対象とする
            if filename.lower().endswith('_processed.csv'):
                # ファイル名から「ファイルグループのルート名」と「ページ番号」を抽出
                # 例: B000001_1.jpg_020_processed.csv -> group_root="B000001", page_num="1"
                match = re.match(r'^(B\d{6})_(\d+)\.jpg_020_processed\.csv$', filename, re.IGNORECASE)
                if match:
                    group_root_name = match.group(1) # 例: B000001
                    page_num = int(match.group(2))   # 例: 1 (ページ番号)
                    filepath = os.path.join(root, filename)

                    if group_root_name not in files_to_merge_by_group:
                        files_to_merge_by_group[group_root_name] = []
                    files_to_merge_by_group[group_root_name].append((page_num, filepath))
                else:
                    print(f"  ℹ️ マージ対象外のファイル形式 (パターン不一致): {filename}")

    merged_files_count = 0
    # ファイルグループのルート名でソートして、結合順を保証
    sorted_merged_groups = sorted(files_to_merge_by_group.keys())

    for group_root_name in sorted_merged_groups: # ソートされたグループ名でループ
        page_files = files_to_merge_by_group[group_root_name]
        # 各ファイルグループ内で、ページ番号の昇順でファイルをソート
        page_files.sort(key=lambda x: x[0]) 

        combined_df = pd.DataFrame(columns=FINAL_POSTGRE_COLUMNS) # 最終カラム順で初期化
        
        print(f"  → グループ '{group_root_name}' のファイルを結合中...")
        
        # 結合されたDF全体でのidを再採番するためのカウンター
        global_id_counter = 1 

        # このグループの ocr_result_id, cif_number, jgroupid_string の期待値は、最初のファイルの値を採用
        expected_ocr_id_for_group = None
        expected_cif_number_for_group = None
        expected_jgroupid_string_for_group = '001' # Jgroupidは常に001固定

        # 最初のファイルを読み込んで、IDの期待値を設定（チェック用）
        if page_files:
            first_page_filepath = page_files[0][1]
            try:
                df_first_page_for_check = pd.read_csv(first_page_filepath, encoding='utf-8-sig', dtype=str, nrows=1)
                if not df_first_page_for_check.empty:
                    expected_ocr_id_for_group = df_first_page_for_check.iloc[0]['ocr_result_id'] if 'ocr_result_id' in df_first_page_for_check.columns else None
                    expected_cif_number_for_group = df_first_page_for_check.iloc[0]['cif_number'] if 'cif_number' in df_first_page_for_check.columns else None
            except Exception as e:
                print(f"  ❌ エラー: グループ '{group_root_name}' の最初のファイル ({os.path.basename(first_page_filepath)}) 読み込み中にエラー。ID期待値取得不可。このグループは結合されません。エラー: {e}")
                continue # このグループの結合をスキップ


        for page_index, (page_num, filepath) in enumerate(page_files):
            try:
                df_page = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str)
                
                if df_page.empty: 
                    print(f"    ℹ️ {os.path.basename(filepath)} は空のためスキップします。")
                    continue

                # ID情報の不一致チェックロジックを緩和し、ファイルから読み込んだ値を信頼する
                current_file_ocr_id = df_page.iloc[0]['ocr_result_id'] if 'ocr_result_id' in df_page.columns else 'N/A_NoCol'
                current_file_jgroupid = df_page.iloc[0]['jgroupid_string'] if 'jgroupid_string' in df_page.columns else 'N/A_NoCol'
                current_file_cif = df_page.iloc[0]['cif_number'] if 'cif_number' in df_page.columns else 'N/A_NoCol'
                
                print(f"    デバッグ: {os.path.basename(filepath)} のID情報: OCR={current_file_ocr_id}, JG={current_file_jgroupid}, CIF={current_file_cif}")

                # 警告は出すが、強制的に期待値に修正する
                if expected_ocr_id_for_group and current_file_ocr_id != expected_ocr_id_for_group and current_file_ocr_id != 'N/A_NoCol':
                    print(f"  ⚠️ 警告: グループ '{group_root_name}' のファイル '{os.path.basename(filepath)}' でocr_result_idの不一致を検出。ファイルの値 ({current_file_ocr_id}) を'{expected_ocr_id_for_group}'に強制修正します。")
                    df_page['ocr_result_id'] = expected_ocr_id_for_group # 強制修正

                if expected_cif_number_for_group and current_file_cif != expected_cif_number_for_group and current_file_cif != 'N/A_NoCol':
                    print(f"  ⚠️ 警告: グループ '{group_root_name}' のファイル '{os.path.basename(filepath)}' でcif_numberの不一致を検出。ファイルの値 ({current_file_cif}) を'{expected_cif_number_for_group}'に強制修正します。")
                    df_page['cif_number'] = expected_cif_number_for_group # 強制修正

                if current_file_jgroupid != expected_jgroupid_string_for_group and current_file_jgroupid != 'N/A_NoCol':
                    print(f"  ⚠️ 警告: グループ '{group_root_name}' のファイル '{os.path.basename(filepath)}' でjgroupid_stringの不一致を検出しました。期待: {expected_jgroupid_string_for_group}, 実際: {current_file_jgroupid}。結合後、強制的に'{expected_jgroupid_string_for_group}'に修正します。")
                
                # ocr_result_id, cif_number, jgroupid_string を上書き修正
                df_page['ocr_result_id'] = expected_ocr_id_for_group
                df_page['cif_number'] = expected_cif_number_for_group
                df_page['jgroupid_string'] = expected_jgroupid_string_for_group


                # 結合前にカラム順をFINAL_POSTGRE_COLUMNSに合わせる（重要）
                df_page = df_page[FINAL_POSTGRE_COLUMNS] 

                # df_page の 'id' は、マージされる各ファイル内で1から始まるため、ここで全体の連番に振り直す。
                df_page['id'] = range(global_id_counter, global_id_counter + len(df_page))
                global_id_counter += len(df_page) 

                # page_no はお客様のご要望で1固定なので、元の値を維持する (または全て1にする)
                df_page['page_no'] = 1 
                
                combined_df = pd.concat([combined_df, df_page], ignore_index=True)
                print(f"    - ページ {page_num} ({os.path.basename(filepath)}) を結合しました。")
            except Exception as e:
                print(f"  ❌ エラー: ページ {page_num} のファイル {os.path.basename(filepath)} の読み込み/結合中に問題が発生しました。エラー: {e}")
                import traceback 
                traceback.print_exc() 
                combined_df = pd.concat([combined_df, pd.DataFrame(columns=FINAL_POSTGRE_COLUMNS)], ignore_index=True)


        # ファイル名を B000001_merged.csv に変更
        merged_output_filename = f"{group_root_name}_merged.csv" # ここを修正
        merged_output_filepath = os.path.join(MERGED_OUTPUT_BASE_DIR, merged_output_filename)
        
        # 古いファイルを削除
        old_filename_pattern = f"{group_root_name}_processed_merged.csv"
        old_filepath = os.path.join(MERGED_OUTPUT_BASE_DIR, old_filename_pattern)
        if os.path.exists(old_filepath):
            try:
                os.remove(old_filepath)
                print(f"  ✅ 古いファイル '{old_filename_pattern}' を削除しました。")
            except Exception as e:
                print(f"  ❌ エラー: 古いファイル '{old_filename_pattern}' の削除中に問題が発生しました。エラー: {e}")

        try:
            if not combined_df.empty: # 結合結果が空でない場合のみ保存
                # header=False を指定してヘッダ行を削除
                combined_df.to_csv(merged_output_filepath, index=False, encoding='utf-8-sig', header=False) 
                merged_files_count += 1
                print(f"  ✅ グループ '{group_root_name}' の結合ファイルを保存しました: {merged_output_filepath}")
            else:
                print(f"  ⚠️ 警告: グループ '{group_root_name}' に結合対象の有効なデータが見つからなかったため、ファイルは保存されません。")
        except Exception as e:
            print(f"  ❌ エラー: グループ '{group_root_name}' の結合ファイルの保存中に問題が発生しました。エラー: {e}")

    print(f"ファイルグループごとの結合処理完了")
    print(f"🎉 結合されたファイルグループ数: {merged_files_count} 🎉")

if __name__ == "__main__":
    print(f"--- 結合処理スクリプト開始: {datetime.now()} ---")
    merge_processed_csv_files()
    print(f"\n🎉 全ての結合処理が完了しました！ ({datetime.now()}) 🎉")
    