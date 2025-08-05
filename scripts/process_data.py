import pandas as pd
import os
import re
import shutil 
from datetime import datetime 
import json 
import glob # glob モジュールは新しいロジックで必須

# 設定項目
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead'

# 加工済みファイルがあるフォルダ
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
# マージ済みファイルを保存するフォルダ
MERGED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'merged_output') 
# マスタデータフォルダ（ocr_id_mapping_notesReceivable.json が保存されている場所）    
MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data')

# このリストは process_data.py の FINAL_POSTGRE_COLUMNS と完全に一致している必要がある
FINAL_POSTGRE_COLUMNS = [
    'ocr_result_id',
    'page_no',
    'id',
    'jgroupid_string',
    'cif_number',
    'settlement_at',
    'registration_number_original',
    'registration_number',
    'maker_name_original',
    'maker_name',
    'maker_com_code',
    'maker_com_code_status_id',
    'maker_comcd_relation_source_type_id',
    'maker_exist_comcd_relation_history_id',
    'issue_date_original',
    'issue_date',
    'due_date_original',
    'due_date',
    'paying_bank_name_original',
    'paying_bank_name',
    'paying_bank_code',
    'paying_bank_branch_name_original',
    'paying_bank_branch_name',
    'balance_original',
    'balance',
    'discount_bank_name_original',
    'discount_bank_name',
    'discount_bank_code',
    'description_original',
    'description',
    'conf_registration_number',
    'conf_maker_name',
    'conf_issue_date',
    'conf_due_date',
    'conf_balance',
    'conf_paying_bank_name',
    'conf_paying_bank_branch_name',
    'conf_discount_bank_name',
    'conf_description',
    'coord_x_registration_number',
    'coord_y_registration_number',
    'coord_h_registration_number',
    'coord_w_registration_number',
    'coord_x_maker_name',
    'coord_y_maker_name',
    'coord_h_maker_name',
    'coord_w_maker_name',
    'coord_x_issue_date',
    'coord_y_issue_date',
    'coord_h_issue_date',
    'coord_w_issue_date',
    'coord_x_due_date',
    'coord_y_due_date',
    'coord_h_due_date',
    'coord_w_due_date',
    'coord_x_balance',
    'coord_y_balance',
    'coord_h_balance',
    'coord_w_balance',
    'coord_x_paying_bank_name',
    'coord_y_paying_bank_name',
    'coord_h_paying_bank_name',
    'coord_w_paying_bank_name',
    'coord_x_paying_bank_branch_name',
    'coord_y_paying_bank_branch_name',
    'coord_h_paying_bank_branch_name',
    'coord_w_paying_bank_branch_name',
    'coord_x_discount_bank_name',
    'coord_y_discount_bank_name',
    'coord_h_discount_bank_name',
    'coord_w_discount_bank_name',
    'coord_x_description',
    'coord_y_description',
    'coord_h_description',
    'coord_w_description',
    'row_no',
    'insertdatetime',
    'updatedatetime',
    'updateuser'             
]

# 金額っぽい値かを判定する関数
def is_money(value: str) -> bool:
    """金額っぽい値（数字のみまたはカンマ区切り）かを判定"""
    if isinstance(value, str):
        # カンマ、円マークなどを除去してから判定
        value = value.replace(",", "").replace("¥", "").replace("￥", "").replace("円", "").strip()
    # 3桁以上の数字、または小数点を含む数字、または符号付き数字を金額と判定
    return re.fullmatch(r"^[+-]?\d{1,}(\.\d+)?$", str(value)) is not None 


def merge_processed_csv_files():
    """
    processed_output フォルダ内の加工済みCSVファイルをファイルグループごとに結合し、
    merged_output フォルダに保存する関数。
    """
    print(f"--- ファイルグループごとの結合処理開始 ({datetime.now()}) ---")
    print(f"加工済みファイルフォルダ: {PROCESSED_OUTPUT_BASE_DIR}")
    print(f"結合済みファイル出力フォルダ: {MERGED_OUTPUT_BASE_DIR}")

    os.makedirs(MERGED_OUTPUT_BASE_DIR, exist_ok=True)

    # ★★★ お客様の新しいマージロジックを全面的に採用 ★★★
    all_data_frames = [] # 各ファイルのデータ部分（ヘッダーなし）を格納するリスト

    # 対象ファイルをすべて取得 (recursive=True でサブディレクトリも検索)
    csv_files_to_merge = glob.glob(os.path.join(PROCESSED_OUTPUT_BASE_DIR, '**', '*_processed.csv'), recursive=True)

    if not csv_files_to_merge:
        print("⚠️ 警告: マージ対象のファイルが見つかりませんでした。")
        print(f"\n--- ファイルグループごとの結合処理完了 ({datetime.now()}) ---")
        print(f"🎉 結合されたファイルグループ数: 0 🎉")
        return

    # グループ名は「all」にする（お客様の指示）
    group_name = 'all' # BXXXXXX_merged.csv ではなく all_merged.csv にするため
    output_file_path = os.path.join(MERGED_OUTPUT_BASE_DIR, f'{group_name}_merged.csv')

    print(f"  → 全てのファイルを結合し、'{group_name}' グループとして保存します。")

    for file_path in sorted(csv_files_to_merge): # ファイルパスをソートして結合順を保証
        try:
            # _processed.csv を読み込む
            # header=0 で読み込み、ヘッダーを正しく認識させる。
            df_current_file = pd.read_csv(file_path, encoding='utf-8-sig', dtype=str, header=0, na_values=['〃'], keep_default_na=False)
            
            if df_current_file.empty: 
                print(f"    ℹ️ {os.path.basename(file_path)} は空のためスキップします。")
                continue

            # デバッグ情報: 読み込み直後のカラム数と一覧
            actual_cols = df_current_file.columns.tolist()
            print(f"    📄 ファイル {os.path.basename(file_path)} 読み込み直後のカラム数: {len(actual_cols)}")
            print(f"    🧩 ファイル {os.path.basename(file_path)} 読み込み直後のカラム一覧: {actual_cols}")

            # 想定される最終カラム数と一致するかを厳密にチェック
            if len(actual_cols) != len(FINAL_POSTGRE_COLUMNS):
                print(f"    ⚠️ 警告: ファイル {os.path.basename(file_path)} の列数が想定と異なります（{len(actual_cols)}列 vs 期待 {len(FINAL_POSTGRE_COLUMNS)}列）。このファイルはスキップされます。")
                continue # 列数が一致しない場合はスキップ

            # 列名に重複がないかチェック
            if len(set(actual_cols)) != len(actual_cols):
                print(f"    ⚠️ 警告: ファイル {os.path.basename(file_path)} で重複する列名が検出されました → {actual_cols}。このファイルはスキップされます。")
                continue # 列名に重複がある場合もスキップ

            # df_current_file のカラム名を FINAL_POSTGRE_COLUMNS に強制的に設定
            # これにより、df_current_fileの物理的なデータとFINAL_POSTGRE_COLUMNSの名前が正しく紐づきます。
            # reindexを使用することで、もし元の_processed.csvの物理的な順序が若干異なっても、
            # カラム名に基づいてデータを正しく配置し、存在しないカラムはNaN/空で埋めます。
            df_current_file = df_current_file.reindex(columns=FINAL_POSTGRE_COLUMNS).fillna('')
            
            # balance列の金額チェック（保存前に整形）
            # is_money関数を使用して、金額として有効な値のみを保持する
            for col in ['balance_original', 'balance']:
                if col in df_current_file.columns:
                    df_current_file[col] = df_current_file[col].apply(lambda x: x if is_money(x) else "")

            all_data_frames.append(df_current_file)
            print(f"    - {os.path.basename(file_path)} のデータを結合リストに追加しました。")

        except Exception as e:
            print(f"  ❌ エラー: ファイル {os.path.basename(file_path)} の読み込み/処理中に問題が発生しました。エラー: {e}")
            import traceback 
            traceback.print_exc() 

    if not all_data_frames:
        print("⚠️ 警告: 結合対象の有効なデータが見つからなかったため、マージは行われません。")
        print(f"\n--- ファイルグループごとの結合処理完了 ({datetime.now()}) ---")
        print(f"🎉 結合されたファイルグループ数: 0 🎉")
        return

    # 全てのデータフレームを結合
    merged_df = pd.concat(all_data_frames, ignore_index=True)
    
    # 最終的なocr_result_id, page_no, id, jgroupid_string, cif_number, settlement_at の設定
    # combined_dfは all_data_frames の concat なので、すでにこれらのカラムは含まれている
    # ocr_id_mapping_from_file から expected_ocr_id_for_group を取得する必要があるが、
    # 'all'グループのファイルには特定のOCR_IDが紐づかない。
    # ここでは、全データが結合された後、ocr_result_idはそのまま保持され、
    # その他の ID (page_no, id, jgroupid_string, cif_number) は個々の行の値を信頼する。
    # ocr_result_idのNoneはmerge_processed_csvs_notesPayable.pyのocr_id_map_filepathの解決で対応済みのはず。

    # 最終的な金額列のチェックとクリーンアップ（この段階で最後の保証）
    for col in ['balance_original', 'balance']:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].apply(lambda x: x if is_money(x) else "")
    print(f"  ℹ️ 最終マージ済みDataFrameの'balance_original'と'balance'カラムの金額チェックとクリーンアップを行いました。")


    # 結合されたDataFrameを保存
    try:
        # header=False で保存 (PostgreSQL COPYコマンド向け)
        merged_df.to_csv(output_file_path, index=False, header=False, encoding='utf-8-sig')
        print(f"✅ 全てマージ完了！→ {output_file_path}")
    except Exception as e:
        print(f"❌ エラー: マージ済みファイル '{output_file_path}' の保存中に問題が発生しました。エラー: {e}")
        import traceback
        traceback.print_exc()


    print(f"\n--- ファイルグループごとの結合処理完了 ({datetime.now()}) ---")
    print(f"🎉 結合されたファイルグループ数: 1 (allグループ) 🎉") # グループはall一つなので常に1
    print(f"\n🎉 全ての結合処理が完了しました！ ({datetime.now()}) 🎉")

# --- メイン処理 ---
if __name__ == "__main__":
    print(f"--- 結合処理スクリプト開始: {datetime.now()} ---")
    merge_processed_csv_files()
    print(f"\n🎉 全ての結合処理が完了しました！ ({datetime.now()}) 🎉")
    