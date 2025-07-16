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

# 元のCSVファイルの最大カラム数に合わせて調整してね。
# 例えば、一番横に長いCSVが20カラムあるなら 20 を設定する。
# これより少ないとデータが欠ける可能性があるよ。
MAX_GENERIC_COLUMNS = 20 # 汎用カラムの数を20に増やしたよ。必要ならさらに増やしてね！

# --- 関数定義（この部分は変更しないでね！） ---
def process_any_csv(input_filepath, output_base_dir, input_base_dir, max_generic_cols, maker_master_df, jgroupid_master_df):
    """
    任意の形式のCSVファイルを読み込み、汎用的なカラム形式で出力する関数
    """
    try:
        # CSVファイルを読み込む
        # 「ふつうのUTF-8」とのことなので、encoding='utf-8' を直接指定するよ。
        # header=None でヘッダーを読み込まず、すべての行をデータとして扱う。
        df_original = pd.read_csv(input_filepath, encoding='utf-8', header=None)
        print(f"  ファイル {os.path.basename(input_filepath)} を UTF-8 で読み込み成功。")
            
    except UnicodeDecodeError as ude:
        # UTF-8で読み込めなかった場合の処理。
        # もしUTF-8-BOM付きのCSVが混じっていた場合に備えて、一度 'utf-8-sig' も試してみるよ。
        try:
            df_original = pd.read_csv(input_filepath, encoding='utf-8-sig', header=None)
            print(f"  ファイル {os.path.basename(input_filepath)} を UTF-8-BOM で読み込み成功。")
        except Exception as e_sig:
            # どちらでも読み込めなかったらエラーとして報告
            print(f"❌ エラー発生（{input_filepath}）: UTF-8でもUTF-8-BOMでも読み込めませんでした。エラー: {ude}, {e_sig}")
            import traceback
            traceback.print_exc()
            return # エラーが発生した場合はこのファイルの処理を中断して次へ
    except Exception as e:
        # その他の予期せぬエラーの場合
        print(f"❌ エラー発生（{input_filepath}）: CSV読み込み中に予期せぬ問題が発生しました。エラー: {e}")
        import traceback
        traceback.print_exc()
        return # エラーが発生した場合はこのファイルの処理を中断して次へ

    # --- データ加工処理（ここから下は変更しないでね） ---
    df_processed = pd.DataFrame() # 関数の先頭に移動済み

    # ファイルが手形情報フォーマットかどうかを判断するフラグ
    is_bill_format = False
    
    # 元のCSVのヘッダー（または最初の行のデータ）を見て判断
    if not df_original.empty:
        # 最初の行のデータを文字列として結合して確認 (デバッグ用)
        first_row_content = " ".join(df_original.iloc[0, :].astype(str).values)
        print(f"  デバッグ（{os.path.basename(input_filepath)}）: 最初の行の内容: '{first_row_content}'")
        
        # 「振出人」が最初の行に含まれているかチェック
        # この条件で手形情報かどうかを判断する
        if '振出人' in first_row_content:
             is_bill_format = True
        
        print(f"  デバッグ（{os.path.basename(input_filepath)}）: is_bill_format = {is_bill_format}")

    if is_bill_format:
        # --- 手形情報の加工ロジック ---
        # df_airead に相当するデータが df_original に入っている

        # ocr_result_id を生成 (ファイルごとに一意)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        random_suffix = str(random.randint(0, 9999)).zfill(4) 
        ocr_result_id = f"{timestamp}{random_suffix}"

        # page_no: ファイル名からページ番号を抽出 (例: B000039_2.jpg_030.csv から 2 を抽出)
        page_no_match = re.search(r'_(?P<page_num>\d+)(?:\.jpg_(\d+))?\.csv$', os.path.basename(input_filepath))
        if page_no_match:
            page_no = int(page_no_match.group('page_num'))
        else:
            page_no = 1 # 見つからない場合はデフォルト値

        # jgroupid_string: jgroupid_masterからランダムに1つ選択
        jgroupid_string = "000" # デフォルト値
        if not jgroupid_master_df.empty and 'jgroupid' in jgroupid_master_df.columns:
            jgroupid_string = random.choice(jgroupid_master_df['jgroupid'].tolist())

        # cif_number: ランダムな数字列（6桁の例）
        cif_number = str(random.randint(100000, 999999))

        settlement_at = datetime.now().strftime('%Y%m') # YYYYMM形式

        # 共通データをDataFrameに設定 (ヘッダー行を除くデータ行数分)
        df_data_rows = df_original.iloc[1:].copy() # ヘッダー行をスキップ

        # id: ファイル内でカウントアップ (各行にユニークなID)
        df_processed['id'] = range(1, len(df_data_rows) + 1)
        
        # df_data_rowsのインデックスをリセットして、元の列を結合しやすくする
        df_data_rows.reset_index(drop=True, inplace=True)
        
        # 元のCSVのデータ列を df_processed に結合
        # ここで、df_originalの列番号がそのままカラム名として使われる
        for col_idx in df_data_rows.columns:
            df_processed[col_idx] = df_data_rows[col_idx]

        # カラム名を元のCSVのヘッダー名にマッピング
        # 例：df_processed.rename(columns={0: '振出人', 1: '振出年月日', ...}, inplace=True)
        # 実際に提供された手形データのカラム順番と名前を基にマッピング
        temp_col_map = {
            0: '振出人', 1: '振出年月日', 2: '支払期日', 3: '支払銀行名称',
            4: '支払銀行支店名', 5: '金額', 6: '割引銀行名及び支店名等', 7: '摘要'
        }
        # 必要なカラムだけリネームし、足りない場合は処理しない
        rename_cols = {c_idx: new_name for c_idx, new_name in temp_col_map.items() if c_idx in df_processed.columns}
        df_processed.rename(columns=rename_cols, inplace=True)

        # maker_name_original と maker_name を設定
        if '振出人' in df_processed.columns:
            df_processed['maker_name_original'] = df_processed['振出人'].fillna('').astype(str)
            df_processed['maker_name'] = df_processed['振出人'].fillna('').astype(str)
        else:
            df_processed['maker_name_original'] = ''
            df_processed['maker_name'] = ''

        # maker_com_code: maker_master_dfからVLOOKUPのように結合
        if '振出人' in df_processed.columns and not maker_master_df.empty:
            df_processed = pd.merge(df_processed, maker_master_df[['会社名', '会社コード']],
                                    left_on='振出人', right_on='会社名', how='left', suffixes=('', '_master'))
            df_processed['maker_com_code'] = df_processed['会社コード'].fillna('').astype(str)
            # 結合に使った一時的なカラムを削除
            df_processed = df_processed.drop(columns=['会社名', '会社コード'], errors='ignore')
        else:
            df_processed['maker_com_code'] = '' # 振出人がないかマスタがない場合は空

        # 日付と金額、銀行情報のマッピング
        mapping_rules = {
            '振出年月日': ['issue_date_rightside_date', 'issue_date'],
            '支払期日': ['due_date_rightside_date', 'due_date'],
            '金額': ['balance_rightside', 'balance'],
            '支払銀行名称': ['payment_bank_name_rightside', 'payment_bank_name'],
            '支払銀行支店名': ['payment_bank_branch_name_rightside', 'payment_bank_branch_name']
        }

        for col_orig, target_cols in mapping_rules.items():
            if col_orig in df_processed.columns:
                for target_col in target_cols:
                    df_processed[target_col] = df_processed[col_orig].fillna('').astype(str)
            else:
                for target_col in target_cols:
                    df_processed[target_col] = ''

        # 割引銀行名及び支店名等 / 摘要 のマッピング
        if '割引銀行名及び支店名等' in df_processed.columns:
            df_processed['description_rightside'] = df_processed['割引銀行名及び支店名等'].fillna('').astype(str)
        else:
            df_processed['description_rightside'] = ''

        if '摘要' in df_processed.columns:
            df_processed['description'] = df_processed['摘要'].fillna('').astype(str)
        else:
            df_processed['description'] = ''
        
        # ocr_result_id, page_no, jgroupid_string, cif_number, settlement_at を結合する
        # この時点でdf_processedにはidと元のデータが結合されている状態
        # これらの値を各行に設定するために、もう一度データフレームを結合する
        fixed_common_data = {
            'ocr_result_id': ocr_result_id,
            'page_no': page_no,
            'jgroupid_string': jgroupid_string,
            'cif_number': cif_number,
            'settlement_at': settlement_at
        }
        # 各行に同じ値を設定
        for k, v in fixed_common_data.items():
            df_processed[k] = v

        # 最終的な列の順序を目標の形式に合わせる
        output_columns_bill = [
            'ocr_result_id', 'page_no', 'id', 'jgroupid_string', 'cif_number',
            'settlement_at', 'maker_name_original', 'maker_name', 'maker_com_code',
            'issue_date_rightside_date', 'issue_date',
            'due_date_rightside_date', 'due_date',
            'balance_rightside', 'balance',
            'payment_bank_name_rightside', 'payment_bank_name',
            'payment_bank_branch_name_rightside', 'payment_bank_branch_name',
            'description_rightside', 'description'
        ]
        # 存在しないカラムは無視して、存在するカラムのみ順序を適用
        # また、元の日本語カラム（振出人など）は最終出力から除外する
        df_processed = df_processed.reindex(columns=[col for col in output_columns_bill if col in df_processed.columns])


    else:
        # --- その他の形式の加工ロジック（以前の「ストレートに吐き出す」処理） ---
        # df_original をそのまま活用し、汎用カラムに割り当てる

        # file_path: 元ファイルのフルパス
        df_processed['file_path'] = input_filepath
        # original_file_name: 元ファイル名
        df_processed['original_file_name'] = os.path.basename(input_filepath)

        # page_index: ファイル名から _1, _2 などを抽出
        page_idx_match = re.search(r'_(?P<page_num>\d+)(?:\.jpg_(\d+))?\.csv$', os.path.basename(input_filepath))
        if page_idx_match:
            df_processed['page_index'] = int(page_idx_match.group('page_num'))
        else:
            first_num_match = re.search(r'(\d+)', os.path.basename(input_filepath))
            if first_num_match:
                df_processed['page_index'] = int(first_num_match.group(1))
            else:
                df_processed['page_index'] = 0

        # row_number: 元CSV内での行番号 (0から始まるDataFrameのインデックス)
        df_processed['row_number'] = df_original.index

        # original_header_text: 最初のカラムの値を格納
        df_processed['original_header_text'] = df_original.iloc[:, 0].fillna('').astype(str)

        # 汎用カラムにデータを格納
        for i in range(max_generic_cols):
            col_name = f'column_{i+1}_value'
            if i < df_original.shape[1]:
                df_processed[col_name] = df_original.iloc[:, i].fillna('').astype(str)
            else:
                df_processed[col_name] = ''
        
        # 出力カラムの順序を定義 (汎用形式用)
        # 全ての汎用カラムが出力されるようにする
        generic_output_columns = ['file_path', 'original_file_name', 'page_index', 'row_number', 'original_header_text'] + \
                                 [f'column_{i+1}_value' for i in range(max_generic_cols)]
        df_processed = df_processed.reindex(columns=generic_output_columns)

    # --- 保存処理（この部分は変更なし） ---
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
    MASTER_DATA_DIR = r'C:\AIRead\master_data' # <--- ここはくまちゃんがマスタを保存したパスに合わせてね！

    # maker_master.csv を読み込む
    maker_master_filepath = os.path.join(MASTER_DATA_DIR, 'maker_master.csv')
    maker_master_df = pd.DataFrame() 
    if os.path.exists(maker_master_filepath):
        try:
            maker_master_df = pd.read_csv(maker_master_filepath, encoding='shift_jis') 
        except Exception as e:
            print(f"❌ エラー: maker_master.csv の読み込みに失敗しました。エンコーディングを確認してください。エラー: {e}")
            maker_master_df = pd.DataFrame({'会社名': [], '会社コード': []}) # 空のDataFrameで継続
    else:
        print(f"⚠️ 警告: maker_master.csv が見つかりません。パスを確認してください: {maker_master_filepath}")
        maker_master_data = {
            '会社名': ['(株)双文社印刷', '(株)太平印刷社', '(株)リーブルテック', '日本ハイコム(株)', '(株)新寿堂', '手持手形計', '割引手形計', '(株)シーフォース'],
            '会社コード': ['4380946945', '9138429316', '2578916640', '5408006886', '0668992415', '9443492307', '4417864013', '7398659210']
        }
        maker_master_df = pd.DataFrame(maker_master_data)

    # jgroupid_master.csv を読み込む
    jgroupid_master_filepath = os.path.join(MASTER_DATA_DIR, 'jgroupid_master.csv')
    jgroupid_master_df = pd.DataFrame() 
    if os.path.exists(jgroupid_master_filepath): # <<-- ここを jgroupid_master_filepath に修正済み！
        try:
            jgroupid_master_df = pd.read_csv(jgroupid_master_filepath, encoding='shift_jis')
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
                # マスタデータを process_any_csv 関数に渡す
                process_any_csv(input_filepath, OUTPUT_BASE_DIR, INPUT_BASE_DIR, MAX_GENERIC_COLUMNS, maker_master_df, jgroupid_master_df)

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    