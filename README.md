# csvRead

## **balance_original(受取手形)**

AIReadで出力したcsvファイルをPostgreSQLに取り込めるように加工するアプリケーションです

## 実行方法

- カレントディレクトリ
`C:\Users\User26\yoko\dev\csvRead\scripts`


- 実行コマンド

検索条件：B*020.csvに絞り込み
```
python filter_and_copy_csv.py
```

加工ファイル作成
```
python process_data.py
```

ファイルをマージ
```
python merge_processed_csv.py
```

- 読取フォルダ(元データ)
```
G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv
```

- 出力フォルダ

絞り込み結果

```
G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\10_受取手形\Import'
```

加工済み
```
C:\Users\User26\yoko\dev\csvRead\processed_output
```

マージ済み
```
C:\Users\User26\yoko\dev\csvRead\merged_output
```

- フローチャート

![alt text](image.png)



## レイヤー構成
```
csvRead/
├── master_data/
│   ├── master.csv
│   └── jgroupid_master.csv
├── processed_output/                   // process_data.py が加工したファイルを保存する場所
│   ├── B000001_1.jpg_020_processed.csv
│   ├── B000001_2.jpg_020_processed.csv
│   └── ...
├── merged_output/                      // merge_processed_csv.py が結合したファイルを保存する場所
│   ├── B000001_processed_merged.csv
│   ├── B000002_processed_merged.csv
│   └── ...
└── scripts/
    ├── filter_and_copy_csv.py          //  検索＆コピーを担当
    ├── process_data.py                 //  加工を担当
    ├── merge_processed_csv.py          //  マージ（結合）を担当
    └── insert_to_postgres.py           //  DB登録を担当
```


### 1. データ取得・準備レイヤー (Data Acquisition & Preparation Layer)
このレイヤーは、外部のデータソースから必要なCSVファイルを取得し、次の加工ステップのためにローカル環境に準備する役割を担っています。

- 担当スクリプト: filter_and_copy_csv.py

- 主な役割:

    指定された共有ドライブのパス (INPUT_BASE_DIR) から、特定のファイル名パターン（例: B*020.csv）に合致するCSVファイルを検索します。

    見つかったファイルを、加工のための一時的な保管場所 (SEARCH_RESULT_OUTPUT_BASE_DIR / filtered_originals) にコピーします。

    この際、元のフォルダ構造（例: 池上、中島、唐木）は引き継がず、フラットな構造で保存します。

### 2. データ加工レイヤー (Data Transformation Layer)
このレイヤーは、取得した個々のCSVファイルを読み込み、PostgreSQLでの利用に適した形式に変換する役割を担っています。

- 担当スクリプト: process_data.py

- 主な役割:

    `filtered_originals`: フォルダ内の個々のCSVファイルを読み込みます。

    データ内の「〃」（同上）を直上のデータで埋め戻します（元のブランクは維持）。

    「合計」「小計」「計」など、集計行を示す特定のキーワードを含む行を削除します。

    各ファイルグループ（例: B000001）に一意の ocr_result_id を割り当てます。

    jgroupid_string を固定値 001 に設定します。

    cif_number をファイル名から抽出した6桁（例: B000001 から 000001）に設定します。

    その他、Excel関数で定義された複雑なカラム（例: issue_date_original、paying_bank_name など）の計算と整形を行います。

    加工されたファイルを processed_output フォルダに _processed.csv というサフィックスを付けて保存します。

### 3. データ結合・出力レイヤー (Data Merging & Output Layer)
このレイヤーは、加工済みの個々のファイルをファイルグループごとに結合し、最終的な出力ファイルを生成する役割を担っています。

- 担当スクリプト: merge_processed_csvs.py

- 主な役割:

    `processed_output`: フォルダ内の全ての `_processed.csv` ファイルをスキャンします。

    ファイル名からファイルグループ（例: B000001）とページ番号を抽出し、同じファイルグループのファイルを特定します。

    各ファイルグループ内のファイルをページ番号の昇順にソートします。

    ソートされたファイルを読み込み、結合します（ocr_result_id や cif_number などは結合後も一貫性を保ちます）。

    結合されたファイルの id カラムを、結合後全体の連番に再採番します。

    結合されたファイルを `merged_output` フォルダに `_processed_merged.csv` というサフィックスを付けて保存します。


## ルール

### カラム

- **ocr_result_id**: ファイルごとに一意

    最後0で18桁にする `yyyymmddhhmmsssss0`

- **page_no**: 全て`1`で固定

- **id**: ファイルごとに連番

- **jgroupid_string(店番)**: 全て`001`で固定

- **cif_number(顧客番号)**: ファイルごとに一意

    ファイル番号の数字部分6桁(B000050->000050)

- **maker_name_original**,**maker_name**: ランダム

- **maker_com_code**: 頭に`2`を追加した3桁の自動採番でカウントアップ

    会社に紐づく(maker_nameが同じならmaker_com_codeも同じ)


### データ

- **〃**: 1つ上の項目に合わせる

- **合計**: 行ごと削除(小計、計などの合計行)


## Excel関数ルール

### ocr_result_id =IF(J4<>"",$L$1,"")

- セル J4 に何か値が入っていたら、$L$1（つまりL1セルの値）を返す
- 空だったら ""（空）を返す

### page_no =IF(L4<>"", REPLACE(REPLACE(A$2,FIND(".jpg",A$2),100,""), 1,FIND("_",A$2),""),"")
- L4 に値があったら、ファイル名（例：aaa_0001.jpg）からページ番号を抜き出す
- .jpg 以降を削除
- _ より前の部分を削除

### id =ROW()-3
- 自分の行番号（ROW）から3を引いて、1から始まる連番を作る。

    例：4行目 → ROW()=4 → 4-3=1 → ID=1

### cif_number =IF(L4<>"", $C$2,"")
-L4 に値があれば、C2 の値（顧客番号など）を返す。
- 空なら ""

### settlement_at =IF(L4<>"", $D$2,"")
- L4 に値があれば、D2 の値（日付など）を返す。
- 空なら ""

### maker_name_original =IF(L4<>"", $D$2,"")
- L4 に値があれば、D2 の値（元のメーカー名）を返す。

### maker_name =IF(A4<>"",A4,"")
- A4 にファイル名が入っていれば、それを「メーカー名」として使う。

### maker_com_code =IF(L4<>"", VLOOKUP($S4,$S$47:$T$86,2,FALSE), "")
- L4 に値があれば$S4（メーカー名）を $S$47:$T$86 という表から探して2列目（会社コード）を返す。
- 空なら ""
