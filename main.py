# coding: UTF-8
import sqlite3
import pandas as pd

#データファイル
filepath_data_csv = r"data.csv"
#マスタファイル
filepath_mst_csv = r"master.csv"
#結果ファイル名
filepath_result_csv = r"result.csv"

#作業DB
filepath_db = r"data.db"
#一時テーブル
sql_create_tmp = "CREATE TABLE IF NOT EXISTS tmp(id integer,name text)"
#マスタテーブル
sql_create_mst = "CREATE TABLE IF NOT EXISTS mst(id integer,name text)"

#変換用SQL
sql_select_convert ='''
SELECT t1.id          AS id
      ,upper(t1.name) AS name 
  FROM tmp t1
  JOIN mst t2
    ON t1.name = t2.name
'''

def create_table():
    #DBをに接続してテーブルを作成
    with sqlite3.connect(filepath_db) as conn:
        cur = conn.cursor()
        #一時テーブル
        cur.execute(sql_create_tmp)
        #マスタテーブル
        cur.execute(sql_create_mst)
        #コミット
        conn.commit()

def insert_mst():
    # CSV 読込
    # headerあり(なしの場合None)
    df = pd.read_csv(filepath_mst_csv, header=0, names=['id', 'name'])
    # DBをに接続
    with sqlite3.connect(filepath_db) as conn:
        # DBにデータを挿入（データを置換）
        df.to_sql("mst", con=conn, if_exists='replace')

def insert_data():
    # CSV 読込
    # headerあり(なしの場合None)
    df = pd.read_csv(filepath_data_csv, header=0, names=['id', 'name'])
    # DBをに接続
    with sqlite3.connect(filepath_db) as conn:
        # DBにデータを挿入（データを置換）
        df.to_sql("tmp", con=conn, if_exists='replace')

def convert():
    with sqlite3.connect(filepath_db) as conn:
        # SQLを実行し、結果を取得
        df = pd.read_sql_query(sql_select_convert,conn)
        # CSVに保存する
        # INDEXを出力しない
        df.to_csv(filepath_result_csv, index=False, header=['id', 'name'])

if __name__ == '__main__':
    # テーブル作成
    create_table()
    # マスタ挿入
    insert_mst()
    # データ挿入
    insert_data()
    # 変換処理＆出力
    convert()
