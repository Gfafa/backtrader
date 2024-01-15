'''
Created on 2020年1月30日

@author: JM
'''
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine 
import time


engine_ts = create_engine('mysql+pymysql://jet:lt67Z,8910ZH@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1')

def read_data(trade_date):
    # TODO mysql查库比较慢，看看有哪些提高速度的办法
    sql = f"""SELECT * FROM daily where trade_date={trade_date}"""
    df = pd.read_sql_query(sql, engine_ts)
    print(df)
    return df


def write_data(df):
    res = df.to_sql('daily', engine_ts, index=False, if_exists='append', chunksize=6000)
    # print(res)


def get_data(trade_date):
    pro = ts.pro_api()
    df = pro.query('daily',trade_date=trade_date)
    print(df)
    print(f'******写入{trade_date}的daily数据********')
    return df


if __name__ == '__main__':
    df_trade_date = pd.read_csv('trade_date.csv')
    # 因为之前按倒序取了一部分日期数据，sql查库比较耽误时间，因此按升序排序后再来读数
    for i in df_trade_date.sort_values(by='trade_date',ascending=True)['trade_date']:
        df1 = read_data(i)
        if df1.empty:
            try:
                df = get_data(i)
                write_data(df)
            except:
                time.sleep(1)
        else:
            print(f'******{i}的daily数据已存在，Paas******')