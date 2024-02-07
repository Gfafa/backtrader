import tushare as ts
import pandas as pd
from copy import deepcopy
import numpy as np
import talib as ta

import backtrader as bt
class PandasData(bt.feed.DataBase):
    '''
    The ``dataname`` parameter inherited from ``feed.DataBase`` is the pandas
    DataFrame
    '''

    # params = (
    #     # Possible values for datetime (must always be present)
    #     #  None : datetime is the "index" in the Pandas Dataframe
    #     #  -1 : autodetect position or case-wise equal name
    #     #  >= 0 : numeric index to the colum in the pandas dataframe
    #     #  string : column name (as index) in the pandas dataframe
    #     ('datetime', 'trade_date'),

    #     # Possible values below:
    #     #  None : column not present
    #     #  -1 : autodetect position or case-wise equal name
    #     #  >= 0 : numeric index to the colum in the pandas dataframe
    #     #  string : column name (as index) in the pandas dataframe
    #     ('open', 'open'),
    #     ('high', 'high'),
    #     ('low', 'low'),
    #     ('close', 'close'),
    #     ('volume', 'vol'),
    #     ('openinterest', None),
    # )
class TestStrategy(bt.Strategy):
    
    params = (
        ('buy_stocks', None), # 传入各个调仓日的股票列表和相应的权重
    )
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('{}, {}'.format(dt.isoformat(), txt))

    def __init__(self):
         # 读取调仓日期，回测时，会在这一天下单，然后在下一个交易日，以开盘价买入
        # self.trade_dates = pd.to_datetime(self.p.buy_stocks['trade_date'].unique()).tolist()
        # self.buy_stock = self.p.buy_stocks # 保留调仓信息
        
        self.trade_dates = pd.to_datetime(df_trade['trade_date'].unique()).tolist()
        #TODO buy_stock可能要使用权重大于0的数据
        self.buy_stock = df_trade[df_trade['weight']>0]
        
        self.order_list = []  # 记录以往订单，在调仓日要全部取消未成交的订单
        self.buy_stocks_pre = [] # 记录上一期持仓
    
    def next(self):
        # 获取当前的回测时间点
        dt = self.datas[0].datetime.date(0)
        # 打印当前时刻的总资产
        self.log('当前总资产 %.2f' %(self.broker.getvalue()))
        # 如果是调仓日，则进行调仓操作
        if dt in self.trade_dates:
            print("--------------{} 为调仓日----------".format(dt))
            #取消之前所下的没成交也未到期的订单
            if len(self.order_list) > 0:
                print("--------------- 撤销未完成的订单 -----------------")
                for od in self.order_list:
                    # 如果订单未完成，则撤销订单
                    self.cancel(od) 
                 #重置订单列表
                self.order_list = [] 
                
            # 提取当前调仓日的持仓列表
            buy_stocks_data = self.buy_stock.query(f"trade_date=='{dt}'")
            long_list = buy_stocks_data['sec_code'].tolist()
            print('long_list', long_list)  # 打印持仓列表
            
            # 对现有持仓中，调仓后不再继续持有的股票进行卖出平仓#trade_info中记录的是每个调仓日权重，如果股票权重为0则不会单独标明，因此不在记录中的为清仓股票
            sell_stock = [i for i in self.buy_stocks_pre if i not in long_list]
            print('sell_stock', sell_stock)
            if len(sell_stock) > 0:
                print("-----------对不再持有的股票进行平仓--------------")
                for stock in sell_stock:
                    data = self.getdatabyname(stock)
                    if self.getposition(data).size > 0 :
                        od = self.close(data=data)  
                        self.order_list.append(od) # 记录卖出订单

            # 买入此次调仓的股票：多退少补原则
            print("-----------买入此次调仓期的股票--------------")
            for stock in long_list:
                w = buy_stocks_data.query(f"sec_code=='{stock}'")['weight'].iloc[0] # 提取持仓权重
                data = self.getdatabyname(stock)
                order = self.order_target_percent(data=data, target=w*0.95) # 为减少可用资金不足的情况，留 5% 的现金做备用
                self.order_list.append(order)
                
            self.buy_stocks_pre = long_list  # 保存此次调仓的股票列表
        
    #订单日志    
    def notify_order(self, order):
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 已被处理的订单
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                    (order.ref,
                     order.executed.price,
                     order.executed.value,
                     order.executed.comm,
                     order.executed.size,
                     order.data._name))
            else:  # Sell
                self.log('SELL EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                        (order.ref,
                         order.executed.price,
                         order.executed.value,
                         order.executed.comm,
                         order.executed.size,
                         order.data._name))
        

if __name__ == "__main__":  

    # ts.set_token('f9d25f4ab3f0abe5e04fdf76c32e8c8a5cc94e384774da025098ec6e')
    ts.set_token('60b6737f6c928ce9ebbfaba3966ab0f890d8984c4361211a164bb59f')
    pro = ts.pro_api()
    df = pro.user(token='60b6737f6c928ce9ebbfaba3966ab0f890d8984c4361211a164bb59f')
    print(df)

    # # 一、获取基金数据
    # 1.从fund_basic获取场内基金基础信息
    # 2.按照1中的代码，从fund_share获取基金规模，并按照规模排序
    # 3.按照生成的基金规模排序表，手动选择不同品类的ETF，预计选择30+
    # 4.从fund_daily获取ETF成立至今的行情
    #提取基金列表和基金规模数据
    df1 = pro.query('fund_basic',market='E')
    df2 = pro.query('fund_share',trade_date='20240115')
    print(df1)
    print(df2)
    #以ts_code建立索引
    df1 = df1.set_index('ts_code')
    df2 = df2.set_index('ts_code')
    #以ts_code为索引将两个df合并
    merged_df = pd.merge(df1, df2, on='ts_code', how='left')
    print(merged_df)
    #以规模份额降序
    merged_df = merged_df.sort_values(by='fd_share', ascending=False)
    merged_df.to_csv('fund_basic.csv', encoding='utf-8-sig')
    # 选择份额排前200的作为备选基金池，以ETF名去重
    df_fund_pool = merged_df[0:199].drop_duplicates(subset='name',keep='first',inplace=False)


    # # 二、实现一个demo
    # 1.测试talib库，使用移动平均线
    # 2.选择510300.SH沪深300ETF，根据均线形成信号，形成trade_info仓位权重，
    # 3.使用backtrader进行回测

    df_daily = ts.pro_bar(ts_code='588000.SH',asset='FD',adj='qfq',freq='D').sort_values(by='trade_date',ascending=True)
    df_daily = df_daily.rename(columns={'trade_date':'datetime', 'vol':'volume', 'ts_code':'sec_code'})
    # df_daily['datetime'] = pd.to_datetime(df_daily['datetime'], format='%Y-%m-%d')
    # CDL3BLACKCROWS = ta.CDL3BLACKCROWS(df_daily.open.values,df_daily.high.values,df_daily.low.values,df_daily.close.values)
    # CDL3BLACKCROWS


    # df_daily['datetime'] = df_daily['datetime'].str.replace(r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3')
    # 修改datetime字段格式为pd.datetime
    df_daily['datetime'] = pd.to_datetime(df_daily['datetime'])
    df_daily = df_daily.assign(openinterest=0)
    df_daily.reset_index(drop=True, inplace=True)

    # 形成交易文件
    df_trade = pd.DataFrame(columns=['trade_date','sec_code','weight'])
    sma = ta.SMA(df_daily.close,timeperiod=10)
    sec_code='588000.SH'
    for i in range(len(df_daily)):
        if not np.isnan(sma[i]):
            # print(i)
            trade_date = df_daily['datetime'][i]
            if df_daily['close'][i] > sma[i]:
                weight = 1
            else:
                weight = 0
            if len(df_trade) > 0:
                if df_trade.iloc[-1]['weight'] != weight:
                    df_trade.loc[len(df_trade)] = [trade_date,sec_code,weight]
                    # print(df_trade.iloc[-1])
            elif weight > 0:#找能建仓的时间点
                df_trade.loc[len(df_trade)] = [trade_date,sec_code,weight]
                # print(df_trade.iloc[-1])
    df_trade = df_trade.sort_values(by='trade_date',ascending=True)
    df_trade.reset_index(drop=True, inplace=True)
    # df_trade['trade_date'] = df_trade['trade_date'].str.replace(r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3')
    print(df_trade)
                
    df_trade.to_csv('trade_info_sma.csv')

    # 以datetime为索引
    # 修改datetime字段格式为pd.datetime
    # df_trade['trade_date'] = pd.to_datetime(df_trade['trade_date'])
    
    # 将日期作为索引
    df_daily = df_daily.set_index('datetime')
    
    # 实例化大脑
    cerebro_ = bt.Cerebro() 

    # 按股票代码，依次循环传入数据
    for stock in df_daily['sec_code'].unique():
        # 日期对齐
        data = pd.DataFrame(index=df_daily.index.unique())
        df = df_daily.query(f"sec_code=='{stock}'")[['open','high','low','close','volume','openinterest']]
        data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
        data_.loc[:,['volume','openinterest']] = data_.loc[:,['volume','openinterest']].fillna(0)
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(method='pad')
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(0)
        datafeed1 = bt.feeds.PandasData(dataname=data_)
        cerebro_.adddata(datafeed1, name=stock)
        print(f"{stock} Done !") 

    cerebro_.addstrategy(TestStrategy)
    
    cerebro = deepcopy(cerebro_)  # 深度复制已经导入数据的 cerebro_，避免重复导入数据 
    # 初始资金 100,000,000    
    cerebro.broker.setcash(100000000.0) 
    # 添加策略
    cerebro.addstrategy(TestStrategy, buy_stocks=df_trade) # 通过修改参数 buy_stocks ，使用同一策略回测不同的持仓列表

    # 添加分析指标
    # 返回年初至年末的年度收益率
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
    # 计算最大回撤相关指标
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    # 计算年化收益
    cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns', tann=252)
    # 计算年化夏普比率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, annualize=True, riskfreerate=0) # 计算夏普比率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    # 返回收益率时序
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')

    # 启动回测
    result = cerebro.run()
    cerebro.plot()
