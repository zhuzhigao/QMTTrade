# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy
from xtquant import xtdata

# 尝试导入您的选股逻辑
try:
    from factor_selection import select 
except ImportError:
    print("错误：未找到 factor_selection.py")

# 保留您的完整 STOCK_POOL
STOCK_POOL = ['301308.SZ', '603986.SH', '002920.SZ', '002555.SZ', '601919.SH', '601857.SH', '601788.SH', '600887.SH', 
              '601898.SH', '600886.SH', '600900.SH', '688981.SH', '688126.SH', '002371.SZ', '002202.SZ', '601633.SH', 
              '300750.SZ', '002594.SZ','601360.SH', '601601.SH', '601600.SH', '600941.SH', '601988.SH', '600050.SH', 
              '300274.SZ']

# ================= 1. 策略逻辑类 =================
class QMTAdvancedStrategy(Strategy):
    rebalance_freq = 5    
    buyin_count = 6       
    watch_count = 10      
    stop_loss_pct = 0.10  
    slippage = 0.001      

    def init(self):
        self.all_stocks = self.all_data_dict
        self.stock_pool = list(self.all_stocks.keys())
        
        # 虚拟账本
        self.virtual_holdings = {} 
        self.virtual_cash = self.equity 
        self.day_counter = 0
        self.banned_today = []
        
        # 【可视化关键】创建一个指标来记录我们的组合净值，它会显示在 Plot 图表上
        self.portfolio_value = self.I(lambda x: x, [self.equity] * len(self.data), name="Selected_Portfolio_NAV")

    def next(self):
        self.day_counter += 1
        dt_idx = self.data.index[-1]
        dt_str = dt_idx.strftime('%Y%m%d')
        self.banned_today = []
        
        # --- 1. 每日个股止损检查 ---
        for code in list(self.virtual_holdings.keys()):
            stock_df = self.all_stocks[code]
            if dt_idx in stock_df.index:
                curr_p = stock_df.loc[dt_idx, 'Close']
                buy_p = self.virtual_holdings[code]['buy_price']
                if (curr_p / buy_p - 1) <= -self.stop_loss_pct:
                    self.virtual_cash += curr_p * self.virtual_holdings[code]['size'] * (1 - self.slippage)
                    self.banned_today.append(code)
                    del self.virtual_holdings[code]
                    print(f"[{dt_str}] !! 止损卖出: {code}")

        # --- 2. 周期调仓逻辑 (每5天) ---
        if self.day_counter % self.rebalance_freq == 0:
            try:
                selected_df = select(stock_pool=self.stock_pool,sector='', at_date=dt_str, top_n=self.watch_count, download=False, output=False)
                top_10 = selected_df.index.tolist()
                top_6 = top_10[:self.buyin_count]
            except:
                top_10, top_6 = [], []

            for code in list(self.virtual_holdings.keys()):
                if code not in top_10:
                    stock_df = self.all_stocks[code]
                    curr_p = stock_df.loc[dt_idx, 'Close']
                    self.virtual_cash += curr_p * self.virtual_holdings[code]['size'] * (1 - self.slippage)
                    del self.virtual_holdings[code]
                    print(f"[{dt_str}] 排名淘汰: {code}")

            current_mv = sum([self.all_stocks[c].loc[dt_idx, 'Close'] * h['size'] for c, h in self.virtual_holdings.items() if dt_idx in self.all_stocks[c].index])
            total_equity = self.virtual_cash + current_mv
            target_per_stock = total_equity / self.buyin_count
            
            for code in top_6:
                if code not in self.virtual_holdings and code not in self.banned_today:
                    stock_df = self.all_stocks[code]
                    if dt_idx in stock_df.index:
                        buy_p = stock_df.loc[dt_idx, 'Close']
                        if buy_p > 0 and stock_df.loc[dt_idx, 'High'] != stock_df.loc[dt_idx, 'Low']:
                            exec_p = buy_p * (1 + self.slippage)
                            size = int(target_per_stock / exec_p / 100) * 100
                            if size > 0 and self.virtual_cash > (size * exec_p):
                                self.virtual_holdings[code] = {'size': size, 'buy_price': exec_p}
                                self.virtual_cash -= (size * exec_p)
                                print(f"[{dt_str}] 买入补位: {code}, 数量: {size}")

        # --- 3. 净值同步与指标更新 (修复 Plot 和 Stats 的核心) ---
        final_mv = sum([self.all_stocks[c].loc[dt_idx, 'Close'] * h['size'] for c, h in self.virtual_holdings.items() if dt_idx in self.all_stocks[c].index])
        real_nav = self.virtual_cash + final_mv
        
        # 更新 Plot 上的曲线值
        self.portfolio_value[-1] = real_nav
        
        # 同步框架 Equity
        current_p = self.data.Close[-1]
        target_units = int(real_nav / current_p)
        diff = target_units - self.position.size
        
        if diff > 0:
            self.buy(size=diff)
        elif diff < 0:
            self.sell(size=abs(diff))

# ================= 2. Main 函数 =================
if __name__ == '__main__':
    START_DATE = '20240101'
    END_DATE = '20241231'

    # 1. 下载数据
    bench_data = xtdata.get_market_data_ex([], ['000300.SH'], '1d', START_DATE, END_DATE)['000300.SH']
    bench_data.index = pd.to_datetime(bench_data.index)
    bench_data = bench_data.rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'})

    all_stocks_data = {}
    for code in STOCK_POOL:
        df = xtdata.get_market_data_ex([], [code], '1d', START_DATE, END_DATE, dividend_type='front')[code]
        if not df.empty:
            df.index = pd.to_datetime(df.index)
            df = df.rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'})
            all_stocks_data[code] = df
    
    QMTAdvancedStrategy.all_data_dict = all_stocks_data
    
    bt = Backtest(bench_data, QMTAdvancedStrategy, cash=1000000, commission=0, trade_on_close=True)

    stats = bt.run()
    print("\n=== 回测统计报告 ===")
    print(stats)
    
    # 此时 plot 出来，除了主图 K 线，下方 Equity 曲线和新增的指标线都会准确反映策略收益
    bt.plot()