# -*- coding: utf-8 -*-
import backtrader_next as bt
import pandas as pd
from datetime import datetime
from xtquant import xtdata
from factor_selection import select
from factor_lib import get_market_sentiment

STOCK_POOL = ['301308.SZ', '603986.SH', '002920.SZ', '002555.SZ', '601919.SH', '601857.SH', '601788.SH', '600887.SH', 
                          '601898.SH', '600886.SH', '600900.SH', '688981.SH', '688126.SH', '002371.SZ', '002202.SZ', '601633.SH', 
                          '300750.SZ', '002594.SZ','601360.SH', '601601.SH', '601600.SH', '600941.SH', '601988.SH', '600050.SH', 
                          '300274.SZ']
# ================= 1. 手续费模型 (最低5元) =================
class QMT_Stock_Comm(bt.CommInfoBase):
    params = (
        ('commission', 0.0001), # 万1
        ('min_fee', 5.0),       # 最低5元
        ('stocklike', True),
    )
    def _getcommission(self, size, price, pseudoexec):
        return max(self.p.min_fee, abs(size) * price * self.p.commission)

# ================= 2. 核心策略类 =================
class QMT_Selective_StopLoss_Strategy(bt.Strategy):
    params = (
        ('rebalance_freq', 5),
        ('buyin_count', 6),
        ('slippage', 0.0005),
        ('stop_loss_pct', 0.10), # 10% 止损
    )

    def __init__(self):
        self.count = 0
        self.stock_pool = STOCK_POOL
        self.stocks = {d._name: d for d in self.datas if d._name != '000300.SH'}

    def next(self):
        dt_str = self.data.datetime.date(0).strftime('%Y%m%d')
        # 今日禁止买入的个股名单 (只针对刚止损卖出的)
        banned_today = []

        # --- 1. 每日个股止损检查 ---
        for d in self.datas:
            code = d._name
            if code == '000300.SH': continue
            pos = self.getposition(d)
            if pos.size > 0:
                cost_price = pos.price
                curr_price = d.close[0]
                
                # 检查是否亏损超过 10%
                if (curr_price / cost_price - 1) <= -self.p.stop_loss_pct:
                    self.close(data=d)
                    banned_today.append(code) # 加入今日黑名单
                    print(f"[{dt_str}] !! 止损卖出: {code}, 跌幅:{(curr_price/cost_price-1)*100:.2f}%")

        # --- 2. 调仓逻辑 (每5天触发) ---
        if self.count % self.p.rebalance_freq == 0:
            # A. 选股
            sentiment = get_market_sentiment('000300.SH', dt_str)
            try:
                selected_df = select(stock_pool=self.stock_pool, at_date=dt_str, sector=False, 
                                     top_n=10, download=False, sentiment=sentiment, output=False)
                top_targets = selected_df.index.tolist()[:self.p.buyin_count]
            except:
                top_targets = []

            # B. 卖出逻辑 (排名淘汰)
            for d in self.datas:
                code = d._name
                if code == '000300.SH': continue
                pos = self.getposition(d)
                # 如果没在上面的止损环节卖掉，但不在新名单里了，则卖出
                if pos.size > 0 and code not in top_targets:
                    self.close(data=d)
                    print(f"[{dt_str}] 排名淘汰: {code}")

            # C. 买入逻辑 (补位)
            total_asset = self.broker.getvalue()
            target_per_stock = total_asset / self.p.buyin_count
            
            for code in top_targets:
                if code not in self.stocks: continue
                # 如果该股今天刚止损卖出，即使它在 Top 名单里也跳过
                if code in banned_today:
                    print(f"[{dt_str}] 跳过买入: {code} (今日已止损)")
                    continue
                
                d = self.stocks[code]
                if self.getposition(d).size == 0:
                    # 检查一字涨停/停牌
                    if d.close[0] <= 0 or d.high[0] == d.low[0]:
                        continue
                        
                    exec_price = d.close[0] * (1 + self.p.slippage)
                    size = int(target_per_stock / exec_price / 100) * 100
                    
                    if size > 0 and self.broker.getcash() > (size * exec_price * 1.01):
                        self.buy(data=d, size=size)
                        print(f"[{dt_str}] 买入补位: {code}, 数量: {size}")

        self.count += 1

# ================= 3. 运行配置 =================
def run_regression():
    cerebro = bt.Cerebro()
    # 启用收盘撮合模式
    cerebro.broker.set_coc(True) 
    
    # 初始化
    cerebro.broker.setcash(600000.0)
    cerebro.broker.addcommissioninfo(QMT_Stock_Comm())
    
    # 模拟数据载入 (此处建议循环全量池)
    for code in STOCK_POOL:
        df = xtdata.get_market_data_ex([], [code], period='1d', start_time='20250101', end_time='20251231',dividend_type='front')[code]
        if not df.empty:
            df.index = pd.to_datetime(df.index)
            data = bt.feeds.PandasData(dataframe=df, name=code)
            cerebro.adddata(data)
   
    cerebro.addstrategy(QMT_Selective_StopLoss_Strategy)
    #cerebro.addobserver(bt.observers.Value)
    print('回测开始...')
    result = cerebro.run()
    strat = result[0]
    print('最终净值: %.2f' % cerebro.broker.getvalue())
    cerebro.show_report()
    #cerebro.plot(style='candle', numfigs=1, volume=False)
if __name__ == '__main__':
    run_regression()