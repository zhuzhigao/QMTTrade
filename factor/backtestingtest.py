# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import random
from backtesting import Backtest, Strategy
from xtquant import xtdata

# ================= 1. 策略逻辑类 =================
class SimpleRandomStrategy(Strategy):
    n_sma = 10  # 均线周期

    def init(self):
        # 定义一个简单的指标：10日均线
        # 【关键】：overlay=False 强制它开启中间的独立面板 (Panel 2)
        self.sma = self.I(lambda x: pd.Series(x).rolling(self.n_sma).mean(), 
                          self.data.Close, 
                          name="SMA_Indicator", 
                          overlay=False)
        
        self.day_count = 0

    def next(self):
        self.day_count += 1
        
        # 每隔 3 个交易日随机操作一次
        if self.day_count % 3 == 0:
            # 随机决定：1-买入, 2-卖出
            action = random.choice([1, 2]) 
            
            if action == 1 and not self.position:
                # 【修复点】：使用 size=0.9 代表使用 90% 的资金买入
                # 这样会产生 Trade，从而激活底部面板 (Panel 3)
                self.buy(size=0.9)
                print(f"{self.data.index[-1]} >> 执行买入 (90%仓位)")
            
            elif action == 2 and self.position:
                # 平掉所有持仓
                self.position.close()
                print(f"{self.data.index[-1]} >> 执行卖出 (平仓)")

# ================= 2. 数据准备与运行 =================
if __name__ == '__main__':
    # 使用你要求的股票
    symbol = '603986.SH'
    start_date = '20240101'
    end_date = '20241231'

    print(f"正在从 QMT 获取 {symbol} 数据...")
    # 获取历史数据
    raw_data = xtdata.get_market_data_ex([], [symbol], '1d', start_date, end_date)[symbol]
    
    if raw_data.empty:
        print("未获取到数据，请检查 QMT 是否下载了该股票的数据。")
    else:
        # 格式化数据
        df = raw_data.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        })
        df.index = pd.to_datetime(df.index)
        
        # 过滤掉成交量为 0 的日期
        df = df[df['Volume'] > 0]

        # 初始化回测
        # 初始资金 100 万，手续费设为 万分之三
        bt = Backtest(df, SimpleRandomStrategy, cash=1000000, commission=0.0003)

        # 运行
        stats = bt.run()
        
        print("\n=== 回测统计摘要 ===")
        print(f"最终价值: {stats['Equity Final [$]']:.2f}")
        print(f"交易笔数: {stats['# Trades']}")
        
        # 【核心】：显示 Plot
        # 1. 主面板 (OHLC)
        # 2. SMA 面板 (因为 overlay=False)
        # 3. Equity/Drawdown 面板 (因为产生了 Trades)
        bt.plot(filename="three_panels_result.html", open_browser=True)