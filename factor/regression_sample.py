import sys
import pandas as pd
import backtrader_next as bt
from datetime import datetime
from xtquant import xtdata


# ==========================================
# 2. 策略定义：双均线金叉死叉
# ==========================================
class SmaCrossStrategy(bt.Strategy):
    params = (('fast', 5), ('slow', 20),)

    def __init__(self):
        # 定义 5日和 20日均线
        self.sma_fast = bt.indicators.SMA(self.data.close, period=self.p.fast)
        self.sma_slow = bt.indicators.SMA(self.data.close, period=self.p.slow)
        # 定义交叉信号：1为金叉，-1为死叉
        self.crossover = bt.indicators.CrossOver(self.sma_fast, self.sma_slow)

    def next(self):
        if not self.position:  # 手中无持仓
            if self.crossover > 0:  # 金叉买入
                self.log(f'【买入信号】价格: {self.data.close[0]:.2f}')
                self.buy(size=100) # 买入100股
        elif self.crossover < 0:  # 死叉卖出
            self.log(f'【卖出信号】价格: {self.data.close[0]:.2f}')
            self.close() # 平仓

    def log(self, txt):
        dt = self.datas[0].datetime.date(0)
        print(f'{dt} {txt}')

# ==========================================
# 3. 数据桥接：QMT -> Backtrader
# ==========================================
def get_qmt_data(stock_code, start_date, end_date):
    print(f"正在从 QMT 获取 {stock_code} 数据...")
    
    # 下载历史数据 (确保 QMT 客户端已登录行情)
    # xtdata.download_history_data(stock_code, period='1d', start_time=start_date, end_time=end_date)
    
    # 获取数据并转为 DataFrame
    raw_data = xtdata.get_market_data_ex([], [stock_code], period='1d', start_time=start_date, end_time=end_date)
    df = raw_data[stock_code]
    
    if df.empty:
        raise ValueError("未能获取到数据，请检查 QMT 是否登录且代码输入正确。")

    # 格式标准化
    df.index = pd.to_datetime(df.index)
    df = df[['open', 'high', 'low', 'close', 'volume']]
    df['openinterest'] = 0

    print(df)
    
    return bt.feeds.PandasData(dataframe=df)

# ==========================================
# 4. 主程序运行
# ==========================================
if __name__ == '__main__':
    # 初始化大脑
    cerebro = bt.Cerebro()

    # 获取平安银行数据 (000001.SZ)
    try:
        data = get_qmt_data('000001.SZ', '20240101', '20241231')
        cerebro.adddata(data)
    except Exception as e:
        print(f"数据获取失败: {e}")
        sys.exit()

    # 注入策略
    cerebro.addstrategy(SmaCrossStrategy)

    # 设置初始资金 (对应账号 47601131)
    cerebro.broker.setcash(100000.0)
    # 设置万三手续费
    cerebro.broker.setcommission(commission=0.0003)

    print(f'回测启动资产: {cerebro.broker.getvalue():.2f}')
    cerebro.run()
    print(f'回测最终资产: {cerebro.broker.getvalue():.2f}')

    # 【关键步骤】绘图
    # backtrader-next 支持更现代的绘图方式
    print("正在生成图表...")
    time_tag = datetime.now().strftime("%Y%m%d_%H%M")
    report_name = f'results/strat_{time_tag}.html'
    cerebro.plot(style='candlestick', volume=True, filename=report_name)