# -*- coding: utf-8 -*-
import time
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
from factor_selection import *

# ================= 配置区 =================
ACC_ID = '47601131'
QMT_PATH = r'D:\光大证券金阳光QMT\userdata_mini' # 请确保路径正确

# 1. 假设这是您已经选好并排好序的股票代码列表（第一名到最后一名）
STOCK_POOL = ['301308.SZ', '603986.SH', '002920.SZ', '002555.SZ', '601919.SH', '601857.SH', '601788.SH', '600887.SH', 
              '601898.SH', '600886.SH', '600900.SH', '688981.SH', '688126.SH', '002371.SZ', '002202.SZ', '601633.SH', 
              '300750.SZ', '002594.SZ','601360.SH', '601601.SH', '601600.SH', '600941.SH', '601988.SH', '600050.SH', 
              '300274.SZ']

# 2. 策略核心参数
BUYIN_COUNT = 6          # 目标持股数
CHECK_COUNT = 10         # 考量池大小（跌出前10名卖出）
DRAWBACK_PCT = 10.0      # 硬性止损百分比
INDEX_CODE = '000001.SH' # 大盘参照：上证指数
SIMUMLATION =  True

# ================= 核心功能函数 =================

def get_market_pos_multiplier():
    """根据大盘趋势决定仓位系数：牛市1.0，熊市0.5，震荡0.8"""
    try:
        xtdata.download_history_data2([INDEX_CODE], period='1d')
        time.sleep(1) # 等待数据落地
        data = xtdata.get_market_data_ex(['close'], [INDEX_CODE], period='1d', count=40)
        df = data[INDEX_CODE]
        if df.empty or len(df) < 20: return 0.8
        
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        current_price = df['close'].iloc[-1]
        
        if current_price > ma20 * 1.02: return 1.0  # 多头趋势，满仓
        if current_price < ma20 * 0.98: return 0.5  # 空头趋势，减仓
        return 0.8  # 震荡市
    except:
        return 0.8 # 出错则保守运行
    
def order_stock(xt_trader: XtQuantTrader, acc, stock, order_type, order_volume, price_type, price):
    if not SIMUMLATION:
        xt_trader.order_stock(acc, 
                              stock_code=stock, 
                              order_type=order_type, 
                              order_volume=order_volume, 
                              price_type= price_type, 
                              price=price)

def run_strategy():
    # ---- 1. 连接客户端 ----
    xt_trader = XtQuantTrader(QMT_PATH, int(time.time()))
    xt_trader.start()
    acc = StockAccount(ACC_ID)
    if xt_trader.connect() != 0:
        print("QMT连接失败，请检查极简模式服务状态")
        return

    # ---- 2. 获取账户资产与持仓 ----
    asset = xt_trader.query_stock_asset(acc)
    pos_res = xt_trader.query_stock_positions(acc)
    # 持仓字典 {代码: {可用数量, 成本价}}
    holdings = {p.stock_code: {'vol': p.can_use_volume, 'cost': p.open_price} for p in pos_res if p.volume > 0}

    # ---- 3. 计算目标金额 (受大盘环境影响) ----
    multiplier = get_market_pos_multiplier()
    total_logic_vol = asset.total_asset * multiplier
    single_target_value = total_logic_vol / BUYIN_COUNT
    print(f"大盘仓位系数: {multiplier}, 每只标的拟分配金额: {single_target_value:.2f}")

    # 获取排名切片
    selected = select(STOCK_POOL, CHECK_COUNT, True, False)
    top_buyin = selected[:BUYIN_COUNT]
    top_check = selected[:CHECK_COUNT]

    # ---- 4. 卖出逻辑 (末位淘汰 + 硬止损) ----
    for stock, info in holdings.items():
        # 获取当前价计算止损
        tick = xtdata.get_full_tick([stock])[stock]
        last_price = tick['lastPrice']
        drawdown = (last_price - info['cost']) / info['cost'] * 100
        
        # 卖出条件 A: 掉出排名考量池
        if stock not in top_check:
            print(f"【淘汰】{stock} 跌出前{CHECK_COUNT}名，全额卖出")
            order_stock(xt_trader, acc, stock, 12, info['vol'], 11, 0)
            
        # 卖出条件 B: 硬止损触发
        elif drawdown <= -DRAWBACK_PCT:
            print(f"【止损】{stock} 亏损达 {drawdown:.2f}%，触发硬止损")
            order_stock(xt_trader, acc, stock, 12, info['vol'], 11, 0)

    # 等待成交同步
    time.sleep(2)

    # ---- 5. 买入逻辑 (排名准入 + 避开涨停) ----
    for stock in top_buyin:
        if stock not in holdings:
            tick = xtdata.get_full_tick([stock])[stock]
            # 避开涨停
            if tick['lastPrice'] < tick['highLimit']:
                buy_vol = int(single_target_value / tick['lastPrice'] / 100) * 100
                if buy_vol >= 100:
                    print(f"【买入】{stock} 排名进入前{BUYIN_COUNT}，数量: {buy_vol}")
                    order_stock(xt_trader, acc, stock, 23, buy_vol, 11, 0)
            else:
                print(f"【跳过】{stock} 封涨停中，不追高")

    print(">>> 调仓任务执行完毕")

if __name__ == "__main__":
    run_strategy()