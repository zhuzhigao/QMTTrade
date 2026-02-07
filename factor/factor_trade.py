# -*- coding: utf-8 -*-
import time
import datetime
import os
import json
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
   #print("=== 正在加载选股模块 ===")
from factor_selection import select
from factor_lib import get_market_sentiment
    #print("=== 完成加载选股模块 ===")
# ================= 配置区 =================
ACC_ID = '47601131'
QMT_PATH = r'D:\光大证券金阳光QMT实盘\userdata_mini' # 请确保路径正确
SIMUMLATION =  True

# 1. 假设这是您已经选好并排好序的股票代码列表（第一名到最后一名）
STOCK_POOL = ['301308.SZ', '603986.SH', '002920.SZ', '002555.SZ', '601919.SH', '601857.SH', '601788.SH', '600887.SH', 
              '601898.SH', '600886.SH', '600900.SH', '688981.SH', '688126.SH', '002371.SZ', '002202.SZ', '601633.SH', 
              '300750.SZ', '002594.SZ','601360.SH', '601601.SH', '601600.SH', '600941.SH', '601988.SH', '600050.SH', 
              '300274.SZ']
#IGNORE_POOL = []
IGNORE_POOL = ['515100.SH', '888880.SH', '000423.SZ', '159887.SZ', '601919.SH']

# 2. 策略核心参数
BUYIN_COUNT = 6          # 目标持股数
CHECK_COUNT = 10         # 考量池大小（跌出前10名卖出）
DRAWBACK_PCT = 10.0      # 硬性止损百分比
BENCHMARK = '000300.SH' # 大盘参照：上证指数
TOTAL_ASSET = 60000      # 交易资产数
MIDTERM_DAYS = 60
SHORTTERM_DAYS = 20
DATA_FILE = 'factor_trade.data'

def load_managed_stocks():
    """读取程序买入的股票列表"""
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_managed_stocks(stock_list):
    """保存程序买入的股票列表"""
    with open(DATA_FILE, 'w') as f:
        json.dump(list(set(stock_list)), f) # 去重保存
# ================= 核心功能函数 =================

def get_market_pos_multiplier(sentiment: int):
    multi = 0.8
    match sentiment:
        case 1: 
            multi = 1
        case 2:
            multi = 0.5
    print(f"仓位 {multi:.2f}")
    return multi

    
def order_stock(xt_trader: XtQuantTrader, acc, stock, order_type, order_volume, price_type, price):
    # if not SIMUMLATION:
    #     return xt_trader.order_stock(acc, 
    #                           stock_code=stock, 
    #                           order_type=order_type, 
    #                           order_volume=order_volume, 
    #                           price_type= price_type, 
    #                           price=price)
    return -1
def run_strategy():  
    tradedate = datetime.date.today().strftime('%Y%m%d')
    #tradedate = '20250101'
    sentiment = get_market_sentiment(BENCHMARK, tradedate, SHORTTERM_DAYS)
    download = False
    if download:
        print('稳固IPC通道：10s')
        time.sleep(10) # 关键：给 IPC 通道 10 秒钟的稳固时间
        
    # 【核心改动】在通道稳定后，再进行局部导入
 
    print('开始选股')
    #处理ignore list
    stock_candidates = list(set(STOCK_POOL) - set(IGNORE_POOL))
     # 获取排名切片
    selected = select(stock_pool=stock_candidates, at_date= tradedate, sector="", top_n= CHECK_COUNT, download=download, sdays=SHORTTERM_DAYS, mdays=MIDTERM_DAYS, sentiment=sentiment)
    #selected = select(stock_pool=stock_candidates, at_date= '20250101', sector="", top_n= CHECK_COUNT, download=download, sdays=SHORTTERM_DAYS, mdays=MIDTERM_DAYS, sentiment=sentiment)
    print("选中股票")
    print(selected)
 
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
    # 2. 剔除 IGNORE_POOL 中的股票
    holdings = {k: v for k, v in holdings.items() if k not in IGNORE_POOL}

    actual_holdings_codes = list(holdings.keys())
   
    managed_stocks = load_managed_stocks()
    # 清理之前记录的股票里头已经卖出的
    dirty_data = [s for s in managed_stocks if s not in actual_holdings_codes]
    if dirty_data:
        print(f"【数据同步】发现手动卖出或数据残留: {dirty_data}，正在清理...")
        for s in dirty_data:
            managed_stocks.remove(s)
        save_managed_stocks(managed_stocks)   
    
    # ---- 3. 计算目标金额 (受大盘环境影响) ----
    multiplier = get_market_pos_multiplier(sentiment)
    # total_logic_vol = asset.total_asset * multiplier
    total_logic_vol = min(TOTAL_ASSET, asset.total_asset) * multiplier
    single_target_value = total_logic_vol / BUYIN_COUNT
    print(f"大盘仓位系数: {multiplier}, 每只标的拟分配金额: {single_target_value:.2f}")

    selected_stocks = selected.index.tolist()
    top_buyin = selected_stocks[:BUYIN_COUNT]
    top_check = selected_stocks[:CHECK_COUNT]
    
    #todo: 隐患一： "止损后的无限回补" 死循环（最危险！）
    # ---- 4. 卖出逻辑 (末位淘汰 + 硬止损) ----
    for stock, info in holdings.items():
        if stock not in managed_stocks:
            continue
        # 获取当前价计算止损
        tick = xtdata.get_full_tick([stock])[stock]
        last_price = tick['lastPrice']
        drawdown = (last_price - info['cost']) / info['cost'] * 100

        stock_name = selected.at[stock, selected.columns[0]] if stock in selected.index else "N/A"
        # 卖出条件 A: 掉出排名考量池
        if stock not in top_check:
            print(f"【淘汰】{stock}: {stock_name} 跌出前{CHECK_COUNT}名，全额卖出")
            res = order_stock(xt_trader, acc, stock, 12, info['vol'], 11, 0)
            if res != -1: # 委托发送成功（或根据 QMT 返回值判断）
                managed_stocks.remove(stock)
                save_managed_stocks(managed_stocks)
                print(f"【同步成功】已从 {DATA_FILE} 中移除 {stock}")
            
        # 卖出条件 B: 硬止损触发
        elif drawdown <= -DRAWBACK_PCT:
            print(f"【止损】{stock}: {stock_name} 亏损达 {drawdown:.2f}%，触发硬止损")         
            res = order_stock(xt_trader, acc, stock, 12, info['vol'], 11, 0)
            if res != -1: # 委托发送成功（或根据 QMT 返回值判断）
                managed_stocks.remove(stock)
                save_managed_stocks(managed_stocks)
                print(f"【同步成功】已从 {DATA_FILE} 中移除 {stock}")

    # 等待成交同步
    time.sleep(2)

    # ---- 5. 买入逻辑 (排名准入 + 避开涨停) ----
    for stock in top_buyin:
        if stock not in holdings:
            tick = xtdata.get_full_tick([stock])[stock]
            # 停牌判断
            if tick['lastPrice'] <= 0 or tick['high'] == 0:
                print(f"【跳过】{stock} 当前处于停牌状态，取消买入")
                continue
                # 避开涨停
            if tick['lastPrice'] < tick['high']:
                buy_vol = int(single_target_value / tick['lastPrice'] / 100) * 100
                stock_name = selected.at[stock, selected.columns[0]] if stock in selected.index else "N/A"
                if buy_vol >= 100:
                    print(f"【买入】{stock}: {stock_name} 排名进入前{BUYIN_COUNT}，数量: {buy_vol}")
                    res = order_stock(xt_trader, acc, stock, 23, buy_vol, 11, 0)
                    if res != -1:
                        # 买入成功，记录到 managed_stocks
                        managed_stocks.append(stock)
                        save_managed_stocks(managed_stocks)
                        print(f"【同步成功】已记录 {stock} 到 {DATA_FILE}")
                else:
                    print(f"【跳过】【买入】{stock}: {stock_name} 不足一手，数量: {int(single_target_value / tick['lastPrice'])}")
            else:
                print(f"【跳过】【买入】 {stock} 封涨停中，不追高")

    print(">>> 调仓任务执行完毕")

if __name__ == "__main__":
    run_strategy()