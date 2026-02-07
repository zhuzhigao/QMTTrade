# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from xtquant import xtdata
import datetime
from factor_selection import select  # 请确保 select 已支持 end_time 参数

# ================= 强化回测配置 =================
STOCK_POOL = ['301308.SZ', '603986.SH', '002920.SZ', '002555.SZ', '601919.SH', '601857.SH', '601788.SH', '600887.SH', 
              '601898.SH', '600886.SH', '600900.SH', '688981.SH', '688126.SH', '002371.SZ', '002202.SZ', '601633.SH', 
              '300750.SZ', '002594.SZ','601360.SH', '601601.SH', '601600.SH', '600941.SH', '601988.SH', '600050.SH', 
              '300274.SZ']

START_DATE = '20250101'
END_DATE = '20251231'
INIT_CASH = 600000.0      # 设置为你的实盘金额 6万
BUYIN_COUNT = 6          # 持股6只
REBALANCE_FREQ = 5       # 5天调仓一次
FEE_RATE = 0.0001        # 万1手续费
MIN_FEE = 5.0            # 每笔最低5元
SLIPPAGE = 0.0005        # 万5滑点

def shift_date(date_str, n):
    """
    根据给定的日期字符串加减 n 天
    :param date_str: 初始日期字符串，格式为 '20250101'
    :param n: 加减的天数，正数为加，负数为减
    :return: 处理后的日期字符串，格式为 '20250101'
    """
    try:
        # 1. 将字符串解析为 datetime 对象
        dt_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
        
        # 2. 使用 timedelta 进行天数加减
        new_date_obj = dt_obj + datetime.timedelta(days=n)
        
        # 3. 将结果转回字符串格式
        return new_date_obj.strftime('%Y%m%d')
    except Exception as e:
        print(f"日期转换错误: {e}")
        return date_str

def run_professional_backtest():
    # 1. 预下载数据
    #xtdata.download_history_data2(STOCK_POOL, '1d', START_DATE, END_DATE)
    trading_days = xtdata.get_trading_calendar('SH', START_DATE, END_DATE)
    
    # 2. 账户初始化
    cash = INIT_CASH
    holdings = {} # {code: {'vol': 股数, 'cost': 成本}}
    history_log = []
    total_fees = 0.0
    
    print(f"开始回测：账户资金 {INIT_CASH} 元，每 {REBALANCE_FREQ} 天调仓...")

    for i, dt_str in enumerate(trading_days):
        
        daily_prices = xtdata.get_market_data_ex(['close', 'high', 'low'], STOCK_POOL, '1d', start_time=dt_str, end_time=shift_date(dt_str, 1), dividend_type = 'front')

        # A. 计算当日市值
        market_value = 0
        for code, info in holdings.items():
            if code in daily_prices and not daily_prices[code].empty:
                curr_price = daily_prices[code]['close'].iloc[0]
                market_value += info['vol'] * curr_price
        
        total_asset = cash + market_value
        history_log.append({'date': dt_str, 'asset': total_asset, 'cash': cash})

        # B. 调仓逻辑（严格每5个交易日触发）
        if i % REBALANCE_FREQ == 0:
            # --- 选股逻辑 (规避未来函数) ---
            # 传入当前的 dt_str，让 select 函数只用今天之前的数据
            try:
                selected_df = select(stock_pool=STOCK_POOL, at_date=dt_str, sector= False, top_n=10, download=False, sentiment=3, output= False)
                top_targets = selected_df.index.tolist()[:BUYIN_COUNT]
            except Exception as e:
                print(f"[{dt_str}] 选股出错: {e}")
                continue

            # --- 1. 卖出逻辑 (排名淘汰) ---
            for code in list(holdings.keys()):
                if code not in top_targets:
                    p = daily_prices[code]['close'].iloc[0]
                    if p <= 0: continue # 停牌
                    
                    exec_price = p * (1 - SLIPPAGE) # 卖出滑点
                    amount = holdings[code]['vol'] * exec_price
                    fee = max(MIN_FEE, amount * FEE_RATE)
                    
                    cash += (amount - fee)
                    total_fees += fee
                    del holdings[code]
                    print(f"[{dt_str}] 卖出 {code}，成交价:{exec_price:.2f}，手续费:{fee:.2f}")

            # --- 2. 买入逻辑 (补足缺位) ---
            # 计算每只股票应占用的理想资金
            target_per_stock = total_asset / BUYIN_COUNT
            
            for code in top_targets:
                if code not in holdings:
                    if code in daily_prices and not daily_prices[code].empty:
                        p = daily_prices[code]['close'].iloc[0]
                        # 排除停牌和一字涨停（买不进）
                        if p <= 0 or daily_prices[code]['high'].iloc[0] == daily_prices[code]['low'].iloc[0]:
                            continue
                        
                        exec_price = p * (1 + SLIPPAGE) # 买入滑点
                        buy_vol = int(target_per_stock / exec_price / 100) * 100
                        
                        if buy_vol > 0:
                            cost = buy_vol * exec_price
                            fee = max(MIN_FEE, cost * FEE_RATE)
                            
                            if cash >= (cost + fee):
                                cash -= (cost + fee)
                                total_fees += fee
                                holdings[code] = {'vol': buy_vol, 'cost': p}
                                print(f"[{dt_str}] 买入 {code}，成交价:{exec_price:.2f}，手续费:{fee:.2f}")

    # 3. 统计结果
    res = pd.DataFrame(history_log)
    res.set_index('date', inplace=True)
    
    total_ret = (res['asset'].iloc[-1] / INIT_CASH - 1) * 100
    mdd = (res['asset'] / res['asset'].cummax() - 1).min() * 100
    
    print("\n" + "="*30)
    print(f"回测结束报告 (初始资金:{INIT_CASH})")
    print(f"最终资产: {res['asset'].iloc[-1]:.2f}")
    print(f"累计收益率: {total_ret:.2f}%")
    print(f"最大回撤: {mdd:.2f}%")
    print(f"累计缴纳手续费: {total_fees:.2f} 元")
    print(f"手续费占初始资金比: {(total_fees/INIT_CASH)*100:.2f}%")
    print("="*30)
    
    return res

if __name__ == "__main__":
    report = run_professional_backtest()