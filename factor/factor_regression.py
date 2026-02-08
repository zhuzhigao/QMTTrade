# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from xtquant import xtdata
import datetime
import matplotlib.pyplot as plt
import platform
from factor_selection import select  # 请确保 select 已支持 end_time 参数
from factor_lib import get_market_sentiment, shift_date

# ================= 强化回测配置 =================
STOCK_POOL = ['301308.SZ', '603986.SH', '002920.SZ', '002555.SZ', '601919.SH', '601857.SH', '601788.SH', '600887.SH', 
              '601898.SH', '600886.SH', '600900.SH', '688981.SH', '688126.SH', '002371.SZ', '002202.SZ', '601633.SH', 
              '300750.SZ', '002594.SZ','601360.SH', '601601.SH', '601600.SH', '600941.SH', '601988.SH', '600050.SH', 
              '300274.SZ']

START_DATE = '20230101'
END_DATE = '20251231'
INIT_CASH = 600000.0      # 设置为你的实盘金额 6万
BUYIN_COUNT = 6          # 持股6只
REBALANCE_FREQ = 5       # 5天调仓一次
FEE_RATE = 0.0001        # 万1手续费
MIN_FEE = 5.0            # 每笔最低5元
SLIPPAGE = 0.0005        # 万5滑点

BENCHMARK = '000300.SH'  # 沪深300

# 自动选择系统支持的中文核心字体
def set_plt_font():
    system = platform.system()
    if system == "Windows":
        # Windows 优先使用微软雅黑，其次是黑体
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'SimSun', 'STFloat']
    elif system == "Darwin":  # Mac 系统
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC']
    else:  # Linux 系统
        plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei']
    
    # 解决负号显示为方块的问题
    plt.rcParams['axes.unicode_minus'] = False

set_plt_font()

def run_professional_backtest():
    # 1. 预下载数据
    #xtdata.download_history_data2(STOCK_POOL+ [BENCHMARK], '1d', START_DATE, END_DATE)

    # 获取交易日列表 (第一个参数是市场代码，SH 或 SZ)
    # 注意：返回的是毫秒时间戳或整数日期，取决于版本
    trading_dates = xtdata.get_trading_dates('SH', start_time=START_DATE, end_time=END_DATE)

    # 转换成 YYYYMMDD 字符串列表，方便后续使用
    trading_days = [datetime.datetime.fromtimestamp(d/1000).strftime('%Y%m%d') for d in trading_dates]
    
    # 2. 账户初始化
    cash = INIT_CASH
    holdings = {} # {code: {'vol': 股数, 'cost': 成本}}
    history_log = []
    total_fees = 0.0
    
    print(f"开始回测：账户资金 {INIT_CASH} 元，每 {REBALANCE_FREQ} 天调仓...")

    for i, dt_str in enumerate(trading_days):
        daily_prices = xtdata.get_market_data_ex(
            ['close', 'high', 'low'], STOCK_POOL, '1d', 
            start_time=dt_str, end_time=shift_date(dt_str, 2), dividend_type = 'front')

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
                selected_df = select(stock_pool=STOCK_POOL, at_date=dt_str, sector= False, top_n=10, download=False, 
                                     sentiment=get_market_sentiment(BENCHMARK, dt_str), output= False)
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

# 【修正1】强制将策略结果的索引统一为 8 位字符串
    res.index = res.index.map(lambda x: str(x)[:8])
    
    # 获取沪深300基准
    benchmark_dict = xtdata.get_market_data_ex(['close'], [BENCHMARK], period='1d', start_time=START_DATE, end_time=END_DATE)
    
    if BENCHMARK in benchmark_dict and not benchmark_dict[BENCHMARK].empty:
        benchmark_data = benchmark_dict[BENCHMARK]
        
        # 【修正2】强制将基准数据的索引也统一为 8 位字符串
        benchmark_data.index = benchmark_data.index.map(lambda x: str(x)[:8])
        
        # 【修正3】使用对齐后的索引进行 reindex，并增加 bfill() 确保第一天有值
        res['benchmark_close'] = benchmark_data['close'].reindex(res.index).ffill().bfill()
        
        # 计算净值与回撤
        res['strategy_cum'] = res['asset'] / INIT_CASH
        res['benchmark_cum'] = res['benchmark_close'] / res['benchmark_close'].iloc[0]
        
        res['strategy_dd'] = (res['strategy_cum'] / res['strategy_cum'].cummax() - 1) * 100
        res['benchmark_dd'] = (res['benchmark_cum'] / res['benchmark_cum'].cummax() - 1) * 100
    else:
        print(f"警告：基准 {BENCHMARK} 数据获取为空，请检查是否下载数据")
        res['strategy_cum'] = res['asset'] / INIT_CASH
        res['benchmark_cum'] = 1.0
        res['strategy_dd'] = (res['strategy_cum'] / res['strategy_cum'].cummax() - 1) * 100
        res['benchmark_dd'] = 0.0

    total_ret = (res['strategy_cum'].iloc[-1] - 1) * 100
    mdd = res['strategy_dd'].min()
    
    # 4. 图形化展示 (保持逻辑不变)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    
    # 子图1：收益曲线
    ax1.plot(res.index, res['strategy_cum'], label='策略 (4-4-2)', color='#e63946', linewidth=2)
    ax1.plot(res.index, res['benchmark_cum'], label='沪深300 (基准)', color='#457b9d', linestyle='--', alpha=0.7)
    ax1.set_title(f'策略收益曲线对比 ({START_DATE} - {END_DATE})', fontsize=14)
    ax1.set_ylabel('累计净值', fontsize=12)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 子图2：回撤曲线
    ax2.fill_between(res.index, res['strategy_dd'], 0, facecolor='#e63946', alpha=0.3, label='策略回撤')
    ax2.plot(res.index, res['benchmark_dd'], label='基准回撤', color='#457b9d', linewidth=1, alpha=0.7)
    ax2.set_title('动态回撤对比 (%)', fontsize=12)
    ax2.set_ylabel('回撤幅度 (%)', fontsize=12)
    ax2.set_xlabel('日期', fontsize=12)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 优化日期显示
    plt.xticks(res.index[::15], rotation=45)
    plt.tight_layout()
    # plt.savefig('backtest_result.png')
    # print("\n>>> 图形报告已生成: backtest_result.png")
    plt.show()

    return res

if __name__ == "__main__":
    report = run_professional_backtest()