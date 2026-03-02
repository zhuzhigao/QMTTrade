# -*- coding: utf-8 -*-
"""
36号策略 - 本地独立回测程序
基于 xtdata 本地数据 + Pandas 步进式回测
"""

import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
from xtquant import xtdata
from tqdm import tqdm # 进度条库，需 pip install tqdm
import sys

# ================= 1. 回测参数配置 =================
START_DATE = '20230101'
END_DATE = '20260201'

INDEX_CODE = '000300.SH'
BOND_ETF = '511010.SH'
ETF_GROUPS = {
    'Commodity': ['159985.SZ', '159981.SZ', '159980.SZ', '518880.SH'],
    'Dividend':  ['510880.SH', '512890.SH'],
    'Core':      ['510150.SH', '159967.SZ', '588000.SH'],
    'Global':    ['513100.SH', '513500.SH', '513030.SH'],
}

# 展平资产池
ALL_SYMBOLS = [item for sublist in ETF_GROUPS.values() for item in sublist] + [BOND_ETF]

RSRS_N = 18
RSRS_M = 600
MOM_DAYS = 20
TRADE_CYCLE = 5 # 每5天调仓一次
SLOP_THRESHOLD = 0.7

# ================= 2. 数据准备与预处理 =================
print(">>> 正在从 QMT 下载和读取本地历史数据...")
# 确保包含前置数据用于计算长周期指标
fetch_start = '20181201' 

xtdata.download_history_data(INDEX_CODE, period='1d', start_time=fetch_start, end_time=END_DATE)
for sym in ALL_SYMBOLS:
    xtdata.download_history_data(sym, period='1d', start_time=fetch_start, end_time=END_DATE)

# 获取基准数据
idx_data = xtdata.get_market_data_ex(['high', 'low', 'close'], [INDEX_CODE], period='1d', start_time=fetch_start, end_time=END_DATE, dividend_type='front')[INDEX_CODE]
idx_data.index = pd.to_datetime(idx_data.index)

# 获取所有标的收盘价
close_df = pd.DataFrame(index=idx_data.index)
for sym in ALL_SYMBOLS:
    df = xtdata.get_market_data_ex(['close'], [sym], period='1d', start_time=fetch_start, end_time=END_DATE, dividend_type='front')[sym]
    df.index = pd.to_datetime(df.index)
    close_df[sym] = df['close']

# ================= 3. 预计算 RSRS 择时指标 =================
print(">>> 正在计算 RSRS 标准分...")
rsrs_slopes = pd.Series(index=idx_data.index, dtype=float)

# 滚动计算斜率
highs = idx_data['high'].values
lows = idx_data['low'].values
for i in tqdm(range(RSRS_N, len(idx_data))):
    y = highs[i-RSRS_N : i]
    x = lows[i-RSRS_N : i]
    slope, _, _, _, _ = stats.linregress(x, y)
    rsrs_slopes.iloc[i] = slope

# 计算 Z-Score (使用前 M 天的数据)
z_scores = (rsrs_slopes - rsrs_slopes.rolling(RSRS_M).mean()) / rsrs_slopes.rolling(RSRS_M).std()

# ================= 4. 预计算动量得分 =================
print(">>> 正在计算各标的动量评分...")
mom_scores = pd.DataFrame(index=close_df.index, columns=ALL_SYMBOLS)

for sym in tqdm(ALL_SYMBOLS):
    prices = close_df[sym].values
    for i in range(MOM_DAYS, len(prices)):
        window = prices[i-MOM_DAYS : i]
        if np.isnan(window).any(): continue
        
        y = np.log(window)
        x = np.arange(len(y))
        slope, _, r_val, _, _ = stats.linregress(x, y)
        score = (np.exp(slope * 250) - 1) * (r_val ** 2)
        mom_scores.loc[mom_scores.index[i], sym] = score

# ================= 5. 步进式回测主引擎 =================
print(">>> 开始执行交易回测...")
# 截取正式回测时间段
bt_dates = idx_data.loc[START_DATE:END_DATE].index
portfolio_returns = []
target_weights = {sym: 0.0 for sym in ALL_SYMBOLS}
days_since_trade = 0

for i in range(1, len(bt_dates)):
    today = bt_dates[i-1] # 昨天收盘后的信号
    tomorrow = bt_dates[i] # 今天的收益
    
    # 1. 计算今日持仓收益
    daily_ret = 0
    for sym, weight in target_weights.items():
        if weight > 0:
            ret = (close_df.loc[tomorrow, sym] / close_df.loc[today, sym]) - 1
            if not np.isnan(ret):
                daily_ret += weight * ret
    portfolio_returns.append(daily_ret)
    
    # 2. 调仓逻辑 (每5天调仓)
    days_since_trade += 1
    if days_since_trade >= TRADE_CYCLE:
        days_since_trade = 0
        z = z_scores.loc[today]
        
        new_targets = []
        if pd.isna(z):
            new_targets = [BOND_ETF]
        elif z > SLOP_THRESHOLD:
            # 进攻模式
            print(today.strftime("%m/%d/%Y")+':进攻模式' )
            day_scores = mom_scores.loc[today].dropna()
            valid_scores = day_scores[day_scores > 0].sort_values(ascending=False)
            
            used_grp = set()
            for sym, score in valid_scores.items():
                if sym == BOND_ETF: continue
                grp = next((k for k, v in ETF_GROUPS.items() if sym in v), 'Other')
                if grp not in used_grp:
                    new_targets.append(sym)
                    used_grp.add(grp)
                if len(new_targets) >= 3: break
            
            if not new_targets: new_targets = [BOND_ETF]
        elif z < -SLOP_THRESHOLD:
            # 防御模式
            print(today.strftime("%m/%d/%Y")+':防御模式' )
            new_targets = [BOND_ETF]
        else:
            # 震荡模式保持原样
            print(today.strftime("%m/%d/%Y")+':震荡模式' )
            continue 

        # 更新权重
        target_weights = {sym: 0.0 for sym in ALL_SYMBOLS}
        weight_per_asset = 1.0 / len(new_targets)
        
        # --- 新增的打印逻辑 ---
        print_info = [] # 用来收集打印信息
        
        for sym in new_targets:
            target_weights[sym] = weight_per_asset
            # 将标的名称和权重(转为百分比)放入列表
            print_info.append(f"{sym}: {weight_per_asset * 100:.1f}%")
            
        # 打印出当天的完整调仓计划
        print(f"[{today.strftime('%Y-%m-%d')}] 调仓计划执行 -> { ' | '.join(print_info) }")

# ================= 6. 回测结果分析与可视化 =================
returns_df = pd.DataFrame({'Strategy': portfolio_returns}, index=bt_dates[1:])
# 基准收益 (沪深300)
hs300_returns = idx_data.loc[bt_dates[1:], 'close'] / idx_data.loc[bt_dates[:-1], 'close'].values - 1
returns_df['Benchmark'] = hs300_returns

# 计算净值
nav = (1 + returns_df).cumprod()

# 绘制曲线

# plt.figure(figsize=(12, 6))
# plt.plot(nav['Strategy'], label='Strategy 36', color='red', linewidth=2)
# plt.plot(nav['Benchmark'], label='HS300 Benchmark', color='blue', alpha=0.6)
# plt.title('Strategy 36 ETF Rotation Backtest')
# plt.ylabel('Net Asset Value (NAV)')
# plt.legend()
# plt.grid(True)
# plt.savefig('strategy36_result.png', dpi=300, bbox_inches='tight')
# plt.show()

# 打印绩效
total_return = nav['Strategy'].iloc[-1] - 1
annual_return = (nav['Strategy'].iloc[-1] ** (252/len(nav))) - 1
print(f"\n【回测绩效】")
print(f"总收益率: {total_return*100:.2f}%")
print(f"年化收益: {annual_return*100:.2f}%")
sys.stdin.readline()