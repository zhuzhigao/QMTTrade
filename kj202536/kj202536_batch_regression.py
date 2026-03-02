# -*- coding: utf-8 -*-
"""
36号策略 - 批量参数回测优化版
针对 RSRS_M, SLOP_THRESHOLD, TRADE_CYCLE 进行网格搜索
"""

import numpy as np
import pandas as pd
from scipy import stats
from xtquant import xtdata
from tqdm import tqdm
import itertools
import warnings

warnings.filterwarnings('ignore')

# ================= 1. 基础配置与参数空间 =================
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
ALL_SYMBOLS = [item for sublist in ETF_GROUPS.values() for item in sublist] + [BOND_ETF]

# 待测试的参数网格
RSRS_N = 18 
MOM_DAYS = 20
RSRS_M_LIST = [600, 450, 300]
SLOP_THRESHOLD_LIST = [0.3, 0.5, 0.7]
TRADE_CYCLE_LIST = [5, 10]

# ================= 2. 数据准备 (仅执行一次) =================
print(">>> 正在初始化数据...")
fetch_start = '20181201' 
# xtdata.download_history_data(INDEX_CODE, period='1d', start_time=fetch_start, end_time=END_DATE)
# for sym in ALL_SYMBOLS:
#      xtdata.download_history_data(sym, period='1d', start_time=fetch_start, end_time=END_DATE)

# 获取基准数据
idx_data = xtdata.get_market_data_ex(['high', 'low', 'close'], [INDEX_CODE], period='1d', start_time=fetch_start, end_time=END_DATE, dividend_type='front')[INDEX_CODE]
idx_data.index = pd.to_datetime(idx_data.index)

# 获取所有标的收盘价
close_df = pd.DataFrame(index=idx_data.index)
for sym in ALL_SYMBOLS:
    df = xtdata.get_market_data_ex(['close'], [sym], period='1d', start_time=fetch_start, end_time=END_DATE, dividend_type='front')[sym]
    df.index = pd.to_datetime(df.index)
    close_df[sym] = df['close']

# 预计算 RSRS 原始斜率 (不随 M 变化)
print(">>> 预计算 RSRS 斜率...")
rsrs_slopes = pd.Series(index=idx_data.index, dtype=float)
highs, lows = idx_data['high'].values, idx_data['low'].values
for i in range(RSRS_N, len(idx_data)):
    slope, _, _, _, _ = stats.linregress(lows[i-RSRS_N:i], highs[i-RSRS_N:i])
    rsrs_slopes.iloc[i] = slope

# 预计算 动量得分 (不随择时参数变化)
print(">>> 预计算动量得分...")
mom_scores = pd.DataFrame(index=close_df.index, columns=ALL_SYMBOLS)
for sym in ALL_SYMBOLS:
    prices = close_df[sym].values
    for i in range(MOM_DAYS, len(prices)):
        window = prices[i-MOM_DAYS:i]
        if np.isnan(window).any(): continue
        slope, _, r_val, _, _ = stats.linregress(np.arange(MOM_DAYS), np.log(window))
        mom_scores.loc[mom_scores.index[i], sym] = (np.exp(slope * 250) - 1) * (r_val ** 2)

# ================= 3. 批量回测引擎 =================
results = []
bt_dates = idx_data.loc[START_DATE:END_DATE].index
param_combinations = list(itertools.product(RSRS_M_LIST, SLOP_THRESHOLD_LIST, TRADE_CYCLE_LIST))

print(f">>> 开始执行批量回测，共 {len(param_combinations)} 组参数...")

for m, threshold, cycle in tqdm(param_combinations):
    # 根据当前 M 计算 Z-Score
    z_scores = (rsrs_slopes - rsrs_slopes.rolling(m).mean()) / rsrs_slopes.rolling(m).std()
    
    portfolio_returns = []
    target_weights = {sym: 0.0 for sym in ALL_SYMBOLS}
    days_since_trade = 999 # 确保第一天触发调仓
    
    for i in range(1, len(bt_dates)):
        today, tomorrow = bt_dates[i-1], bt_dates[i]
        
        # 1. 计算收益
        daily_ret = sum(target_weights[s] * (close_df.loc[tomorrow, s]/close_df.loc[today, s] - 1) 
                        for s in ALL_SYMBOLS if target_weights[s] > 0)
        portfolio_returns.append(daily_ret)
        
        # 2. 调仓
        days_since_trade += 1
        if days_since_trade >= cycle:
            days_since_trade = 0
            z = z_scores.loc[today]
            new_targets = []
            
            if pd.isna(z):
                new_targets = [BOND_ETF]
            elif z > threshold:
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
            elif z < -threshold:
                new_targets = [BOND_ETF]
            else:
                continue # 震荡区维持现状

            target_weights = {sym: (1.0/len(new_targets) if sym in new_targets else 0.0) for sym in ALL_SYMBOLS}

    # 计算绩效指标
    nav = (1 + pd.Series(portfolio_returns)).cumprod()
    total_ret = nav.iloc[-1] - 1
    ann_ret = (nav.iloc[-1] ** (252/len(nav))) - 1
    mdd = (nav / nav.cummax() - 1).min()
    
    results.append({
        'RSRS_M': m,
        'Threshold': threshold,
        'Cycle': cycle,
        'TotalReturn': f"{total_ret*100:.2f}%",
        'AnnualReturn': f"{ann_ret*100:.2f}%",
        'MaxDrawdown': f"{mdd*100:.2f}%"
    })

# ================= 4. 输出结果 =================
res_df = pd.DataFrame(results)
print("\n" + "="*30 + " 批量回测最终结果 " + "="*30)
print(res_df.to_string(index=False))
print("="*78)