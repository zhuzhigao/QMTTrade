# -*- coding: utf-8 -*-
"""
kj202590_regression.py — 固收+ ETF 再平衡策略回测

复现 kj202590.py 的核心逻辑：
  - 固定权重配置：国债70% / 黄金14% / 红利8% / 纳指8%
  - 偏差超过 REBALANCE_THRESHOLD（15%）且份数 > MIN_SHARES 时触发再平衡
  - 权益类 ETF（红利/纳指）跌破成本 STOPLOSS_PCT（12%）触发止损清仓
  - 交易成本：单边佣金万分之二（最低5元）+ 单边滑点0.2%
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from xtquant import xtdata

# ================= 可配置参数 =================

WEIGHTS = {
    '511010.SH': 0.70,   # 国债ETF
    '518880.SH': 0.14,   # 黄金ETF
    '510880.SH': 0.08,   # 红利ETF
    '513100.SH': 0.08,   # 纳指ETF
}
EQUITY_ETFS         = {'510880.SH', '513100.SH'}   # 需要止损监控的权益类 ETF
BENCHMARK           = '511010.SH'                  # 基准：国债ETF（与原策略一致）

BUDGET              = 100_000.0     # 初始资金（元）
REBALANCE_THRESHOLD = 0.15          # 偏差率阈值：超过目标仓位 15% 才触发
MIN_SHARES          = 100           # 最小偏差份数：折算后不足 100 份不触发
STOPLOSS_PCT        = 0.12          # 权益类 ETF 止损线：成本回撤超 12% 清仓
COMMISSION          = 0.0002        # 单边佣金率（万分之二）
MIN_COMMISSION      = 5.0           # 最低佣金（元）
SLIPPAGE            = 0.002         # 单边滑点率

START_TIME          = '20200101'
END_TIME            = '20260401'

SAVE_PLOT           = True
PLOT_DIR            = os.path.dirname(os.path.abspath(__file__))

# ================= 1. 数据加载 =================

def load_etf_data(codes, start_time, end_time):
    """从 QMT 本地缓存加载 ETF 日线，返回对齐后的 DataFrame（index=date）"""
    data = {}
    for code in codes:
        raw = xtdata.get_market_data_ex(
            ['close'], [code], period='1d',
            start_time=start_time, end_time=end_time,
            dividend_type='front'
        )
        if code in raw and not raw[code].empty:
            s = raw[code]['close']
            s.index = pd.to_datetime(s.index)
            data[code] = s
        else:
            print(f"[警告] {code} 无数据，跳过。")

    df = pd.DataFrame(data)
    df = df.ffill().dropna()
    return df

print(">> 加载 ETF 日线数据...")
etf_codes = list(WEIGHTS.keys())
df = load_etf_data(etf_codes, START_TIME, END_TIME)
print(f">> 数据加载完成：{len(df)} 个交易日，{df.index[0].date()} — {df.index[-1].date()}\n")

# ================= 2. 成本辅助函数 =================

def buy_cost(shares: float, price: float) -> float:
    """买入实际花费（含佣金+滑点）"""
    raw    = shares * price
    comm   = max(raw * COMMISSION, MIN_COMMISSION)
    slip   = raw * SLIPPAGE
    return raw + comm + slip

def sell_proceeds(shares: float, price: float) -> float:
    """卖出实际到手（扣佣金+滑点）"""
    raw    = shares * price
    comm   = max(raw * COMMISSION, MIN_COMMISSION)
    slip   = raw * SLIPPAGE
    return raw - comm - slip

def trade_fee(shares: float, price: float) -> float:
    """单向交易摩擦成本（用于累计统计）"""
    raw  = shares * price
    comm = max(raw * COMMISSION, MIN_COMMISSION)
    slip = raw * SLIPPAGE
    return comm + slip

# ================= 3. 模拟交易循环 =================

# 持仓结构：{code: {'shares': float, 'cost': float}}  cost = 均价
positions = {code: {'shares': 0.0, 'cost': 0.0} for code in etf_codes}
cash              = BUDGET
portfolio_value   = []
commissions_paid  = 0.0
rebalance_count   = 0
stoploss_count    = 0

print(f">> 开始回测 ({START_TIME} — {END_TIME})，初始资金: {BUDGET:,.0f} 元")
print(f"   再平衡阈值: {REBALANCE_THRESHOLD:.0%} | 止损线: {STOPLOSS_PCT:.0%} | 最小份数: {MIN_SHARES}\n")

for i, today in enumerate(df.index):
    prices = df.iloc[i]   # {code: price}

    # ── 当日组合总值 ────────────────────────────────────────────────
    holding_val = sum(positions[c]['shares'] * prices[c] for c in etf_codes)
    total_val   = cash + holding_val

    # ── 止损检查（权益类 ETF）───────────────────────────────────────
    stopped = set()
    for code in EQUITY_ETFS:
        pos   = positions[code]
        if pos['shares'] <= 0 or pos['cost'] <= 0:
            continue
        price    = prices[code]
        drawdown = (price - pos['cost']) / pos['cost']
        if drawdown <= -STOPLOSS_PCT:
            proceeds = sell_proceeds(pos['shares'], price)
            fee      = trade_fee(pos['shares'], price)
            cash    += proceeds
            commissions_paid += fee
            stoploss_count   += 1
            print(f"[{today.date()}] [止损] {code}  成本={pos['cost']:.4f}  现价={price:.4f}  "
                  f"跌幅={drawdown:.1%}  卖出{pos['shares']:.0f}份  到手={proceeds:,.0f}")
            positions[code] = {'shares': 0.0, 'cost': 0.0}
            stopped.add(code)

    # ── 再平衡偏差计算 ──────────────────────────────────────────────
    # 重算总值（止损后现金变了）
    holding_val  = sum(positions[c]['shares'] * prices[c] for c in etf_codes)
    total_val    = cash + holding_val

    balances = {}
    for code, weight in WEIGHTS.items():
        if code in stopped:
            continue   # 止损品种本日不再平衡
        target_val  = total_val * weight
        cur_val     = positions[code]['shares'] * prices[code]
        diff_val    = target_val - cur_val     # 正=欠配, 负=超配
        diff_shares = diff_val / prices[code]
        balances[code] = {
            'target_val':  target_val,
            'cur_val':     cur_val,
            'diff_val':    diff_val,
            'diff_shares': diff_shares,
        }

    # 按偏差升序（超配/需卖排最前）
    sorted_items = sorted(balances.items(), key=lambda x: x[1]['diff_val'])

    day_has_trade = False

    # ── 第一轮：卖出超配品种 ────────────────────────────────────────
    for code, info in sorted_items:
        diff_val    = info['diff_val']
        target_val  = info['target_val']
        diff_shares = info['diff_shares']

        if diff_val >= 0:
            continue   # 欠配，留给买入轮

        # 触发条件
        if not (abs(diff_val) > target_val * REBALANCE_THRESHOLD
                and abs(diff_shares) > MIN_SHARES):
            continue

        sell_shares = int(abs(diff_shares) / 100) * 100
        sell_shares = min(sell_shares, positions[code]['shares'])   # 不超过持仓
        if sell_shares <= 0:
            continue

        price    = prices[code]
        proceeds = sell_proceeds(sell_shares, price)
        fee      = trade_fee(sell_shares, price)
        cash    += proceeds
        commissions_paid += fee
        positions[code]['shares'] -= sell_shares
        # 卖出不改 cost（成本价维持均价，用于止损判断）
        day_has_trade = True
        print(f"[{today.date()}] [再平衡-卖] {code}  -{sell_shares:.0f}份 @{price:.4f}  "
              f"到手={proceeds:,.0f}  偏差={diff_val/target_val:.1%}")

    # ── 第二轮：买入欠配品种 ────────────────────────────────────────
    for code, info in sorted_items:
        diff_val    = info['diff_val']
        target_val  = info['target_val']
        diff_shares = info['diff_shares']

        if diff_val <= 0:
            continue   # 超配，已处理

        if not (abs(diff_val) > target_val * REBALANCE_THRESHOLD
                and abs(diff_shares) > MIN_SHARES):
            continue

        price       = prices[code]
        cost_factor = 1 + SLIPPAGE + COMMISSION
        max_buy_val = cash / cost_factor
        buy_val     = min(diff_val, max_buy_val)
        buy_shares  = int(buy_val / price / 100) * 100

        if buy_shares <= 0:
            continue

        cost = buy_cost(buy_shares, price)
        if cost > cash:
            buy_shares -= 100
            if buy_shares <= 0:
                continue
            cost = buy_cost(buy_shares, price)

        if cost > cash:
            continue

        # 更新均价成本
        old_shares  = positions[code]['shares']
        old_cost    = positions[code]['cost']
        new_shares  = old_shares + buy_shares
        if new_shares > 0:
            # 加权均价
            positions[code]['cost'] = (old_shares * old_cost + buy_shares * price) / new_shares
        positions[code]['shares'] = new_shares

        fee = trade_fee(buy_shares, price)
        cash -= cost
        commissions_paid += fee
        day_has_trade = True
        print(f"[{today.date()}] [再平衡-买] {code}  +{buy_shares:.0f}份 @{price:.4f}  "
              f"花费={cost:,.0f}  偏差={diff_val/target_val:.1%}")

    if day_has_trade:
        rebalance_count += 1

    # ── 记录当日净值 ────────────────────────────────────────────────
    holding_val = sum(positions[c]['shares'] * prices[c] for c in etf_codes)
    portfolio_value.append(cash + holding_val)

# ================= 4. 指标计算 =================

df['strategy']  = portfolio_value
df['benchmark'] = (df[BENCHMARK] / df[BENCHMARK].iloc[0]) * BUDGET

total_return = df['strategy'].iloc[-1] / BUDGET - 1
bm_return    = df['benchmark'].iloc[-1] / BUDGET - 1
max_dd       = (df['strategy'] / df['strategy'].cummax() - 1).min()
bm_max_dd    = (df['benchmark'] / df['benchmark'].cummax() - 1).min()

# 年均收益（CAGR）
n_years       = len(df) / 252
cagr          = (df['strategy'].iloc[-1] / BUDGET) ** (1 / n_years) - 1
bm_cagr       = (df['benchmark'].iloc[-1] / BUDGET) ** (1 / n_years) - 1

# 夏普（无风险利率取2.5%）
daily_ret     = df['strategy'].pct_change().dropna()
sharpe        = (daily_ret.mean() - 0.025 / 252) / daily_ret.std() * np.sqrt(252)

# 年度收益对比
yearly_strat  = (df['strategy'].resample('YE').last() /
                 df['strategy'].resample('YE').first() - 1).rename('strategy')
yearly_bm     = (df['benchmark'].resample('YE').last() /
                 df['benchmark'].resample('YE').first() - 1).rename('benchmark')

# ================= 5. 输出 =================

print(f"\n{'='*55}")
print(f"  回测结果 ({START_TIME[:4]}—{END_TIME[:4]})  初始资金: {BUDGET:,.0f} 元")
print(f"{'='*55}")
print(f"{'指标':<18} {'策略':>12} {'基准(国债ETF)':>14}")
print(f"{'-'*44}")
print(f"{'总收益率':<18} {total_return:>12.2%} {bm_return:>14.2%}")
print(f"{'年化收益(CAGR)':<18} {cagr:>12.2%} {bm_cagr:>14.2%}")
print(f"{'最大回撤':<18} {max_dd:>12.2%} {bm_max_dd:>14.2%}")
print(f"{'夏普比率':<18} {sharpe:>12.2f}")
print(f"{'累计手续费':<18} {commissions_paid:>12,.0f} 元")
print(f"{'再平衡触发次数':<18} {rebalance_count:>12} 天")
print(f"{'止损触发次数':<18} {stoploss_count:>12} 次")
print(f"{'最终净值':<18} {df['strategy'].iloc[-1]:>12,.0f} 元")
print(f"{'='*55}")

print(f"\n{'年份':<6}  {'策略收益':>10}  {'基准收益':>10}  {'超额':>10}")
print(f"{'------':<6}  {'----------':>10}  {'----------':>10}  {'----------':>10}")
for yr in yearly_strat.index:
    s  = yearly_strat.loc[yr]
    bm = yearly_bm.loc[yr] if yr in yearly_bm.index else float('nan')
    print(f"{yr.year:<6}  {s:>10.2%}  {bm:>10.2%}  {s-bm:>+10.2%}")

# ================= 6. 绘图 =================

fig, axes = plt.subplots(2, 1, figsize=(13, 9), gridspec_kw={'height_ratios': [3, 1]})
fig.suptitle('kj202590 固收+ ETF 再平衡策略回测', fontsize=14)

# 上图：净值曲线
ax1 = axes[0]
ax1.plot(df.index, df['strategy'],  label='固收+ 策略', linewidth=1.8, color='steelblue')
ax1.plot(df.index, df['benchmark'], label=f'基准 ({BENCHMARK} 国债ETF)',
         linewidth=1.2, linestyle='--', color='gray')
ax1.set_ylabel('净值（元）')
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
ax1.legend(loc='upper left')
ax1.grid(True, alpha=0.4)

# 标注关键信息
info_text = (f"总收益: {total_return:.1%}  CAGR: {cagr:.1%}  "
             f"最大回撤: {max_dd:.1%}  夏普: {sharpe:.2f}")
ax1.set_title(info_text, fontsize=10, color='dimgray', pad=6)

# 下图：回撤曲线
ax2 = axes[1]
drawdown = df['strategy'] / df['strategy'].cummax() - 1
ax2.fill_between(df.index, drawdown, 0, alpha=0.4, color='tomato', label='策略回撤')
ax2.plot(df.index, drawdown, linewidth=0.8, color='tomato')
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0%}'))
ax2.set_ylabel('回撤')
ax2.set_ylim(min(drawdown.min() * 1.2, -0.01), 0.01)
ax2.legend(loc='lower left')
ax2.grid(True, alpha=0.4)

plt.tight_layout()

if SAVE_PLOT:
    path = os.path.join(PLOT_DIR, 'kj202590_regression.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"\n图表已保存: {path}")

plt.show()
