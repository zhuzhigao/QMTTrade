import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
from xtquant import xtdata  # 假设你在QMT环境下，直接用内置数据获取

# ================= 可配置参数 =================
DEFENSE_ETFS        = ['518880.SH', '513100.SH']  # 防御ETF列表，可自由增减，等权分配
ENABLE_MONKEY_CHECK = True   # True: 启用猴市巡检（模块0）；False: 禁用，仅依赖月度动量+周度熔断
SAVE_PLOT           = True   # True: 保存图表到文件；False: 仅显示不保存
PLOT_DIR            = r'C:\Users\xiusan\OneDrive\Investment\QMTTrade\kj202509'  # 图表保存目录

# ================= 1. 数据获取与预处理 =================
def get_local_data(code_list, start_time):
    # 从QMT获取数据 (需确保QMT已下载对应历史数据)
    download_start_date = (datetime.datetime.strptime(start_time, "%Y%m%d") - datetime.timedelta(days=10)).strftime("%Y%m%d")
    xtdata.download_history_data2(code_list, period='1d', start_time=download_start_date, end_time='')
    data = {}
    for code in code_list:
        df = xtdata.get_market_data_ex([], [code], period='1d', start_time=start_time, dividend_type='front')
        df = df[code].reset_index().rename(columns={'index': 'date'})
        df['date'] = pd.to_datetime(df['date'])
        data[code] = df.set_index('date')
    return data

# 2022年1月至今
codes = ['000300.SH', '000852.SH'] + DEFENSE_ETFS
raw_data = get_local_data(codes, '20230101')

# 对齐数据
df = pd.DataFrame(index=raw_data['000300.SH'].index)
df['close_300'] = raw_data['000300.SH']['close']
df['close_852'] = raw_data['000852.SH']['close']

# 为每个防御ETF动态建列，列名如 close_518880_SH
etf_col = {etf: f"close_{etf.replace('.', '_')}" for etf in DEFENSE_ETFS}
for etf, col in etf_col.items():
    df[col] = raw_data[etf]['close']

df = df.ffill().dropna()

# ================= 2. 策略逻辑计算 =================
# 两个基准各自计算MA20，供周度熔断分别使用
df['ma20_300'] = df['close_300'].rolling(20).mean()
df['ma20_852'] = df['close_852'].rolling(20).mean()

# 策略用count=21取iloc[-11]和iloc[-21]，对应10期和20期收益
def calc_mom(series):
    mom10 = series / series.shift(10) - 1
    mom20 = series / series.shift(20) - 1
    return 0.5 * mom10 + 0.5 * mom20

df['mom_300'] = calc_mom(df['close_300'])
df['mom_852'] = calc_mom(df['close_852'])

# 猴市判断（向量化复现 MarketMgr.is_monkey_market 逻辑，ENABLE_MONKEY_CHECK=False 时跳过）
# MarketMgr 使用 count=window+1=21 个点：
#   ER  = |close[-1] - close[0]| / sum(|daily_changes|)  →  20期净变化 / 20期绝对变化之和
#   CV  = std(closes) / mean(closes)                      →  21点的变异系数
if ENABLE_MONKEY_CHECK:
    _MONKEY_WINDOW = 20
    _ER_THRESHOLD  = 0.25
    _VOL_THRESHOLD = 0.015
    _c = df['close_300']
    _net_change  = (_c - _c.shift(_MONKEY_WINDOW)).abs()
    _sum_changes = _c.diff().abs().rolling(_MONKEY_WINDOW).sum()
    _er          = (_net_change / _sum_changes.replace(0, np.nan)).fillna(0.0)
    _cv          = _c.rolling(_MONKEY_WINDOW + 1).std() / _c.rolling(_MONKEY_WINDOW + 1).mean()
    df['is_monkey'] = ((_er < _ER_THRESHOLD) & (_cv > _VOL_THRESHOLD)).fillna(False)
else:
    df['is_monkey'] = False

# ================= 3. 模拟交易循环 =================
cash                  = 1000000.0
equity_pos            = 0.0   # 权益持仓（大盘或小盘，以对应指数价格计）
etf_positions         = {etf: 0.0 for etf in DEFENSE_ETFS}  # 各防御ETF持仓
hold_style            = None  # 当前风格: 'BIG' | 'SMALL' | 'DEFENSE'
last_rebalance_month  = -1    # 对应策略 monthly_adjusted_month，防止月内重复调仓
portfolio_value       = []
commissions_paid      = 0.0

for i in range(len(df)):
    today     = df.index[i]
    close_300 = df['close_300'].iloc[i]
    close_852 = df['close_852'].iloc[i]
    mom_300   = df['mom_300'].iloc[i]
    mom_852   = df['mom_852'].iloc[i]
    ma20_300  = df['ma20_300'].iloc[i]
    ma20_852  = df['ma20_852'].iloc[i]

    # 当前各防御ETF价格
    etf_prices = {etf: df[col].iloc[i] for etf, col in etf_col.items()}

    is_friday     = today.weekday() == 4
    is_monkey     = df['is_monkey'].iloc[i]
    current_month = today.month

    # 模块0：猴市巡检 — 猴市时强制 DEFENSE，同时挂起月度/周度模块（is_paused=True）
    if ENABLE_MONKEY_CHECK and is_monkey:
        target_style = 'DEFENSE'
    else:
        # 模块2：周五熔断，使用当前风格对应的基准指数，优先于月度调仓
        if hold_style == 'SMALL':
            circuit_breaker = is_friday and (close_852 < ma20_852)
        else:
            circuit_breaker = is_friday and (close_300 < ma20_300)

        if circuit_breaker:
            target_style = 'DEFENSE'
        elif current_month != last_rebalance_month and not (pd.isna(mom_300) or pd.isna(mom_852)):
            # 模块1：月度动量研判 — 每月首个交易日执行一次，与策略 monthly_adjusted_month 门控一致
            # pd.isna 守护：预热期（前20根K线）动量为NaN时跳过，等数据足够再研判
            if mom_300 < 0 and mom_852 < 0:
                target_style = 'DEFENSE'
            elif mom_300 >= mom_852:
                target_style = 'BIG'
            else:
                target_style = 'SMALL'
            last_rebalance_month = current_month
        else:
            target_style = hold_style  # 月内非熔断日：维持现状，不重新研判

    # 模拟调仓
    if target_style != hold_style:
        date_str = today.strftime("%Y-%m-%d")

        # 1. 清仓全部当前持仓
        if equity_pos > 0:
            eq_code  = '000852.SH' if hold_style == 'SMALL' else '000300.SH'
            eq_price = close_852   if hold_style == 'SMALL' else close_300
            sell_amount = equity_pos * eq_price
            fee = max(sell_amount * 0.0001, 5.0)
            cash += sell_amount - fee
            commissions_paid += fee
            print(f"[{date_str}] SELL  {eq_code:<12}  shares={equity_pos:>12.2f}  price={eq_price:>8.3f}  amount={sell_amount:>12.2f}  fee={fee:>7.2f}")
            equity_pos = 0.0

        for etf in DEFENSE_ETFS:
            if etf_positions[etf] > 0:
                sell_amount = etf_positions[etf] * etf_prices[etf]
                fee = max(sell_amount * 0.0001, 5.0)
                cash += sell_amount - fee
                commissions_paid += fee
                print(f"[{date_str}] SELL  {etf:<12}  shares={etf_positions[etf]:>12.2f}  price={etf_prices[etf]:>8.3f}  amount={sell_amount:>12.2f}  fee={fee:>7.2f}")
                etf_positions[etf] = 0.0

        # 2. 买入目标
        if target_style == 'BIG':
            buy_amount = cash
            fee = max(buy_amount * 0.0001, 5.0)
            cash -= buy_amount
            equity_pos = (buy_amount - fee) / close_300
            commissions_paid += fee
            print(f"[{date_str}] BUY   {'000300.SH':<12}  shares={equity_pos:>12.2f}  price={close_300:>8.3f}  amount={buy_amount:>12.2f}  fee={fee:>7.2f}")

        elif target_style == 'SMALL':
            buy_amount = cash
            fee = max(buy_amount * 0.0001, 5.0)
            cash -= buy_amount
            equity_pos = (buy_amount - fee) / close_852
            commissions_paid += fee
            print(f"[{date_str}] BUY   {'000852.SH':<12}  shares={equity_pos:>12.2f}  price={close_852:>8.3f}  amount={buy_amount:>12.2f}  fee={fee:>7.2f}")

        else:  # DEFENSE: 等权买入所有防御ETF
            per_etf_cash = cash / len(DEFENSE_ETFS)
            for etf in DEFENSE_ETFS:
                fee = max(per_etf_cash * 0.0001, 5.0)
                cash -= per_etf_cash
                etf_positions[etf] = (per_etf_cash - fee) / etf_prices[etf]
                commissions_paid += fee
                print(f"[{date_str}] BUY   {etf:<12}  shares={etf_positions[etf]:>12.2f}  price={etf_prices[etf]:>8.3f}  amount={per_etf_cash:>12.2f}  fee={fee:>7.2f}")

        print(f"[{date_str}] >> {hold_style or 'INIT'} -> {target_style}  cash_after={cash:>12.2f}")
        hold_style = target_style

    # 各资产使用各自实际价格计算净值
    eq_price = close_852 if hold_style == 'SMALL' else close_300
    etf_val = sum(etf_positions[etf] * etf_prices[etf] for etf in DEFENSE_ETFS)
    current_val = cash + equity_pos * eq_price + etf_val
    portfolio_value.append(current_val)

df['strategy_value'] = portfolio_value
df['benchmark_value'] = (df['close_300'] / df['close_300'].iloc[0]) * 1000000

# ================= 4. 指标输出与绘图 =================
total_return = (df['strategy_value'].iloc[-1] / 1000000) - 1
max_drawdown = (df['strategy_value'] / df['strategy_value'].cummax() - 1).min()

# 逐年收益：每年首末交易日收盘价计算
yearly_start = df['strategy_value'].resample('YE').first()
yearly_end   = df['strategy_value'].resample('YE').last()
yearly_return = (yearly_end / yearly_start - 1).rename('strategy')

bm_yearly_start = df['benchmark_value'].resample('YE').first()
bm_yearly_end   = df['benchmark_value'].resample('YE').last()
bm_yearly_return = (bm_yearly_end / bm_yearly_start - 1).rename('benchmark')

avg_yearly_return = yearly_return.mean()

print(f"--- 回测结果 (2022-至今) ---")
print(f"防御ETF: {DEFENSE_ETFS}")
print(f"注意：日内个股8%止损（模块3）未纳入回测，实盘表现可能略有偏差。")
print(f"最终收益率:   {total_return:.2%}")
print(f"最大回撤:     {max_drawdown:.2%}")
print(f"累计手续费:   {commissions_paid:.2f} 元")
print(f"年均收益率:   {avg_yearly_return:.2%}")
print(f"")
print(f"{'年份':<6}  {'策略收益':>10}  {'沪深300':>10}")
print(f"{'------':<6}  {'----------':>10}  {'----------':>10}")
for year in yearly_return.index:
    y     = year.year
    strat = yearly_return.loc[year]
    bm    = bm_yearly_return.loc[year] if year in bm_yearly_return.index else float('nan')
    print(f"{y:<6}  {strat:>10.2%}  {bm:>10.2%}")

plt.figure(figsize=(12,6))
plt.plot(df['strategy_value'], label='My All-Weather Strategy')
plt.plot(df['benchmark_value'], label='Benchmark (HS300)', linestyle='--')
plt.title('Backtest Result: 2022-Now')
plt.legend()
plt.grid(True)

if SAVE_PLOT:
    import os
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(PLOT_DIR, f"regression_nomonkey.png")
    if ENABLE_MONKEY_CHECK:
        path = os.path.join(PLOT_DIR, f"regression_withmonkey.png")
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"图表已保存: {path}")

plt.show()
