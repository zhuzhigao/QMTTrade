import sys
import os
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
from xtquant import xtdata

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.stockmgr import StockMgr

# ================= 可配置参数 =================
DEFENSE_ETFS        = ['518880.SH', '513100.SH']  # 防御ETF列表，可自由增减，等权分配
STOCK_NUM           = 3      # 每次选股数量，与策略 stock_num 一致
REBALANCE_DAY       = 10     # 每月几号（自然日）之后首个交易日调仓
ENABLE_MONKEY_CHECK = True   # True: 启用猴市巡检（模块0）；False: 禁用
SAVE_PLOT           = True   # True: 保存图表到文件；False: 仅显示不保存
PLOT_DIR            = r'C:\Users\xiusan\OneDrive\Investment\QMTTrade\kj202509'

START_TIME = '20230101'  # 回测起始日期
END_TIME = '20260401'

# ================= 1. 数据获取与预处理 =================
def get_local_data(code_list, start_time):
    # download_start = (datetime.datetime.strptime(start_time, "%Y%m%d") - datetime.timedelta(days=10)).strftime("%Y%m%d")
    # xtdata.download_history_data2(code_list, period='1d', start_time=download_start, end_time=END_TIME)
    data = {}
    for code in code_list:
        raw = xtdata.get_market_data_ex([], [code], period='1d', start_time=start_time, dividend_type='front')
        d = raw[code].reset_index().rename(columns={'index': 'date'})
        d['date'] = pd.to_datetime(d['date'])
        data[code] = d.set_index('date')
    return data

# 1a. 指数 + 防御ETF 日线
codes    = ['000300.SH', '000852.SH'] + DEFENSE_ETFS
raw_data = get_local_data(codes, START_TIME)

df = pd.DataFrame(index=raw_data['000300.SH'].index)
df['close_300'] = raw_data['000300.SH']['close']
df['close_852'] = raw_data['000852.SH']['close']

etf_col = {etf: f"close_{etf.replace('.', '_')}" for etf in DEFENSE_ETFS}
for etf, col in etf_col.items():
    df[col] = raw_data[etf]['close']

df = df.ffill().dropna()

# 1b. 获取成分股池
print(">> 获取成分股池...")
stockmgr = StockMgr()
pool_300 = stockmgr.query_stocks_in_sector('000300.SH')
pool_852 = stockmgr.query_stocks_in_sector('000852.SH')
all_stocks = list(set(pool_300 + pool_852))
# print(f">> 股票池共 {len(all_stocks)} 只，开始下载历史日线...")

# # 1c. 下载并加载所有个股历史收盘价，对齐至主日历
# def _on_download(data):
#     print(f"\r>> 下载进度: {data.get('finished', '?')}/{data.get('total', '?')}", end='', flush=True)

# xtdata.download_history_data2(all_stocks, period='1d', start_time=START_TIME, end_time=END_TIME, callback=_on_download)
# print()
print(">> 加载个股收盘价...")
stock_close = {}  # {code: Series, index=df.index}
for stock in all_stocks:
    raw = xtdata.get_market_data_ex(['close'], [stock], period='1d', start_time=START_TIME, dividend_type='front')
    if stock in raw and not raw[stock].empty:
        s = raw[stock]['close']
        s.index = pd.to_datetime(s.index)
        stock_close[stock] = s.reindex(df.index).ffill()

# 1d. 批量加载财务数据（PershareIndex + Income），index = 公告日期
print(">> 加载财务数据（耗时较长，请稍候）...")
all_fin  = xtdata.get_financial_data(all_stocks, table_list=['PershareIndex', 'Income'],
                                     start_time=START_TIME, end_time=END_TIME, report_type='announce_time')
fin_data = {s: all_fin[s] for s in all_stocks if s in all_fin}

# 1e. 加载个股基本信息（ST过滤用）
details = {s: xtdata.get_instrument_detail(s) for s in all_stocks}
print(">> 数据准备完成。\n")

# ================= 2. 选股逻辑（复现 buy_a_shares + _filter_fundamentals）=================
def select_stocks(style, as_of_date):
    """
    在 as_of_date 当天，用历史财务数据 + 历史价格复现基本面选股，返回个股列表。
    注意：成分股使用今日池（存在幸存者偏差），财务数据 / 价格均取截至 as_of_date 最新值。
    """
    pool = pool_300 if style == 'BIG' else pool_852
    ts   = pd.Timestamp(as_of_date)

    # 剔除 ST / 退市
    valid_pool = [
        s for s in pool
        if details.get(s)
        and 'ST'  not in details[s].get('InstrumentName', '')
        and '退'  not in details[s].get('InstrumentName', '')
    ]

    rows = {}
    for stock in valid_pool:
        fd = fin_data.get(stock)
        if fd is None:
            continue

        pershare = fd.get('PershareIndex')
        income   = fd.get('Income')
        detail   = details.get(stock)
        if pershare is None or income is None or detail is None:
            continue

        try:
            pershare.index = pd.to_datetime(pershare.index)
            income.index   = pd.to_datetime(income.index)
        except Exception:
            continue

        # 防未来函数：只取公告日 <= as_of_date 的最新一期
        ps_hist  = pershare[pershare.index <= ts]
        inc_hist = income[income.index   <= ts]
        if ps_hist.empty or inc_hist.empty:
            continue

        last_ps  = ps_hist.iloc[-1]
        last_inc = inc_hist.iloc[-1]

        eps     = last_ps.get('s_fa_eps_basic', None)
        roe     = last_ps.get('equity_roe',     None)
        dedu_np = last_inc.get('net_profit_incl_min_int_inc_after', None)

        if any(v is None or pd.isna(v) for v in [eps, roe, dedu_np]):
            continue

        # 历史价格（截至 as_of_date 最后可用收盘价）
        sc = stock_close.get(stock)
        if sc is None:
            continue
        sc_hist = sc[sc.index <= ts].dropna()
        if sc_hist.empty:
            continue
        price = sc_hist.iloc[-1]
        if price <= 0:
            continue

        pe         = (price / eps) if eps != 0 else None
        total_sh   = detail.get('TotalVolume', None)
        market_cap = (price * total_sh) if total_sh else None

        if pe is None or market_cap is None:
            continue

        rows[stock] = {'roe': roe, 'pe_ttm': pe, 'market_cap': market_cap, 'dedu_np': dedu_np}

    if not rows:
        print(f"  [select_stocks] {as_of_date.date()} 无有效财务数据，跳过建仓。")
        return []

    sdf = pd.DataFrame.from_dict(rows, orient='index').dropna()
    sdf = sdf[sdf['dedu_np'] > 0]  # 扣非净利润必须 > 0

    if style == 'BIG':
        sdf = sdf[(sdf['roe'] > 10) & (sdf['pe_ttm'] > 0) & (sdf['pe_ttm'] < 30)]
        sdf = sdf.sort_values('market_cap', ascending=False)
    else:
        sdf = sdf[sdf['roe'] > 15]
        sdf = sdf.sort_values('market_cap', ascending=True)

    result = sdf.index.tolist()[:STOCK_NUM]
    print(f"  [select_stocks] {as_of_date.date()} {style} -> {result}")
    return result

# ================= 3. 策略指标计算 =================
df['ma20_300'] = df['close_300'].rolling(20).mean()
df['ma20_852'] = df['close_852'].rolling(20).mean()

def calc_mom(series):
    return 0.5 * (series / series.shift(10) - 1) + 0.5 * (series / series.shift(20) - 1)

df['mom_300'] = calc_mom(df['close_300'])
df['mom_852'] = calc_mom(df['close_852'])

if ENABLE_MONKEY_CHECK:
    _MONKEY_WINDOW = 20
    _c = df['close_300']
    _er = ((_c - _c.shift(_MONKEY_WINDOW)).abs() /
           _c.diff().abs().rolling(_MONKEY_WINDOW).sum().replace(0, np.nan)).fillna(0.0)
    _cv = _c.rolling(_MONKEY_WINDOW + 1).std() / _c.rolling(_MONKEY_WINDOW + 1).mean()
    df['is_monkey'] = ((_er < 0.25) & (_cv > 0.015)).fillna(False)
else:
    df['is_monkey'] = False

_mask = df.index.day >= REBALANCE_DAY
rebalance_dates = [g.index[0] for _, g in df[_mask].groupby(df[_mask].index.to_period('M'))]
print(f">> 调仓日（每月 {REBALANCE_DAY} 号后首个交易日）: {[d.strftime('%Y-%m-%d') for d in rebalance_dates]}")

# ================= 4. 模拟交易循环 =================
cash                 = 1000000.0
equity_positions     = {}   # {stock_code: shares}，等权持仓各个股
etf_positions        = {etf: 0.0 for etf in DEFENSE_ETFS}
hold_style           = None
last_rebalance_month = -1
portfolio_value      = []
commissions_paid     = 0.0

for i in range(len(df)):
    today     = df.index[i]
    close_300 = df['close_300'].iloc[i]
    close_852 = df['close_852'].iloc[i]
    mom_300   = df['mom_300'].iloc[i]
    mom_852   = df['mom_852'].iloc[i]
    ma20_300  = df['ma20_300'].iloc[i]
    ma20_852  = df['ma20_852'].iloc[i]

    etf_prices    = {etf: df[col].iloc[i] for etf, col in etf_col.items()}
    is_friday     = today.weekday() == 4
    is_monkey     = df['is_monkey'].iloc[i]
    current_month = today.month

    # 模块0：猴市巡检
    if ENABLE_MONKEY_CHECK and is_monkey:
        target_style = 'DEFENSE'
    else:
        # 模块2：周五熔断
        if hold_style == 'SMALL':
            circuit_breaker = is_friday and (close_852 < ma20_852)
        else:
            circuit_breaker = is_friday and (close_300 < ma20_300)

        if circuit_breaker:
            target_style = 'DEFENSE'
        elif (current_month != last_rebalance_month
              and today.day >= REBALANCE_DAY
              and not (pd.isna(mom_300) or pd.isna(mom_852))):
            # 模块1：月度动量研判 — 每月 REBALANCE_DAY 号之后首个交易日触发
            if mom_300 < 0 and mom_852 < 0:
                target_style = 'DEFENSE'
            elif mom_300 >= mom_852:
                target_style = 'BIG'
            else:
                target_style = 'SMALL'
            last_rebalance_month = current_month
            print(f'每月调仓日：' + today.strftime("%Y-%m-%d"))
        else:
            target_style = hold_style

    # 模拟调仓
    if target_style != hold_style:
        date_str = today.strftime("%Y-%m-%d")

        # 1. 清仓个股持仓（含印花税 0.1%，ETF 免印花税）
        for stock, shares in equity_positions.items():
            if shares <= 0:
                continue
            sc    = stock_close.get(stock)
            price = sc.iloc[i] if sc is not None and not pd.isna(sc.iloc[i]) else None
            if price is None or price <= 0:
                continue
            sell_amount  = shares * price
            fee          = max(sell_amount * 0.0001, 5.0) + sell_amount * 0.001  # 佣金 + 印花税
            cash += sell_amount - fee
            commissions_paid += fee
            print(f"[{date_str}] SELL  {stock:<12}  shares={shares:>10.2f}  price={price:>8.3f}  amount={sell_amount:>12.2f}  fee={fee:>7.2f}")
        equity_positions = {}

        # 2. 清仓防御ETF持仓
        for etf in DEFENSE_ETFS:
            if etf_positions[etf] <= 0:
                continue
            sell_amount = etf_positions[etf] * etf_prices[etf]
            fee = max(sell_amount * 0.0001, 5.0)
            cash += sell_amount - fee
            commissions_paid += fee
            print(f"[{date_str}] SELL  {etf:<12}  shares={etf_positions[etf]:>10.2f}  price={etf_prices[etf]:>8.3f}  amount={sell_amount:>12.2f}  fee={fee:>7.2f}")
            etf_positions[etf] = 0.0

        # 3. 买入目标
        if target_style in ('BIG', 'SMALL'):
            target_list = select_stocks(target_style, today)
            if target_list:
                cash_per_stock = cash * 0.98 / len(target_list)
                for stock in target_list:
                    sc    = stock_close.get(stock)
                    price = sc.iloc[i] if sc is not None and not pd.isna(sc.iloc[i]) else None
                    if price is None or price <= 0:
                        continue
                    fee    = max(cash_per_stock * 0.0001, 5.0)
                    shares = (cash_per_stock - fee) / price
                    cash  -= cash_per_stock
                    equity_positions[stock] = shares
                    commissions_paid += fee
                    print(f"[{date_str}] BUY   {stock:<12}  shares={shares:>10.2f}  price={price:>8.3f}  amount={cash_per_stock:>12.2f}  fee={fee:>7.2f}")
            else:
                # 选股为空：维持 DEFENSE（与策略"维持原状"对齐）
                target_style = hold_style if hold_style else 'DEFENSE'

        else:  # DEFENSE: 等权买入防御ETF
            per_etf_cash = cash / len(DEFENSE_ETFS)
            for etf in DEFENSE_ETFS:
                fee = max(per_etf_cash * 0.0001, 5.0)
                cash -= per_etf_cash
                etf_positions[etf] = (per_etf_cash - fee) / etf_prices[etf]
                commissions_paid += fee
                print(f"[{date_str}] BUY   {etf:<12}  shares={etf_positions[etf]:>10.2f}  price={etf_prices[etf]:>8.3f}  amount={per_etf_cash:>12.2f}  fee={fee:>7.2f}")

        print(f"[{date_str}] >> {hold_style or 'INIT'} -> {target_style}  cash_after={cash:>12.2f}")
        hold_style = target_style

    # 计算当日净值
    equity_val = sum(
        equity_positions[s] * (stock_close[s].iloc[i] if stock_close.get(s) is not None and not pd.isna(stock_close[s].iloc[i]) else 0)
        for s in equity_positions
    )
    etf_val    = sum(etf_positions[etf] * etf_prices[etf] for etf in DEFENSE_ETFS)
    current_val = cash + equity_val + etf_val
    portfolio_value.append(current_val)

df['strategy_value'] = portfolio_value
df['benchmark_value'] = (df['close_300'] / df['close_300'].iloc[0]) * 1000000

# ================= 5. 指标输出与绘图 =================
total_return = (df['strategy_value'].iloc[-1] / 1000000) - 1
max_drawdown = (df['strategy_value'] / df['strategy_value'].cummax() - 1).min()

yearly_start     = df['strategy_value'].resample('YE').first()
yearly_end       = df['strategy_value'].resample('YE').last()
yearly_return    = (yearly_end / yearly_start - 1).rename('strategy')
bm_yearly_return = ((df['benchmark_value'].resample('YE').last() /
                     df['benchmark_value'].resample('YE').first()) - 1).rename('benchmark')
avg_yearly_return = yearly_return.mean()

print(f"\n--- 回测结果 ({START_TIME[:4]}-至今) ---")
print(f"防御ETF: {DEFENSE_ETFS}  |  每次选股: {STOCK_NUM} 只")
print(f"注意: 成分股使用今日池（幸存者偏差），日内个股止损（模块3）未纳入。")
print(f"最终收益率:   {total_return:.2%}")
print(f"最大回撤:     {max_drawdown:.2%}")
print(f"累计手续费:   {commissions_paid:.2f} 元")
print(f"年均收益率:   {avg_yearly_return:.2%}")
print(f"")
print(f"{'年份':<6}  {'策略收益':>10}  {'沪深300':>10}")
print(f"{'------':<6}  {'----------':>10}  {'----------':>10}")
for year in yearly_return.index:
    bm = bm_yearly_return.loc[year] if year in bm_yearly_return.index else float('nan')
    print(f"{year.year:<6}  {yearly_return.loc[year]:>10.2%}  {bm:>10.2%}")

plt.figure(figsize=(12, 6))
plt.plot(df['strategy_value'], label='My All-Weather Strategy')
plt.plot(df['benchmark_value'], label='Benchmark (HS300)', linestyle='--')
plt.title('Backtest Result')
plt.legend()
plt.grid(True)

if SAVE_PLOT:
    fname = "regression_withmonkey.png" if ENABLE_MONKEY_CHECK else "regression_nomonkey.png"
    path  = os.path.join(PLOT_DIR, fname)
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"图表已保存: {path}")

plt.show()
