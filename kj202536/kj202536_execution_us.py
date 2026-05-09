# -*- coding: utf-8 -*-
"""
策略名称：36号策略美股版 - 全天候多资产轮动 (仅输出调仓方案)
核心目标：中期持股、风险可控、供人工操作
数据来源：Yahoo Finance (yfinance)
运行方式：直接执行，输出当次调仓建议，不涉及实盘下单
"""

import sys
import datetime
import argparse
import numpy as np
import pandas as pd
from scipy import stats

try:
    import yfinance as yf
except ImportError:
    print("!! 请先安装 yfinance: pip install yfinance")
    sys.exit(1)


# ======================== 1. 策略配置 ========================
class Config:
    # ── 全天候美国本土化轮动资产池（带详细描述） ────
    etf_groups = {

        # ── 防守层：流动性保障 + 宏观危机对冲 ─────────────────────
        'Defensive': {
            'SGOV': 'iShares 0-3 Month Treasury Bond ETF | [角色: 现金替代] 超短期美债，年化~5%无风险收益，大盘暴跌时的终极避风港。',
            'GLD':  'SPDR Gold Trust                | [角色: 危机对冲] 全球最大实物黄金ETF，对冲地缘政治、美元贬值与信用风险。',
            'SHY':  'iShares 1-3 Year Treasury ETF  | [角色: 债权防守] 短期美债ETF，底层安全，熊市中提供稳定的利息缓冲。',
        },

        # ── 核心Beta：美国两大权益基准 ───────────────────────────
        'Core_Beta': {
            'SPY': 'SPDR S&P 500 ETF     | [角色: 美股风向标] 标普500指数，全美大盘蓝筹基准，流动性断层第一，复苏先锋。',
            'QQQ': 'Invesco QQQ Trust     | [角色: 科技核心] 纳斯达克100指数，科技与成长龙头集中营，牛市弹性极强。',
        },

        # ── 风格层：价值、红利与成长因子的交替切换 ─────────────────
        'Style_Factors': {
            'VTV':  'Vanguard Value ETF        | [角色: 深度价值] 美国大中盘价值因子，金融/工业/能源低位蓝筹，估值修复利器。',
            'SCHD': 'Schwab US Dividend ETF    | [角色: 红利护盾] 优质高分红公司，震荡市中抗跌与收息兼备，防御属性强。',
            'VUG':  'Vanguard Growth ETF       | [角色: 成长进攻] 美国大中盘成长因子，科技/消费龙头，牛市动量引擎。',
            'IWM':  'iShares Russell 2000 ETF  | [角色: 小盘高Beta] 罗素2000小盘股指数，美国本土经济敏感，复苏期弹性最大。',
        },

        # ── 行业主题：高弹性单边方向 ─────────────────────────────
        'Themes': {
            'XLK': 'Technology Select Sector SPDR | [角色: 科技趋势] 纯美国科技板块，覆盖AAPL/MSFT/NVDA等巨头，强单边趋势矛。',
            'XBI': 'SPDR S&P Biotech ETF          | [角色: 生物科技] 高Beta生物技术，创新药催化剂驱动，波动大弹性强。',
            'TAN': 'Invesco Solar ETF             | [角色: 清洁能源] 全球太阳能产业链，政策驱动型强趋势主题。',
        },
    }

    bond_etf   = 'SGOV'
    rsrs_index = 'SPY'

    all_symbols    = [c for g in etf_groups.values() for c in g]
    symbol_to_name = {
        c: n.split('|')[0].strip()
        for g in etf_groups.values() for c, n in g.items()
    }

    lot_sizes = {c: 1 for c in all_symbols}

    rsrs_n         = 18
    rsrs_m         = 600
    buy_threshold  =  0.5
    sell_threshold = -0.5
    rank_days      = 20
    target_num     = 3

    policy_asset   = 10000   # USD


# ======================== 2. 数据获取 ========================
def _fetch_ohlc(symbol: str, count: int) -> pd.DataFrame:
    """通过 yfinance 获取美股 ETF 日线数据（前复权）。"""
    end   = datetime.datetime.today()
    start = end - datetime.timedelta(days=count * 3)

    try:
        df = yf.download(
            symbol,
            start=start.strftime('%Y-%m-%d'),
            end=end.strftime('%Y-%m-%d'),
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            raise ValueError(f"No data returned for {symbol}")

        df = df.reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        date_col = next((c for c in df.columns if c in ('date', 'datetime', 'time')), 'date')
        result = pd.DataFrame()
        result['date'] = pd.to_datetime(df[date_col])
        result['high'] = pd.to_numeric(df['high'], errors='coerce')
        result['low']  = pd.to_numeric(df['low'], errors='coerce')
        result['close'] = pd.to_numeric(df['close'], errors='coerce')
        result = result.dropna().sort_values('date').tail(count).reset_index(drop=True)
        return result[['date', 'high', 'low', 'close']]
    except Exception as e:
        raise RuntimeError(f"yfinance download failed for {symbol}: {e}")


# ======================== 3. 核心算法 ========================
def calc_rsrs(df: pd.DataFrame, n: int, m: int) -> float:
    """
    RSRS 标准分 Z-Score：
      1. 对每个 n 天窗口做 low→high 线性回归，得到斜率 β
      2. 取最新 β，在前 m 个 β 的分布中标准化
    """
    needed = n + m
    if len(df) < needed:
        print(f"  [RSRS] 数据不足 (有 {len(df)} 条，需要 {needed} 条)，返回 0")
        return 0.0

    data  = df.tail(needed).reset_index(drop=True)
    highs = data['high'].values
    lows  = data['low'].values

    slopes = []
    for i in range(len(highs) - n + 1):
        slope, *_ = stats.linregress(lows[i:i + n], highs[i:i + n])
        slopes.append(slope)

    current_slope  = slopes[-1]
    history_slopes = np.array(slopes[:-1])
    z = (current_slope - history_slopes.mean()) / (history_slopes.std() + 1e-9)
    return float(z)


def calc_momentum(df: pd.DataFrame, rank_days: int) -> float:
    """动量平稳度评分 = 年化收益率 × R²"""
    prices = df['close'].values[-rank_days:]
    if len(prices) < rank_days:
        return -999.0
    y = np.log(prices)
    x = np.arange(len(y))
    slope, _, r_val, *_ = stats.linregress(x, y)
    return float((np.exp(slope * 250) - 1) * (r_val ** 2))


# ======================== 4. 主逻辑 ========================
def run(policy_asset: float):
    now = datetime.datetime.now()
    sep = '=' * 62
    print(f"\n{sep}")
    print(f"  36号策略美股版 — 调仓方案")
    print(f"  执行时间 : {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  策略仓位 : USD {policy_asset:,.0f}")
    print(f"{sep}\n")

    ohlc_cache: dict[str, pd.DataFrame] = {}

    # ── 1. RSRS 择时信号 ────────────────────────────────────────
    idx_sym  = Config.rsrs_index
    idx_name = Config.symbol_to_name.get(idx_sym, idx_sym)
    print(f">>> [择时] 计算 RSRS，基准: {idx_sym} {idx_name}")
    try:
        idx_df = _fetch_ohlc(idx_sym, count=Config.rsrs_n + Config.rsrs_m + 20)
        ohlc_cache[idx_sym] = idx_df
        z = calc_rsrs(idx_df, Config.rsrs_n, Config.rsrs_m)
    except Exception as e:
        print(f"  [错误] 获取基准数据失败: {e}")
        sys.exit(1)

    print(f"  RSRS Z-Score = {z:+.4f}  "
          f"(看多阈值 {Config.buy_threshold:+.1f}  看空阈值 {Config.sell_threshold:+.1f})\n")

    # ── 2. 确定目标持仓 ─────────────────────────────────────────
    target_list: list[str] = []

    if z < Config.sell_threshold:
        target_list = [Config.bond_etf]
        print(">>> [信号] 防御模式 — 撤离至避险资产\n")
    else:
        if z > Config.buy_threshold:
            print(">>> [信号] 进攻模式 — 计算各 ETF 动量评分...\n")
        else:
            print(">>> [信号] 震荡区间 — 计算当前动量排名作为参考...\n")
            print("    ★ 空仓用户：建议按下方方案建仓")
            print("    ★ 已有持仓：市场无明确趋势，建议维持现有仓位不动\n")

        scores: list[dict] = []
        print(f"  {'代码':<8}  {'名称':<38}  {'动量得分':>10}")
        print(f"  {'-' * 60}")
        for code in Config.all_symbols:
            name = Config.symbol_to_name.get(code, code)
            try:
                df = _fetch_ohlc(code, count=max(Config.rank_days + 10, 30))
                ohlc_cache[code] = df
                s = calc_momentum(df, Config.rank_days)
                flag = "✓" if s > 0 else " "
                print(f"  {code:<8}  {name:<38}  {s:>10.4f} {flag}")
                if s > 0:
                    scores.append({'code': code, 'score': s})
            except Exception as e:
                print(f"  {code:<8}  {name:<38}  获取失败: {e}")
        print(f"  {'-' * 60}\n")

        if scores:
            df_s = pd.DataFrame(scores).sort_values('score', ascending=False)
            used_grp: set[str] = set()
            for _, row in df_s.iterrows():
                code = row['code']
                grp = next((k for k, v in Config.etf_groups.items() if code in v), 'Other')
                if grp not in used_grp:
                    target_list.append(code)
                    used_grp.add(grp)
                if len(target_list) >= Config.target_num:
                    break

        if not target_list:
            target_list = [Config.bond_etf]
            print("  所有标的动量为负，切换至避险资产\n")

    # ── 3. 输出调仓方案 ─────────────────────────────────────────
    target_names = [Config.symbol_to_name.get(c, c) for c in target_list]
    target_value = policy_asset / len(target_list)

    print(f">>> [目标持仓]  {target_names}")
    print(f"    每仓目标金额: USD {target_value:,.0f}\n")

    print(f"  {sep}")
    print(f"  调仓方案")
    print(f"  {sep}")
    print(f"  {'代码':<8}  {'名称':<38}  {'最新价(USD)':>11}  "
          f"{'建议股数':>10}  {'估算金额(USD)':>14}")
    print(f"  {'-' * 85}")

    for code in target_list:
        name = Config.symbol_to_name.get(code, code)
        try:
            df = ohlc_cache.get(code) or _fetch_ohlc(code, count=15)
            price = float(df['close'].iloc[-1])
            shares = max(1, int(target_value / price))
            est_value = shares * price
            print(f"  {code:<8}  {name:<38}  {price:>8.2f} USD  "
                  f"{shares:>10,} 股  ≈{est_value:>10,.0f} USD")
        except Exception as e:
            print(f"  {code:<8}  {name:<38}  价格获取失败: {e}")

    print(f"  {'-' * 85}")
    print(f"{sep}\n")


# ======================== 5. 入口 ========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="36号策略美股版 — 输出调仓方案")
    parser.add_argument(
        '--asset', type=float, default=Config.policy_asset,
        help=f'策略仓位金额（美元），默认 ${Config.policy_asset:,.0f}'
    )
    args = parser.parse_args()
    run(policy_asset=args.asset)
