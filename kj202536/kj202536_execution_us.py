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
    etf_groups = {

        # ── 防守 (Defensive)：构建全久期债权护城河 + 硬通货对冲 ──
        'Defensive': {
            'SGOV': 'iShares 0-3月美债 ETF | [角色: 现金替代] 几乎无价格波动的无风险收益标的。在加息周期或全场暴跌、RSRS转空时，作为最终的避风港，赚取无风险利息。',
            'IEF':  'iShares 7-10年美债 ETF | [角色: 配置中轴] 中久期国债，对利率变动中度敏感。用于平滑由于长债波动过大带来的回撤，是经典的股债平衡核心。',
            'TLT':  'iShares 20年+美债 ETF  | [角色: 衰退利器] 高久期债权，与股市具有极强的负相关性。在经济衰退预期升温或股市急剧崩盘时，通过久期杠杆实现逆势上涨。',
            'GLD':  'SPDR 黄金 ETF          | [角色: 硬资产避险] 针对地缘政治冲突、信用风险及恶性通胀的终极对冲工具。当信用货币受压时，它能提供非线性的防御收益。',
        },

        # ── 核心 (Core)：获取美国市场最纯粹的 Beta 收益 ──
        'Core': {
            'QQQ': '纳指100 ETF (Invesco)  | [角色: 成长先锋] 集中于科技、生物医药及互联网巨头。在降息预期或技术创新驱动的牛市中，作为动量评分最高的“进攻矛”。',
            'SPY': '标普500 ETF (SPDR)     | [角色: 市场基准] 覆盖美国500家蓝筹企业，行业分布均衡。作为策略的底仓逻辑，代表美国整体经济的平均增长溢价。',
        },

        # ── 因子 (Factors)：利用二级市场风格漂移获取 Alpha ──
        'Factors': {
            'SCHD': '嘉信理财美国红利 ETF   | [角色: 红利护盾] 筛选现金流稳健、持续派息的优质企业。在震荡市或价值风格占优时，提供极强的抗跌性和确定性现金流。',
            'MTUM': 'iShares 动量因子 ETF   | [角色: 趋势加速] 专门追逐过去一段时间涨幅最猛的股票。在单边牛市中能显著增强收益，但在风格切换期需警惕“动量崩塌”。',
            'USMV': 'iShares 低波动因子 ETF | [角色: 避震器] 选取波动率最低的股票组合。在市场方向不明、多空博弈激烈的“磨底期”，通过极低回撤实现净值的稳定攀升。',
            'QUAL': 'iShares 质量因子 ETF   | [角色: 穿越周期] 侧重于高净资产收益率、低负债的公司。在经济放缓阶段，这类具备“护城河”的企业是机构资金的抱团首选。',
        },

        # ── 宏观/抗通胀 (Macro)：针对滞胀与供应端的针对性布局 ──
        'Macro': {
            'XLE': '能源行业 ETF (SPDR)    | [角色: 通胀对冲] 挂钩原油及天然气价格。在能源通胀期或地缘政治影响供给时，往往与科技股形成完美的翘翘板效应。',
            'DBA': '农业基金 ETF (Invesco)  | [角色: 粮食安全] 涵盖玉米、大豆、糖等主要农产品期货。作为与股市相关性极低的资产，在气候异常或滞胀环境下提供独立的动量来源。',
        },

        # ── 国际 (International)：地理溢价与汇率套利 ──
        'International': {
            'VEA': 'Vanguard 发达市场 ETF  | [角色: 非美补位] 覆盖欧洲、日本等发达经济体。在美元指数走弱或非美资产估值极度低廉时，作为逃离美股高估值的轮动选择。',
            'EEM': 'iShares 新兴市场 ETF   | [角色: 高弹性Beta] 侧重于中国、印度、巴西等高增长地区。在全市场风险偏好提升时，能捕捉到比美股更大的波动溢价。',
        }
    }

    # 策略运行关键参数
    target_num = 3           # 每一轮调仓最终选出的精英标的数量
    rsrs_index = 'SPY'       # 择时基准，锚定全球流动性风向标标普500

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
def _fetch_ohlc(symbol: str, count: int, max_retries: int = 3) -> pd.DataFrame:
    """
    通过 yfinance (Yahoo Finance) 获取美股 ETF 日线数据（复权）。
    使用 Ticker.history() 避免 yf.download() 的 MultiIndex 列名问题。
    count: 所需交易日条数（反推 count*3 个日历天以覆盖节假日）。
    max_retries: 遇到网络瞬时抖动时的重试次数，每次间隔指数增长。
    """
    import time
    end   = datetime.datetime.today() + datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=count * 3)

    last_err = None
    for attempt in range(max_retries):
        try:
            raw = yf.Ticker(symbol).history(
                start=start.strftime('%Y-%m-%d'),
                end=end.strftime('%Y-%m-%d'),
                auto_adjust=True,
            )
            if raw.empty:
                raise ValueError(f"No data returned for {symbol}")
            df = raw.reset_index()
            df = df.rename(columns={'Date': 'date', 'High': 'high', 'Low': 'low', 'Close': 'close'})
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            df = df.dropna(subset=['high', 'low', 'close'])
            df = df.sort_values('date').tail(count).reset_index(drop=True)
            return df[['date', 'high', 'low', 'close']]
        except Exception as e:
            last_err = e
            wait = 2 ** attempt          # 1s → 2s → 4s
            print(f"  [重试 {attempt + 1}/{max_retries}] {symbol} 请求失败，{wait}s 后重试... ({e})")
            time.sleep(wait)

    raise RuntimeError(f"{symbol} 连续 {max_retries} 次请求失败: {last_err}")


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
        for code in Config.all_symbols[:]:
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
            df = ohlc_cache.get(code)
            if df is None:
                df = _fetch_ohlc(code, count=15)
            price = float(df['close'].iloc[-1])
            shares = int(target_value / price)
            if shares == 0:
                print(f"  {code:<8}  {name:<38}  {price:>8.2f} USD  "
                      f"  ⚠ 仓位不足以买入1股（最低需 USD {price:,.2f}，当前每仓 USD {target_value:,.0f}）")
            else:
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
