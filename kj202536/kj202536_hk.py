# -*- coding: utf-8 -*-
"""
策略名称：36号策略港股版 - 全天候多资产轮动 (仅输出调仓方案)
核心目标：中期持股、风险可控、供人工操作
数据来源：akshare（无需 QMT / xtquant）
运行方式：直接执行，输出当次调仓建议，不涉及实盘下单
"""

import sys
import datetime
import argparse
import numpy as np
import pandas as pd
from scipy import stats

try:
    import akshare as ak
except ImportError:
    print("!! 请先安装 akshare: pip install akshare")
    sys.exit(1)


# ======================== 1. 策略配置 ========================
class Config:
    # ── 全天候中国及港股本土化轮动资产池（带详细描述） ──
    etf_groups = {

        # ── 防守层：流动性保障 + 宏观危机对冲 ─────────────────────
        'Defensive': {
            '03053.HK': '南方港元货币ETF | [角色: 终极避风港] 港币无风险现金管理工具，年化收益稳健，大盘两地暴跌时的绝对防御端。',
            '02840.HK': 'SPDR黄金ETF    | [角色: 危机对冲器] 全球最大实物黄金信托在港分支，对冲地缘政治、美元贬值及全球信用风险。',
            '02821.HK': 'ABF泛亚债券ETF | [角色: 债权防守端] 追踪亚洲多国优质本币主权债，底层极为安全，提供熊市中的稳定利息垫。',
        },

        # ── 核心Beta：中国资产境内外的两大权益基准（Benchmark） ───────────
        'Core_Beta': {
            '02800.HK': '盈富基金       | [角色: 港股风向标] 追踪香港恒生指数，涵盖离岸中国及香港本地大盘蓝筹，流动性断层第一，复苏先锋。',
            '03188.HK': '华夏沪深300ETF  | [角色: A股核心仓] 追踪境内沪深300指数，代表中国核心制造、主流消费及传统支柱产业的宏观基准。',
        },

        # ── 风格层：价值因子、红利因子与成长因子的交替切换 ───────────────────
        'Style_Factors': {
            '02828.HK': '恒生中国企业ETF | [角色: 中字头价值] 追踪H股国企指数，成分股100%为中资企业，集中于大金融、中字头破净低估值蓝筹。',
            '03110.HK': '恒生高股息ETF   | [角色: 震荡市护盾] 专守香港本地及中资优质高分红股票，震荡市、高利率环境下极强的抗跌与高收息神器。',
            '03033.HK': '南方恒生科技ETF | [角色: 离岸新经济] 追踪恒生科技指数，打包腾讯、美团、阿里、小米等中国互联网轻资产巨头，牛市动量极强。',
            '03147.HK': '南方中国创业板  | [角色: 境内硬科技] 追踪境内创业板指数，代表 A 股高 Beta 成长小盘，集中于新能源、创新药、高端制造。',
        },

        # ── 行业主题：高度精简，只保留一个最具颠覆性的高弹性单边方向 ───────────────
        'Themes': {
            '02845.HK': 'GlobalX中国电动车| [角色: 强趋势矛] 纯中国高Beta绿色制造、新能源汽车产业链与锂电池龙头，专门捕捉强单边产业趋势。',
        }
    }

    # 防御模式避险标的：港元货币基金，零回撤真正避险
    safe_havens = ['03053.HK']
    # 以盈富基金的 high/low 代理恒生指数做 RSRS 择时
    rsrs_index = '02800.HK'

    # 扁平化代码列表和名称映射（取 '|' 前的短名用于展示）
    all_symbols    = [c for g in etf_groups.values() for c in g]
    symbol_to_name = {
        c: n.split('|')[0].strip()
        for g in etf_groups.values() for c, n in g.items()
    }

    # 每手股数参考（下单前请在券商确认，HKEX 可能调整）
    lot_sizes = {
        '03053.HK':    1,   # 南方港元货币ETF ✅
        '02840.HK':    1,   # SPDR 黄金 ETF ✅ 已改为1手
        '02821.HK':   10,   # ABF 泛亚债券 ETF ✅
        '02800.HK':  500,   # 盈富基金 ✅
        '03188.HK':  200,   # 华夏沪深300 ✅
        '02828.HK':  200,   # 恒生中国企业 ETF ✅
        '03110.HK':  100,   # 恒生高股息 ETF ✅
        '03033.HK':  200,   # 南方恒生科技 ETF（大概率✅）
        '03147.HK':  200,   # 南方中国创业板（大概率✅）
        '02845.HK':   50,   # GlobalX 中国电动车 ✅
    }

    rsrs_n         = 18
    rsrs_m         = 600
    buy_threshold  =  0.5
    sell_threshold = -0.5
    rank_days      = 20
    target_num     = 3

    policy_asset   = 100000   # 港元 HKD，可通过 --asset 命令行参数覆盖


# ======================== 2. 数据获取 ========================
def _fetch_ohlc(symbol: str, count: int, max_retries: int = 3) -> pd.DataFrame:
    """
    通过 akshare (东方财富) 获取港股/ETF 日线数据（前复权）。
    akshare 不接受 '.HK' 后缀，传入前自动去除。
    count: 所需交易日条数（反推 count*2 个日历天以覆盖节假日）。
    max_retries: 遇到服务器瞬时断连时的重试次数，每次间隔指数增长。
    """
    import time
    bare     = symbol.replace('.HK', '')
    end_dt   = datetime.datetime.today()
    start_dt = end_dt - datetime.timedelta(days=count * 3)

    last_err = None
    for attempt in range(max_retries):
        try:
            df = ak.stock_hk_hist(
                symbol=bare,
                period="daily",
                start_date=start_dt.strftime('%Y%m%d'),
                end_date=end_dt.strftime('%Y%m%d'),
                adjust="qfq",
            )
            df = df.rename(columns={'日期': 'date', '最高': 'high', '最低': 'low', '收盘': 'close'})
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').tail(count).reset_index(drop=True)
            return df[['date', 'high', 'low', 'close']]
        except Exception as e:
            last_err = e
            wait = 2 ** attempt          # 1s → 2s → 4s
            print(f"  [重试 {attempt + 1}/{max_retries}] {bare} 请求失败，{wait}s 后重试... ({e})")
            time.sleep(wait)

    raise RuntimeError(f"{symbol} 连续 {max_retries} 次请求失败: {last_err}")


# ======================== 3. 核心算法 ========================
def calc_rsrs(df: pd.DataFrame, n: int, m: int) -> float:
    """
    RSRS 标准分 Z-Score（与 MarketMgr.get_rsrs_signal 逻辑一致）：
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
    """动量质量评分 = (年化收益率 / 年化波动率) × R²（平滑夏普比率）"""
    prices = df['close'].values[-rank_days:]
    if len(prices) < rank_days:
        return -999.0
    y = np.log(prices)
    x = np.arange(len(y))
    slope, _, r_val, *_ = stats.linregress(x, y)
    annualized_return = np.exp(slope * 250) - 1
    annualized_vol = np.diff(y).std() * np.sqrt(250) + 1e-9
    return float((annualized_return / annualized_vol) * (r_val ** 2))


# ======================== 4. 主逻辑 ========================
def run(policy_asset: float):
    now = datetime.datetime.now()
    sep = '=' * 62
    print(f"\n{sep}")
    print(f"  36号策略港股版 — 调仓方案")
    print(f"  执行时间 : {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  策略仓位 : HKD {policy_asset:,.0f}")
    print(f"{sep}\n")

    ohlc_cache: dict[str, pd.DataFrame] = {}   # 缓存本次所有 OHLC 请求，避免重复拉取

    # ── 1. RSRS 择时信号 ────────────────────────────────────────
    idx_sym  = Config.rsrs_index
    idx_name = Config.symbol_to_name.get(idx_sym, idx_sym)
    print(f">>> [择时] 计算 RSRS，基准: {idx_sym} {idx_name}")
    try:
        idx_df = _fetch_ohlc(idx_sym, count=Config.rsrs_n + Config.rsrs_m + 20)
        # 存入缓存，动量扫描阶段直接复用，避免重复请求
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
        target_list = list(Config.safe_havens)
        print(">>> [信号] 防御模式 — 撤离至避险资产\n")

    else:
        # 进攻模式（z > buy_threshold）或震荡区间（sell_threshold ≤ z ≤ buy_threshold）
        # 无论哪种，都计算动量排名并输出目标组合：
        #   · 震荡区间时：空仓用户参考此方案建仓；已有持仓则维持不动
        #   · 进攻模式时：正常执行换仓
        if z > Config.buy_threshold:
            print(">>> [信号] 进攻模式 — 计算各 ETF 动量评分...\n")
        else:
            print(">>> [信号] 震荡区间 — 计算当前动量排名作为参考...\n")
            print("    ★ 空仓用户：建议按下方方案建仓")
            print("    ★ 已有持仓：市场无明确趋势，建议维持现有仓位不动\n")

        full_pool = Config.all_symbols[:]
        scores: list[dict] = []

        print(f"  {'代码':<12}  {'名称':<22}  {'动量得分':>10}")
        print(f"  {'-' * 50}")
        for code in full_pool:
            name = Config.symbol_to_name.get(code, code)
            try:
                df = _fetch_ohlc(code, count=max(Config.rank_days + 10, 30))
                ohlc_cache[code] = df
                s = calc_momentum(df, Config.rank_days)
                flag = "✓" if s > 0 else " "
                print(f"  {code:<12}  {name:<22}  {s:>10.4f} {flag}")
                if s > 0:
                    scores.append({'code': code, 'score': s})
            except Exception as e:
                print(f"  {code:<12}  {name:<22}  获取失败: {e}")
        print(f"  {'-' * 50}\n")

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
            target_list = list(Config.safe_havens)
            print("  所有标的动量为负，切换至避险资产\n")

    # ── 3. 输出调仓方案 ─────────────────────────────────────────
    target_names = [Config.symbol_to_name.get(c, c) for c in target_list]
    target_value = policy_asset / len(target_list)

    print(f">>> [目标持仓]  {target_names}")
    print(f"    每仓目标金额: HKD {target_value:,.0f}\n")

    print(f"  {sep}")
    print(f"  调仓方案")
    print(f"  {sep}")
    print(f"  {'代码':<12}  {'名称':<22}  {'最新价(HKD)':>12}  "
          f"{'建议股数':>10}  {'参考手数':>8}  {'估算金额(HKD)':>14}")
    print(f"  {'-' * 84}")

    for code in target_list:
        name = Config.symbol_to_name.get(code, code)
        try:
            # 复用动量扫描缓存；防御模式下 target 不在缓存里，重新拉取
            # count=15 反推 30 个日历天，应对港股长假（圣诞+元旦、清明+劳动节等）
            df = ohlc_cache.get(code)
            if df is None:
                df = _fetch_ohlc(code, count=15)
            price = float(df['close'].iloc[-1])
            lot = Config.lot_sizes.get(code, 100)
            lot_count     = int(target_value / price / lot)
            actual_shares = lot_count * lot
            est_value     = actual_shares * price

            if lot_count == 0:
                min_invest = price * lot
                print(f"  {code:<12}  {name:<22}  {price:>9.3f} HKD  "
                      f"  ⚠ 仓位不足以买入1手（最低需 HKD {min_invest:,.0f}，当前每仓 HKD {target_value:,.0f}）")
            else:
                print(f"  {code:<12}  {name:<22}  {price:>9.3f} HKD  "
                      f"{actual_shares:>10,} 股  {lot_count:>5} 手  "
                      f"≈{est_value:>10,.0f} HKD")
        except Exception as e:
            print(f"  {code:<12}  {name:<22}  价格获取失败: {e}")

    print(f"  {'-' * 84}")
    print(f"\n  ℹ  手数来自 Config.lot_sizes 参考值，HKEX 可能调整，下单前请在券商确认。")
    print(f"{sep}\n")


# ======================== 5. 入口 ========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="36号策略港股版 — 输出调仓方案")
    parser.add_argument(
        '--asset', type=float, default=Config.policy_asset,
        help=f'策略仓位金额（港元），默认 {Config.policy_asset:,.0f}'
    )
    args = parser.parse_args()
    run(policy_asset=args.asset)
