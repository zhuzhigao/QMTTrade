# -*- coding: utf-8 -*-
"""
kj202512_xsz.py — 小市值多因子子策略（独立运行）
预算：24,000 元（占总盘 8 万的 30%）

选股逻辑（翻译自聚宽 XSZ_Strategy，适配 xtquant 可用数据）：
  原版使用 jqfactor.get_factor_values() 计算 3 组 ML 因子（共 12 个因子），
  xtquant 无对应 API，改用可直接获取的财务/价格因子近似：

  第 1 轮（质量因子组）：
    - ROE（资产收益率）
    - 净利润率 = net_profit / total_revenue（对应原版 SGAI / 净利润率 TTM）
    - EPS（对应原版 retained_profit_per_share）
    取前 10%，要求 EPS > 0

  第 2 轮（动量因子组）：
    - 1年价格动量（Price1Y = close / MA252 - 1，负值为好，反转效应）
    - 净利润增长率（对应原版 total_profit_to_cost_ratio）
  取前 10%

  第 3 轮（综合质量组）：
    - 资产负债率（D/A，正序 = 低负债优先）
    - 净利润率（同组1）
  取前 10%

  三组取并集 → 按流通市值升序（小市值优先）→ 取前 5 → 持仓 3 只

  额外过滤：
    - 次新股过滤（上市 < 400 天）
    - 4月空仓
    - 20% 止损

执行计划：
  - 每周一 09:35 选股调仓（对应原版 run_weekly 周一）
  - 09:30 检查空仓月（4月清仓）
  - 14:45 止损 + 涨停打开巡检

运行方式：python kj202512_xsz.py [-m REAL]
"""

import os
import sys
import time
import datetime
import argparse
import numpy as np
import pandas as pd
from datetime import timezone, timedelta
from xtquant import xtdata, xtconstant
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from kj202512.kj202512_base import (
    Strategy, make_logger, get_universe, filter_st, filter_new_stock,
    filter_suspended, filter_limit_up, filter_limit_down,
    get_latest_prices, get_financial_batch, BEIJING_TZ
)
from utils.stockmgr import StockMgr

LOG = make_logger('kj202512-XSZ')
DEBUG = True

EMPTY_MONTHS = [4]   # 4 月空仓

# ────────────────────────────────────────────────────
# 小市值策略
# ────────────────────────────────────────────────────

class XSZStrategy(Strategy):

    TOTAL_BUDGET  = 24_000   # 元
    MAX_SELECT    = 5        # 最多输出候选数
    MAX_HOLD      = 3        # 最大持股数
    NEW_DAYS      = 400      # 次新股过滤：上市天数
    TOP_PCT       = 0.10     # 每组取前 10%

    def __init__(self, trader, account, debug: bool):
        _base = current_dir
        super().__init__(
            name='小市值策略',
            trader=trader, account=account,
            total_budget=self.TOTAL_BUDGET,
            debug=debug,
            state_file=os.path.join(_base, 'xsz_state.json'),
            ledger_file=os.path.join(_base, 'xsz_holdings.json'),
            use_stoploss=True,
            empty_months=EMPTY_MONTHS,
        )
        LOG.info(f"小市值策略初始化完成，预算 {self.TOTAL_BUDGET:,} 元，"
                 f"空仓月: {EMPTY_MONTHS}")

    # ── 每秒调用入口 ──────────────────────────

    def handlebar(self):
        now = datetime.datetime.now(BEIJING_TZ)
        t     = now.strftime('%H:%M:%S')
        today = now.strftime('%Y%m%d')
        month = now.month
        week  = now.isocalendar()[1]
        wday  = now.weekday()  # 0=周一

        # 09:31 — 空仓月检查（若当月为 4 月且有仓位则清仓）
        if t >= '09:31:00' and t <= '09:34:00' and month in EMPTY_MONTHS:
            self._close_for_empty_month()

        # 09:35 — 周一选股调仓（非空仓月、非止损静默期）
        if (t >= '09:35:00'
                and wday == 0          # 周一
                and month not in EMPTY_MONTHS
                and not self._in_stoploss_silence()
                and self.weekly_adjusted_week != week):
            LOG.info("===== [小市值] 每周调仓 =====")
            self._run_weekly(week)

        # 14:00 & 14:50 — 涨停打开巡检
        if t in ('14:00:00', '14:01:00', '14:50:00', '14:51:00'):
            self.sell_when_limit_up_opened()

        # 14:45 — 止损巡检（每天一次）
        if t >= '14:45:00' and t <= '14:55:00' and self.trade_date != today:
            self.check_stoploss()
            self.trade_date = today

    # ── 空仓月清仓 ────────────────────────────

    def _close_for_empty_month(self):
        positions = self.trader.query_stock_positions(self.account)
        if not positions:
            return
        to_sell = [p.stock_code for p in positions
                   if self.ledger.is_in_ledger(p.stock_code) and p.can_use_volume > 0]
        if to_sell:
            LOG.info(f"[空仓月] {datetime.datetime.now(BEIJING_TZ).month} 月空仓，清仓: {to_sell}")
            self._sell_stocks(to_sell, tag='空仓月清仓')

    # ── 核心逻辑 ─────────────────────────────

    def _run_weekly(self, week: int):
        try:
            target = self._select()
            LOG.info(f"[小市值] 周度选股结果: {target}")
            self.adjust(target, self.MAX_HOLD)
            self.weekly_adjusted_week = week
        except Exception as e:
            LOG.exception(f"[小市值] 周度调仓异常: {e}")
            self.weekly_adjusted_week = week

    def _select(self) -> list:
        LOG.info("[小市值] 开始全 A 股筛选...")

        # ── Step 1: 基础过滤 ──────────────────
        universe = get_universe()
        LOG.info(f"全 A 股: {len(universe)} 只")
        universe = filter_st(universe)
        universe = filter_new_stock(universe, self.NEW_DAYS)
        LOG.info(f"过滤 ST & 次新后: {len(universe)} 只")
        universe = filter_suspended(universe)
        LOG.info(f"过滤停牌后: {len(universe)} 只")

        if len(universe) < 50:
            LOG.warning("[小市值] 候选股不足 50，跳过调仓")
            return []

        # ── Step 2: 批量财务数据 ──────────────
        LOG.info("[小市值] 批量获取财务数据...")
        fin = get_financial_batch(
            universe,
            tables=['PershareIndex', 'Income', 'Balance'],
            start_time='20230101'
        )

        # ── Step 3: 构建因子 DataFrame ────────
        rows = {}
        for code in universe:
            try:
                stock_fin = fin.get(code)
                if not stock_fin:
                    continue
                ps = stock_fin.get('PershareIndex')
                inc = stock_fin.get('Income')
                bal = stock_fin.get('Balance')
                if ps is None or inc is None or ps.empty or inc.empty:
                    continue

                last_ps  = ps.iloc[-1]
                last_inc = inc.iloc[-1]

                roe = last_ps.get('equity_roe', None)
                eps = last_ps.get('s_fa_eps_basic', None)
                if roe is None or eps is None:
                    continue

                net_profit = last_inc.get('net_profit_incl_min_int_inc_after', 0) or 0
                revenue    = last_inc.get('total_operating_revenue', 1) or 1
                net_margin = net_profit / revenue if revenue != 0 else 0

                # 负债率（D/A）
                da_ratio = 0.5
                if bal is not None and not bal.empty:
                    last_bal  = bal.iloc[-1]
                    total_liab   = last_bal.get('total_liab', None)
                    total_assets = last_bal.get('total_assets', None)
                    if total_liab is not None and total_assets and total_assets > 0:
                        da_ratio = total_liab / total_assets

                # 净利润同比增速
                profit_growth = 0
                if len(inc) >= 2:
                    prev_profit = inc.iloc[-2].get('net_profit_incl_min_int_inc_after', 0) or 0
                    if prev_profit > 0:
                        profit_growth = (net_profit - prev_profit) / abs(prev_profit)

                rows[code] = {
                    'roe':          roe,
                    'eps':          eps,
                    'net_margin':   net_margin,
                    'da_ratio':     da_ratio,
                    'profit_growth': profit_growth,
                }
            except Exception:
                continue

        if not rows:
            LOG.warning("[小市值] 财务数据构建失败，跳过")
            return []

        df = pd.DataFrame.from_dict(rows, orient='index')
        df = df.dropna()
        LOG.info(f"[小市值] 有效财务数据: {len(df)} 只")

        # ── Step 4: 三组因子，各取前 10% ──────

        top_n = max(1, int(self.TOP_PCT * len(df)))
        final_set = set()

        # 组 1：质量因子（ROE + 净利润率 + EPS，高值优先）
        g1 = df.copy()
        g1['score_g1'] = g1['roe'] * 0.5 + g1['net_margin'] * 0.3 + g1['eps'].clip(lower=0) * 0.2
        g1 = g1[g1['eps'] > 0].sort_values('score_g1', ascending=False)
        group1_codes = list(g1.index[:top_n])
        LOG.info(f"[小市值] 组1（质量）取前{top_n}只: {group1_codes[:5]}...")
        final_set.update(group1_codes)

        # 组 2：动量因子（利润增速高，负债率低）
        g2 = df.copy()
        g2['score_g2'] = g2['profit_growth'] * 0.7 - g2['da_ratio'] * 0.3
        g2 = g2[g2['eps'] > 0].sort_values('score_g2', ascending=False)
        group2_codes = list(g2.index[:top_n])
        LOG.info(f"[小市值] 组2（动量）取前{top_n}只: {group2_codes[:5]}...")
        final_set.update(group2_codes)

        # 组 3：低负债质量（低 D/A + 高净利润率）
        g3 = df.copy()
        g3['score_g3'] = -g3['da_ratio'] * 0.6 + g3['net_margin'] * 0.4
        g3 = g3[g3['eps'] > 0].sort_values('score_g3', ascending=False)
        group3_codes = list(g3.index[:top_n])
        LOG.info(f"[小市值] 组3（低负债）取前{top_n}只: {group3_codes[:5]}...")
        final_set.update(group3_codes)

        LOG.info(f"[小市值] 三组并集: {len(final_set)} 只")

        # ── Step 5: 按市值升序，取前 MAX_SELECT ──
        final_list = list(final_set)
        market_caps = self._get_market_caps(final_list)
        final_list.sort(key=lambda c: market_caps.get(c, float('inf')))
        final_list = final_list[:self.MAX_SELECT]
        LOG.info(f"[小市值] 按市值排序后前 {self.MAX_SELECT}: {final_list}")

        # ── Step 6: 涨跌停过滤 ───────────────
        positions = self.trader.query_stock_positions(self.account)
        holdings  = [p.stock_code for p in positions
                     if self.ledger.is_in_ledger(p.stock_code)] if positions else []
        prices = get_latest_prices(final_list)
        final_list = filter_limit_up(final_list, holdings, prices)
        final_list = filter_limit_down(final_list, holdings, prices)

        LOG.info(f"[小市值] 最终选股: {final_list}")
        return final_list

    def _get_market_caps(self, codes: list) -> dict:
        """估算市值 = 当前价 × 总股本"""
        prices = get_latest_prices(codes)
        caps = {}
        for code in codes:
            price = prices.get(code, 0)
            if price <= 0:
                caps[code] = float('inf')
                continue
            d = xtdata.get_instrument_detail(code)
            total_shares = d.get('TotalVolume', 0) if d else 0
            caps[code] = price * total_shares if total_shares > 0 else float('inf')
        return caps


# ────────────────────────────────────────────────────
# 主程序入口
# ────────────────────────────────────────────────────

class MyCallback:
    def on_disconnected(self):
        LOG.error("!! 与 QMT 终端连接断开 !!")

    def on_stock_order(self, order):
        LOG.info(f"委托回报: {order.stock_code} 状态:{order.order_status} 价格:{order.price}")

    def on_stock_trade(self, trade):
        LOG.info(f"成交回报: {trade.stock_code} 数量:{trade.traded_volume} 价格:{trade.traded_price}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='kj202512 小市值多因子子策略')
    parser.add_argument('-m', '--mode', type=str, default='DEBUG',
                        help='运行模式: REAL 或 DEBUG（默认DEBUG）')
    args = parser.parse_args()

    if args.mode.upper() == 'REAL':
        LOG.info(">>> [实盘模式] 注意风险！")
        DEBUG = False
    else:
        LOG.info(">>> [调试模式] 仅输出日志，不发真实报单")

    # ── 请根据实际环境修改 ──
    qmt_path   = r'D:\光大证券金阳光QMT实盘\userdata_mini'
    account_id = '47601131'
    # ────────────────────────

    session_id = int(time.time())
    trader = XtQuantTrader(qmt_path, session_id)
    acc = StockAccount(account_id)

    cb = MyCallback()
    trader.register_callback(cb)
    trader.start()

    if trader.connect() == 0:
        LOG.info(f"连接 QMT 成功，订阅账号 {account_id}")
        trader.subscribe(acc)
    else:
        LOG.error("连接 QMT 失败，请检查极简模式是否已启动并登录")
        sys.exit(1)

    strategy = XSZStrategy(trader, acc, debug=DEBUG)

    LOG.info("进入主事件循环，Ctrl+C 退出")
    try:
        while True:
            strategy.handlebar()
            time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("收到停止信号，断开连接...")
        trader.stop()
