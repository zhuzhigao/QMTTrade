# -*- coding: utf-8 -*-
"""
kj202512_pb.py — PB 低估值子策略（独立运行）
预算：16,000 元（占总盘 8 万的 20%）

选股逻辑（翻译自聚宽 PB_Strategy）：
  过滤条件（全 A 股，排除 KCBJ / ST / 停牌）：
    - PB < 0.98（市净率极低，资产折价）
    - ROE > 15%（用 equity_roe 代替原版 ROA > 15%，xtquant 无直接 ROA 字段）
    - EPS > 0（正盈利）
    - 经营现金流 > 0（排除空壳）
    - 营业利润同比增长 > 0（排除业绩恶化）
  排序：ROE 降序，取前 3，持仓 1 只

执行计划：
  - 每月首个满足条件的交易日 09:35 调仓（对应原版 run_monthly 1）

运行方式：python kj202512_pb.py [-m REAL]
"""

import os
import sys
import time
import datetime
import argparse
from datetime import timezone, timedelta
from xtquant import xtdata, xtconstant
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from kj202512.kj202512_base import (
    Strategy, make_logger, get_universe, filter_st,
    filter_suspended, filter_limit_up, filter_limit_down,
    get_latest_prices, get_financial_batch, BEIJING_TZ
)

LOG = make_logger('kj202512-PB')
DEBUG = True

# ────────────────────────────────────────────────────
# PB 策略
# ────────────────────────────────────────────────────

class PBStrategy(Strategy):

    TOTAL_BUDGET   = 16_000   # 元
    MAX_SELECT     = 3        # 最多输出候选数
    MAX_HOLD       = 1        # 最大持股数
    REBALANCE_DAY  = 1        # 每月第几个自然日之后的首个交易日触发

    def __init__(self, trader, account, debug: bool):
        _base = current_dir
        super().__init__(
            name='PB策略',
            trader=trader, account=account,
            total_budget=self.TOTAL_BUDGET,
            debug=debug,
            state_file=os.path.join(_base, 'pb_state.json'),
            ledger_file=os.path.join(_base, 'pb_holdings.json'),
            use_stoploss=True,   # 原版有 close_for_stoplost
        )
        LOG.info(f"PB策略初始化完成，预算 {self.TOTAL_BUDGET:,} 元")

    # ── 每秒调用入口 ──────────────────────────

    def handlebar(self):
        now = datetime.datetime.now(BEIJING_TZ)
        t   = now.strftime('%H:%M:%S')
        today = now.strftime('%Y%m%d')
        month = now.month

        # 09:35 — 月度调仓（每月首次）
        if (t >= '09:35:00'
                and now.day >= self.REBALANCE_DAY
                and self.monthly_adjusted_month != month):
            LOG.info("===== [PB策略] 月度调仓 =====")
            self._run_monthly(month)

        # 14:00 & 14:50 — 涨停打开巡检（下午两档）
        if t in ('14:00:00', '14:01:00', '14:50:00', '14:51:00'):
            self.sell_when_limit_up_opened()

        # 14:45 — 止损巡检
        if t >= '14:45:00' and t <= '14:55:00' and self.trade_date != today:
            self.check_stoploss()
            self.trade_date = today  # 止损检查每天一次

    # ── 核心逻辑 ─────────────────────────────

    def _run_monthly(self, month: int):
        try:
            target = self._select()
            LOG.info(f"[PB策略] 月度选股结果: {target}")
            self.adjust(target, self.MAX_HOLD)
            self.monthly_adjusted_month = month
        except Exception as e:
            LOG.exception(f"[PB策略] 月度调仓异常: {e}")
            self.monthly_adjusted_month = month  # 失败也锁月，下月重试

    def _select(self) -> list:
        LOG.info("[PB策略] 开始全 A 股筛选...")

        # ── Step 1: 基础过滤 ──────────────────
        universe = get_universe()
        LOG.info(f"全 A 股: {len(universe)} 只")

        universe = filter_st(universe)
        LOG.info(f"过滤 ST 后: {len(universe)} 只")

        universe = filter_suspended(universe)
        LOG.info(f"过滤停牌后: {len(universe)} 只")

        if not universe:
            LOG.warning("[PB策略] 候选股为空，跳过调仓")
            return []

        # ── Step 2: 财务数据过滤 ──────────────
        LOG.info("[PB策略] 批量获取财务数据（PershareIndex / Income）...")
        fin = get_financial_batch(
            universe,
            tables=['PershareIndex', 'Income'],
            start_time='20230101'
        )
        LOG.info(f"成功获取财务数据: {len(fin)} 只")

        # 获取最新价格（用于计算 PB）
        LOG.info("[PB策略] 获取最新价格...")
        prices = get_latest_prices(universe)

        candidates = []
        for code in universe:
            try:
                stock_fin = fin.get(code)
                if not stock_fin:
                    continue

                pershare = stock_fin.get('PershareIndex')
                income   = stock_fin.get('Income')
                if pershare is None or income is None:
                    continue
                if pershare.empty or income.empty:
                    continue

                last_ps = pershare.iloc[-1]
                last_in = income.iloc[-1]

                # ROE（代替 ROA，xtquant 财务数据中 ROA 无直接字段）
                roe = last_ps.get('equity_roe', None)
                if roe is None or roe <= 15:
                    continue

                # EPS > 0
                eps = last_ps.get('s_fa_eps_basic', None)
                if eps is None or eps <= 0:
                    continue

                # 账面价值（BPS）
                bps = last_ps.get('s_fa_bps', None)
                if bps is None or bps <= 0:
                    continue

                # PB 计算
                price = prices.get(code, 0)
                if price <= 0:
                    continue
                pb = price / bps
                if pb >= 0.98:
                    continue

                # 经营性现金流 > 0（用净利润替代，xtquant Income 表字段）
                net_profit = last_in.get('net_profit_incl_min_int_inc_after', None)
                if net_profit is None or net_profit <= 0:
                    continue

                # 营业利润同比 > 0（比较最近两期）
                if len(income) >= 2:
                    prev_profit = income.iloc[-2].get('net_profit_incl_min_int_inc_after', 0)
                    if prev_profit > 0 and net_profit <= prev_profit:
                        continue

                candidates.append({
                    'code': code,
                    'pb':   pb,
                    'roe':  roe,
                    'eps':  eps,
                })

            except Exception:
                continue

        LOG.info(f"[PB策略] 通过财务过滤: {len(candidates)} 只")

        if not candidates:
            LOG.warning("[PB策略] 无符合条件股票，维持原仓位")
            return []

        # ── Step 3: 按 ROE 排序 ──────────────
        candidates.sort(key=lambda x: x['roe'], reverse=True)
        top = candidates[:self.MAX_SELECT]
        LOG.info("[PB策略] 候选前 3:")
        for c in top:
            LOG.info(f"  {c['code']}  PB={c['pb']:.3f}  ROE={c['roe']:.1f}%  EPS={c['eps']:.3f}")

        # ── Step 4: 涨跌停过滤 ───────────────
        top_codes = [c['code'] for c in top]
        positions = self.trader.query_stock_positions(self.account)
        holdings  = [p.stock_code for p in positions
                     if self.ledger.is_in_ledger(p.stock_code)] if positions else []

        prices_top = get_latest_prices(top_codes)
        top_codes = filter_limit_up(top_codes, holdings, prices_top)
        top_codes = filter_limit_down(top_codes, holdings, prices_top)

        LOG.info(f"[PB策略] 最终选股: {top_codes}")
        return top_codes


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
    parser = argparse.ArgumentParser(description='kj202512 PB低估值子策略')
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

    strategy = PBStrategy(trader, acc, debug=DEBUG)

    LOG.info("进入主事件循环，Ctrl+C 退出")
    try:
        while True:
            strategy.handlebar()
            time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("收到停止信号，断开连接...")
        trader.stop()
