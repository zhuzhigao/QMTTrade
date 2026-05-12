# -*- coding: utf-8 -*-
"""
kj202512_dama.py — 菜场大妈高股息子策略（独立运行）
预算：24,000 元（占总盘 8 万的 30%）

选股逻辑（翻译自聚宽 DaMa_Strategy）：
  1. 全 A 股（排除 KCBJ / ST / 停牌）
  2. 计算股息率 = DPS / 当前价格（DPS 取 PershareIndex.s_fa_dps）
     取股息率最高的前 25%（对应原版 get_dividend_ratio_filter_list p2=0.26）
  3. 按总市值升序（小市值优先）
  4. 价格 < 9 元（低价股过滤）
  5. 持 1 只，选 3 只候选

执行计划（对应原版 run_monthly 15）：
  - 每月第 15 个交易日之后的首次运行（即月中偏后）10:30 调仓
  - 09:31 检查空仓期（本策略无空仓月，但保留机制）
  - 14:00 & 14:50 — 涨停打开巡检
  - 14:45 — 止损巡检（20% 止损）

运行方式：python kj202512_dama.py [-m REAL]
"""

import os
import sys
import time
import datetime
import argparse
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
    Strategy, make_logger, get_universe, filter_st,
    filter_suspended, filter_limit_up, filter_limit_down,
    get_latest_prices, get_financial_batch, get_trading_day_of_month, BEIJING_TZ
)

LOG = make_logger('kj202512-DaMa')
DEBUG = True

TRIGGER_TRADING_DAY = 15   # 每月第几个交易日触发调仓

# ────────────────────────────────────────────────────
# 菜场大妈策略
# ────────────────────────────────────────────────────

class DaMaStrategy(Strategy):

    TOTAL_BUDGET   = 24_000   # 元
    MAX_SELECT     = 3        # 最多候选
    MAX_HOLD       = 1        # 最大持仓数
    HIGH_DIV_PCT   = 0.25     # 高股息：取全市场前 25%
    MAX_PRICE      = 9.0      # 价格上限（元）

    def __init__(self, trader, account, debug: bool):
        _base = current_dir
        super().__init__(
            name='菜场大妈',
            trader=trader, account=account,
            total_budget=self.TOTAL_BUDGET,
            debug=debug,
            state_file=os.path.join(_base, 'dama_state.json'),
            ledger_file=os.path.join(_base, 'dama_holdings.json'),
            use_stoploss=True,
        )
        LOG.info(f"菜场大妈策略初始化完成，预算 {self.TOTAL_BUDGET:,} 元，"
                 f"触发日：每月第 {TRIGGER_TRADING_DAY} 个交易日")

    # ── 每秒调用入口 ──────────────────────────

    def handlebar(self):
        now = datetime.datetime.now(BEIJING_TZ)
        t     = now.strftime('%H:%M:%S')
        today = now.strftime('%Y%m%d')
        month = now.month

        # 10:30 — 月度调仓（第 15 个交易日之后首次）
        if (t >= '10:30:00'
                and self.monthly_adjusted_month != month):
            td = get_trading_day_of_month()
            if td >= TRIGGER_TRADING_DAY:
                LOG.info(f"===== [菜场大妈] 月度调仓（本月第 {td} 个交易日） =====")
                self._run_monthly(month)

        # 14:00 & 14:50 — 涨停打开巡检
        if t in ('14:00:00', '14:01:00', '14:50:00', '14:51:00'):
            self.sell_when_limit_up_opened()

        # 14:45 — 止损巡检（每天一次）
        if t >= '14:45:00' and t <= '14:55:00' and self.trade_date != today:
            self.check_stoploss()
            self.trade_date = today

    # ── 核心逻辑 ─────────────────────────────

    def _run_monthly(self, month: int):
        try:
            target = self._select()
            LOG.info(f"[菜场大妈] 月度选股结果: {target}")
            self.adjust(target, self.MAX_HOLD)
            self.monthly_adjusted_month = month
        except Exception as e:
            LOG.exception(f"[菜场大妈] 月度调仓异常: {e}")
            self.monthly_adjusted_month = month

    def _select(self) -> list:
        LOG.info("[菜场大妈] 开始全 A 股筛选...")

        # ── Step 1: 基础过滤 ──────────────────
        universe = get_universe()
        LOG.info(f"全 A 股: {len(universe)} 只")
        universe = filter_st(universe)
        LOG.info(f"过滤 ST 后: {len(universe)} 只")
        universe = filter_suspended(universe)
        LOG.info(f"过滤停牌后: {len(universe)} 只")

        if not universe:
            LOG.warning("[菜场大妈] 候选股为空，跳过调仓")
            return []

        # ── Step 2: 获取当前价格 ──────────────
        LOG.info("[菜场大妈] 获取最新价格...")
        prices = get_latest_prices(universe)

        # ── Step 3: 批量财务数据（DPS + 市值）──
        LOG.info("[菜场大妈] 批量获取财务数据（PershareIndex）...")
        fin = get_financial_batch(universe, tables=['PershareIndex'], start_time='20230101')
        LOG.info(f"成功获取财务数据: {len(fin)} 只")

        # ── Step 4: 计算股息率，筛选前 25% ────
        div_rows = []
        for code in universe:
            try:
                stock_fin = fin.get(code)
                if not stock_fin:
                    continue
                ps = stock_fin.get('PershareIndex')
                if ps is None or ps.empty:
                    continue

                dps = ps.iloc[-1].get('s_fa_dps', None)  # 每股现金股息
                if dps is None or dps <= 0:
                    continue

                price = prices.get(code, 0)
                if price <= 0:
                    continue

                div_yield = dps / price
                div_rows.append({'code': code, 'div_yield': div_yield})

            except Exception:
                continue

        if not div_rows:
            LOG.warning("[菜场大妈] 无有效股息率数据，跳过")
            return []

        df_div = pd.DataFrame(div_rows).set_index('code')
        df_div = df_div.sort_values('div_yield', ascending=False)
        top_n  = max(1, int(self.HIGH_DIV_PCT * len(df_div)))
        high_div_codes = list(df_div.index[:top_n])
        LOG.info(f"[菜场大妈] 高股息前 {self.HIGH_DIV_PCT:.0%}（{top_n} 只）: "
                 f"{high_div_codes[:5]}...")

        # ── Step 5: 按总市值升序 ──────────────
        market_caps = self._get_market_caps(high_div_codes, prices)
        high_div_codes.sort(key=lambda c: market_caps.get(c, float('inf')))

        # ── Step 6: 价格 < MAX_PRICE 过滤 ────
        positions = self.trader.query_stock_positions(self.account)
        holdings  = [p.stock_code for p in positions
                     if self.ledger.is_in_ledger(p.stock_code)] if positions else []

        final = []
        for code in high_div_codes:
            price = prices.get(code, 0)
            # 已持仓的不受价格限制（避免频繁换仓）
            if code in holdings or (0 < price < self.MAX_PRICE):
                final.append(code)
            if len(final) >= self.MAX_SELECT * 3:
                break

        LOG.info(f"[菜场大妈] 价格过滤（<{self.MAX_PRICE}元）后: {len(final)} 只")

        # ── Step 7: 涨跌停过滤 ───────────────
        prices_final = get_latest_prices(final)
        final = filter_limit_up(final, holdings, prices_final)
        final = filter_limit_down(final, holdings, prices_final)
        final = final[:self.MAX_SELECT]

        # 输出候选明细
        LOG.info(f"[菜场大妈] 最终选股（前{self.MAX_SELECT}）:")
        for code in final:
            dy = df_div.loc[code, 'div_yield'] if code in df_div.index else 0
            LOG.info(f"  {code}  股息率={dy:.2%}  价格={prices.get(code,0):.2f}  "
                     f"市值={market_caps.get(code,0)/1e8:.2f}亿")

        return final

    def _get_market_caps(self, codes: list, prices: dict) -> dict:
        """估算市值 = 当前价 × 总股本"""
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
    parser = argparse.ArgumentParser(description='kj202512 菜场大妈高股息子策略')
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

    strategy = DaMaStrategy(trader, acc, debug=DEBUG)

    LOG.info("进入主事件循环，Ctrl+C 退出")
    try:
        while True:
            strategy.handlebar()
            time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("收到停止信号，断开连接...")
        trader.stop()
