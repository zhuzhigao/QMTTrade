# -*- coding: utf-8 -*-
"""
kj202512_etf.py — ETF 轮动子策略（独立运行）
预算：16,000 元（占总盘 8 万的 20%）

标的池（四种独立逻辑）：
  - 510300.SH 沪深300：A股大盘均衡
  - 515080.SH 中证红利：高股息价值，与成长风格负相关
  - 159915.SZ 创业板100：A股成长/科技
  - 511010.SH 30年国债：利率避险，与权益负相关

选股逻辑（翻译自聚宽 ETF_Strategy）：
  1. 对每只 ETF 计算 25 日线性回归动量分数 = 年化收益 × R²
  2. 减去 200 日长期动量/6（加入均值回归惩罚）
  3. 只有当最高分 - 最低分在 (0.1, 15) 区间内才建仓，否则空仓
  4. RSRS 择时：当前 18 日斜率 > (均值 - 2σ) 才买入

执行计划（对应原聚宽 run_daily）：
  - 09:35：每日计算分数，若目标 ETF 变化则调仓
  - 14:45：涨停打开巡检（ETF 一般无涨停，保留逻辑备用）

运行方式：python kj202512_etf.py [-m REAL]
"""

import os
import sys
import math
import time
import datetime
import argparse
import numpy as np
from scipy import stats
from datetime import timezone, timedelta
from xtquant import xtdata, xtconstant
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for _p in (current_dir, parent_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from kj202512_base import (
    Strategy, make_logger, get_latest_prices, BEIJING_TZ
)
from utils.stockmgr import StockMgr

LOG = make_logger('kj202512-ETF')
DEBUG = True

# ────────────────────────────────────────────────────
# ETF 策略
# ────────────────────────────────────────────────────

class ETFStrategy(Strategy):

    TOTAL_BUDGET  = 16_000    # 元
    MAX_HOLD      = 1         # 最多持有 1 只 ETF
    M_DAYS        = 25        # 短期动量窗口（交易日）
    MID_DAYS      = 60        # 中期动量窗口（交易日）
    L_DAYS        = 200       # 长期动量窗口（用于反转惩罚）
    RSRS_N        = 18        # RSRS 短窗口
    RSRS_M        = 250       # RSRS 历史基准窗口
    STOPLOSS_ETF  = 0.08      # ETF 持仓亏损 8% 强制切换至国债
    BOND_ETF      = '511010.SH'

    ETF_POOL = [
        '510300.SH',   # 沪深300（大盘均衡）
        '515080.SH',   # 中证红利（高股息价值，与成长负相关）
        '159915.SZ',   # 创业板100（A股成长/科技）
        '511010.SH',   # 30年国债ETF（利率避险）
    ]

    def __init__(self, trader, account, debug: bool):
        _base = current_dir
        super().__init__(
            name='ETF轮动',
            trader=trader, account=account,
            total_budget=self.TOTAL_BUDGET,
            debug=debug,
            state_file=os.path.join(_base, 'etf_state.json'),
            ledger_file=os.path.join(_base, 'etf_holdings.json'),
            use_stoploss=False,
        )
        if not self.state.get('last_adjust_date'):
            self.state.set('last_adjust_date', '')
        LOG.info(f"ETF轮动策略初始化完成，预算 {self.TOTAL_BUDGET:,} 元，"
                 f"标的池: {self.ETF_POOL}")

    # ── 每秒调用入口 ──────────────────────────

    def handlebar(self):
        now = datetime.datetime.now(BEIJING_TZ)
        t = now.strftime('%H:%M:%S')
        today = now.strftime('%Y%m%d')

        # 09:35 — 每日选股 + 调仓（每天最多执行一次）
        if t >= '09:35:00' and self.trade_date != today:
            LOG.info("===== [ETF轮动] 每日选股 & 调仓 =====")
            self._run_daily(today)

        # 14:30 — ETF 止损巡检
        if '14:30:00' <= t <= '14:35:00':
            self._check_etf_stoploss()

        # 14:45 — 涨停打开巡检
        if '14:45:00' <= t <= '14:55:00':
            self.sell_when_limit_up_opened()

    # ── 核心逻辑 ─────────────────────────────

    def _in_cool_period(self, today: str) -> bool:
        """距上次换仓不足 COOL_DAYS 个交易日则返回 True"""
        last = self.state.get('last_adjust_date')
        if not last:
            return False
        try:
            days = xtdata.get_trading_dates('SH', last, today)
            elapsed = len(days) - 1  # 包含两端，减1得区间内交易日数
            if elapsed < self.COOL_DAYS:
                LOG.info(f"[冷却期] 上次换仓 {last}，已过 {elapsed} 个交易日，"
                         f"冷却期 {self.COOL_DAYS} 日未满，跳过调仓")
                return True
        except Exception:
            pass
        return False

    def _run_daily(self, today: str):
        try:
            ranked = self._get_rank()
            LOG.info(f"[选股结果] 目标ETF: {ranked}")

            if self._in_cool_period(today):
                return

            # 记录调仓前持仓，用于判断是否实际发生换仓
            positions = self.trader.query_stock_positions(self.account)
            hold_before = {p.stock_code for p in positions
                           if self.ledger.is_in_ledger(p.stock_code) and p.volume > 0} \
                          if positions else set()
            target = set(ranked[:self.MAX_HOLD]) if ranked else set()

            self.adjust(ranked, self.MAX_HOLD)

            if hold_before != target:
                self.state.set('last_adjust_date', today)
                LOG.info(f"[冷却期] 换仓完成，冷却期起始日更新为 {today}")
        except Exception as e:
            LOG.exception(f"[ETF轮动] 每日调仓异常: {e}")
        finally:
            self.trade_date = today  # 无论成功与否，今日已执行

    def _check_etf_stoploss(self):
        """ETF 止损：持仓亏损超 8% 时强制切换至国债，不进入静默期"""
        positions = self.trader.query_stock_positions(self.account)
        if not positions:
            return
        for pos in positions:
            code = pos.stock_code
            if not self.ledger.is_in_ledger(code):
                continue
            if code == self.BOND_ETF or pos.volume <= 0 or pos.open_price <= 0:
                continue
            prices = get_latest_prices([code])
            cur = prices.get(code, 0)
            if cur <= 0:
                continue
            drop = (pos.open_price - cur) / pos.open_price
            LOG.info(f"[止损巡检] {code} 均价:{pos.open_price:.4f} "
                     f"现价:{cur:.4f} 跌幅:{drop:.2%}")
            if drop >= self.STOPLOSS_ETF:
                LOG.warning(f"[ETF止损] {code} 跌幅 {drop:.1%} ≥ "
                            f"{self.STOPLOSS_ETF:.0%}，强制切换至国债")
                self.adjust([self.BOND_ETF], self.MAX_HOLD)
                break  # 每次只处理一只，等下次巡检确认

    def _get_rank(self) -> list:
        """计算各 ETF 动量分数，返回按分数降序排列的列表（已通过 RSRS 过滤）"""
        # 下载最新日线（取 RSRS 所需最长窗口，覆盖所有计算需求）
        start_rsrs = (datetime.datetime.now(BEIJING_TZ)
                      - datetime.timedelta(days=(self.RSRS_M + self.RSRS_N) * 2)).strftime('%Y%m%d')
        StockMgr.download_history(self.ETF_POOL, start_time=start_rsrs, period='1d')

        scores = {}
        for etf in self.ETF_POOL:
            score = self._calc_score(etf)
            if score is not None:
                scores[etf] = score
                LOG.info(f"  {etf} 动量分数: {score:.4f}")

        if not scores:
            LOG.warning("[ETF轮动] 所有ETF分数计算失败，返回空列表")
            return []

        # 分数区间过滤：max-min 在 (0.1, 15) 才交易
        vals = list(scores.values())
        spread = max(vals) - min(vals)
        LOG.info(f"[ETF轮动] 分数区间: {spread:.4f}（需在 0.1~15 之间才建仓）")
        if not (0.1 < spread < 15):
            LOG.warning(f"[ETF轮动] 分数区间={spread:.4f} 超出范围，本次空仓")
            return []

        # 按分数降序
        ranked = sorted(scores.keys(), key=lambda e: scores[e], reverse=True)

        # RSRS 过滤（国债 511010.SH 免于 RSRS 检测，作为防御保底）
        BOND_ETF = '511010.SH'
        filtered = []
        for etf in ranked:
            if etf == BOND_ETF or self._rsrs_pass(etf):
                filtered.append(etf)
            else:
                LOG.info(f"  {etf} RSRS 不通过，排除")

        # 全部权益 ETF 均被排除时，强制切换到国债
        equity_passed = [e for e in filtered if e != BOND_ETF]
        if not equity_passed and BOND_ETF in self.ETF_POOL:
            LOG.warning("[ETF轮动] 全部权益ETF RSRS不通过，强制切换至国债")
            return [BOND_ETF]

        return filtered

    def _calc_score(self, etf: str) -> float | None:
        """计算单只 ETF 的复合动量分数（双周期加权 + 长期反转惩罚）"""
        try:
            data_l = xtdata.get_market_data_ex(['close'], [etf], period='1d',
                                                count=self.L_DAYS)
            if etf not in data_l or data_l[etf]['close'].empty:
                return None
            all_closes = np.log(data_l[etf]['close'].values.astype(float))
            if len(all_closes) < self.L_DAYS:
                return None

            def _momentum(closes):
                x = np.arange(len(closes))
                slope, intercept, _, _, _ = stats.linregress(x, closes)
                ann_ret = math.pow(math.exp(slope), 250) - 1
                y_hat = slope * x + intercept
                ss_res = np.sum((closes - y_hat) ** 2)
                ss_tot = np.var(closes, ddof=1) * (len(closes) - 1)
                r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
                return ann_ret * r2

            # 短期（25日）× 0.6 + 中期（60日）× 0.4
            score_short = _momentum(all_closes[-self.M_DAYS:])
            score_mid   = _momentum(all_closes[-self.MID_DAYS:])
            score = 0.6 * score_short + 0.4 * score_mid

            # 长期（200日）反转惩罚 × 1/6
            score -= _momentum(all_closes) / 6

            LOG.debug(f"  {etf} score_25d={score_short:.4f} score_60d={score_mid:.4f} "
                      f"final={score:.4f}")
            return score

        except Exception as e:
            LOG.warning(f"[ETF分数] {etf} 计算异常: {e}")
            return None

    def _rsrs_pass(self, etf: str) -> bool:
        """RSRS 择时：当前 18 日斜率 > (历史均值 - 0.5σ)"""
        try:
            total = self.RSRS_N + self.RSRS_M
            data = xtdata.get_market_data_ex(['high', 'low'], [etf], period='1d', count=total)
            if etf not in data:
                return True  # 数据不足时放行
            df = data[etf]
            highs = df['high'].values.astype(float)
            lows  = df['low'].values.astype(float)
            if len(highs) < self.RSRS_N + 2:
                return True

            slopes = []
            for i in range(len(highs) - self.RSRS_N + 1):
                s, _, _, _, _ = stats.linregress(lows[i:i + self.RSRS_N],
                                                  highs[i:i + self.RSRS_N])
                slopes.append(s)

            current_slope = slopes[-1]
            history = slopes[:-1]
            threshold = np.mean(history) - 0.5 * np.std(history)
            result = current_slope > threshold
            LOG.info(f"  {etf} RSRS slope={current_slope:.4f}  threshold={threshold:.4f}  "
                     f"通过={'✓' if result else '✗'}")
            return result

        except Exception as e:
            LOG.warning(f"[RSRS] {etf} 计算异常: {e}，默认放行")
            return True


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
    parser = argparse.ArgumentParser(description='kj202512 ETF轮动子策略')
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

    strategy = ETFStrategy(trader, acc, debug=DEBUG)

    LOG.info("进入主事件循环，Ctrl+C 退出")
    try:
        while True:
            strategy.handlebar()
            time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("收到停止信号，断开连接...")
        trader.stop()
