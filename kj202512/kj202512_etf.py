# -*- coding: utf-8 -*-
"""
kj202512_etf.py — ETF 轮动子策略（独立运行）
预算：16,000 元（占总盘 8 万的 20%）

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
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from kj202512.kj202512_base import (
    Strategy, make_logger, get_latest_prices, BEIJING_TZ
)
from utils.stockmgr import StockMgr

LOG = make_logger('kj202512-ETF')
DEBUG = True

# ────────────────────────────────────────────────────
# ETF 策略
# ────────────────────────────────────────────────────

class ETFStrategy(Strategy):

    TOTAL_BUDGET = 16_000     # 元
    MAX_HOLD     = 1          # 最多持有 1 只 ETF
    M_DAYS       = 25         # 短期动量窗口（交易日）
    L_DAYS       = 200        # 长期动量窗口（用于反转惩罚）
    RSRS_N       = 18         # RSRS 短窗口
    RSRS_M       = 250        # RSRS 历史基准窗口

    ETF_POOL = [
        '510180.SH',   # 上证180
        '159915.SZ',   # 创业板100（成长/科技/中小盘）
        '513100.SH',   # 纳指100（海外资产）
        '518880.SH',   # 黄金ETF（大宗商品/避险）
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

        # 14:45 — 涨停打开巡检
        if t >= '14:45:00' and t <= '14:55:00':
            self.sell_when_limit_up_opened()

    # ── 核心逻辑 ─────────────────────────────

    def _run_daily(self, today: str):
        try:
            ranked = self._get_rank()
            LOG.info(f"[选股结果] 目标ETF: {ranked}")
            self.adjust(ranked, self.MAX_HOLD)
        except Exception as e:
            LOG.exception(f"[ETF轮动] 每日调仓异常: {e}")
        finally:
            self.trade_date = today  # 无论成功与否，今日已执行

    def _get_rank(self) -> list:
        """计算各 ETF 动量分数，返回按分数降序排列的列表（已通过 RSRS 过滤）"""
        # 下载最新日线数据
        start_short = (datetime.datetime.now(BEIJING_TZ)
                       - datetime.timedelta(days=self.M_DAYS * 2)).strftime('%Y%m%d')
        start_long  = (datetime.datetime.now(BEIJING_TZ)
                       - datetime.timedelta(days=self.L_DAYS * 2)).strftime('%Y%m%d')
        start_rsrs  = (datetime.datetime.now(BEIJING_TZ)
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

        # RSRS 过滤
        filtered = []
        for etf in ranked:
            if self._rsrs_pass(etf):
                filtered.append(etf)
            else:
                LOG.info(f"  {etf} RSRS 不通过，排除")

        return filtered

    def _calc_score(self, etf: str) -> float | None:
        """计算单只 ETF 的复合动量分数"""
        try:
            # 短期（25日）
            data_s = xtdata.get_market_data_ex(['close'], [etf], period='1d',
                                                count=self.M_DAYS)
            if etf not in data_s or data_s[etf]['close'].empty:
                return None
            closes_s = np.log(data_s[etf]['close'].values.astype(float))
            if len(closes_s) < self.M_DAYS:
                return None
            x_s = np.arange(len(closes_s))
            slope_s, intercept_s, _, _, _ = stats.linregress(x_s, closes_s)
            ann_ret_s = math.pow(math.exp(slope_s), 250) - 1
            y_hat_s = slope_s * x_s + intercept_s
            ss_res = np.sum((closes_s - y_hat_s) ** 2)
            ss_tot = np.var(closes_s, ddof=1) * (len(closes_s) - 1)
            r2_s = 1 - ss_res / ss_tot if ss_tot > 0 else 0
            score = ann_ret_s * r2_s

            # 长期（200日）反转惩罚
            data_l = xtdata.get_market_data_ex(['close'], [etf], period='1d',
                                                count=self.L_DAYS)
            if etf in data_l and not data_l[etf]['close'].empty:
                closes_l = np.log(data_l[etf]['close'].values.astype(float))
                if len(closes_l) >= self.L_DAYS:
                    x_l = np.arange(len(closes_l))
                    slope_l, intercept_l, _, _, _ = stats.linregress(x_l, closes_l)
                    ann_ret_l = math.pow(math.exp(slope_l), 250) - 1
                    y_hat_l = slope_l * x_l + intercept_l
                    ss_res_l = np.sum((closes_l - y_hat_l) ** 2)
                    ss_tot_l = np.var(closes_l, ddof=1) * (len(closes_l) - 1)
                    r2_l = 1 - ss_res_l / ss_tot_l if ss_tot_l > 0 else 0
                    score -= ann_ret_l * r2_l / 6  # 长期反转 1/6 惩罚

            return score

        except Exception as e:
            LOG.warning(f"[ETF分数] {etf} 计算异常: {e}")
            return None

    def _rsrs_pass(self, etf: str) -> bool:
        """RSRS 择时：当前 18 日斜率 > (历史均值 - 2σ)"""
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
            threshold = np.mean(history) - 2 * np.std(history)
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
