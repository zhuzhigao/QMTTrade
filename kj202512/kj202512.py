# -*- coding: utf-8 -*-
"""
kj202512_base.py — 多策略公共基类与工具函数
原始来源：聚宽 kj201512 多策略分仓框架，翻译至 xtquant 极简模式

四个子策略共享：
  - Strategy 基类（持仓隔离、止损、空仓月、涨停卖出）
  - 股票池过滤函数（KCBJ、ST、停牌、涨跌停、次新股）
  - 批量买卖执行逻辑
"""

import os
import sys
import time
import datetime
import logging
import numpy as np
import pandas as pd
from datetime import timezone, timedelta
from xtquant import xtdata, xtconstant
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.utilities import StrategyLedger, StateManager, BlacklistManager, MessagePusher
from utils.stockmgr import StockMgr
from utils.trademgr import TradeMgr

BEIJING_TZ = timezone(timedelta(hours=8))

# ─────────────────────────────────────────────
# 日志工具
# ─────────────────────────────────────────────

def make_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s  %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


# ─────────────────────────────────────────────
# 全局工具函数（无状态，可被各策略直接调用）
# ─────────────────────────────────────────────

def get_universe() -> list:
    """获取全 A 股列表（含主板/中小板/创业板，排除 KCBJ）"""
    stocks = xtdata.get_stock_list_in_sector('沪深A股')
    if not stocks:
        stocks = xtdata.get_stock_list_in_sector('全部A股')
    return [s for s in stocks if _not_kcbj(s)]


def _not_kcbj(code: str) -> bool:
    """排除科创板(68x)和北交所(.BJ / 4xx / 8xx)"""
    if code.endswith('.BJ'):
        return False
    prefix = code.split('.')[0]
    if prefix.startswith('68') or prefix[0] in ('4', '8'):
        return False
    return True


def filter_st(stock_list: list) -> list:
    """过滤 ST / *ST / 退市风险股"""
    result = []
    for code in stock_list:
        d = xtdata.get_instrument_detail(code)
        if d:
            name = d.get('InstrumentName', '')
            if 'ST' not in name and '退' not in name and '*' not in name:
                result.append(code)
    return result


def filter_new_stock(stock_list: list, min_days: int) -> list:
    """过滤上市不足 min_days 天的次新股"""
    today = datetime.datetime.now(BEIJING_TZ).date()
    result = []
    for code in stock_list:
        d = xtdata.get_instrument_detail(code)
        if d:
            open_date_str = d.get('OpenDate', '')
            if open_date_str:
                try:
                    open_date = datetime.datetime.strptime(str(open_date_str), '%Y%m%d').date()
                    if (today - open_date).days >= min_days:
                        result.append(code)
                except Exception:
                    result.append(code)  # 无法解析则不过滤
    return result


def filter_suspended(stock_list: list) -> list:
    """过滤停牌股（前一交易日成交量为0）"""
    if not stock_list:
        return []
    data = xtdata.get_market_data_ex(['volume'], stock_list, period='1d', count=1)
    result = []
    for code in stock_list:
        if code in data:
            vol = data[code]['volume']
            if not vol.empty and vol.iloc[-1] > 0:
                result.append(code)
    return result


def get_latest_prices(stock_list: list) -> dict:
    """订阅并获取最新 tick 价格，返回 {code: price}"""
    for code in stock_list:
        xtdata.subscribe_quote(code, period='tick', count=1)
    tick = xtdata.get_full_tick(stock_list)
    return {code: tick[code].get('lastPrice', 0) for code in stock_list if code in tick}


def filter_limit_up(stock_list: list, holdings: list, prices: dict) -> list:
    """过滤涨停股（已持仓的保留，避免换仓选出同价股）"""
    result = []
    for code in stock_list:
        if code in holdings:
            result.append(code)
            continue
        d = xtdata.get_instrument_detail(code)
        if d:
            high_limit = d.get('UpStopPrice', 0)
            price = prices.get(code, 0)
            if high_limit > 0 and price >= high_limit:
                continue  # 涨停不买
        result.append(code)
    return result


def filter_limit_down(stock_list: list, holdings: list, prices: dict) -> list:
    """过滤跌停股（已持仓的保留）"""
    result = []
    for code in stock_list:
        if code in holdings:
            result.append(code)
            continue
        d = xtdata.get_instrument_detail(code)
        if d:
            low_limit = d.get('DownStopPrice', 0)
            price = prices.get(code, 0)
            if low_limit > 0 and price <= low_limit:
                continue  # 跌停不买
        result.append(code)
    return result


def get_financial_batch(stocks: list, tables: list, start_time: str) -> dict:
    """分批调用 get_financial_data（每批 200 只），合并结果"""
    result = {}
    chunk_size = 200
    for i in range(0, len(stocks), chunk_size):
        chunk = stocks[i:i + chunk_size]
        try:
            data = xtdata.get_financial_data(
                chunk, table_list=tables,
                start_time=start_time, report_type='announce_time'
            )
            result.update(data)
        except Exception as e:
            pass  # 单批失败不影响其他批次
    return result


def get_trading_day_of_month() -> int:
    """返回今天是本月第几个交易日（从1开始）"""
    now = datetime.datetime.now(BEIJING_TZ)
    month_start = now.strftime('%Y%m01')
    today_str = now.strftime('%Y%m%d')
    try:
        days = xtdata.get_trading_dates('SH', month_start, today_str)
        return len(days) if days else 0
    except Exception:
        return 0


# ─────────────────────────────────────────────
# Strategy 基类
# ─────────────────────────────────────────────

class Strategy:
    """
    子策略基类。
    负责：持仓隔离（StrategyLedger）、状态持久化（StateManager）、
          止损、空仓月、涨停打开卖出、通用买卖流程。
    """

    STOPLOSS_LEVEL = 0.20       # 跌幅达到此比例触发止损（按均价）
    STOPLOSS_SILENCE_DAYS = 28  # 止损后静默日历天数（约 20 个交易日）

    def __init__(self, name: str, trader: XtQuantTrader, account,
                 total_budget: float, debug: bool,
                 state_file: str, ledger_file: str,
                 use_stoploss: bool = False,
                 empty_months: list = None):
        self.name = name
        self.trader = trader
        self.account = account
        self.total_budget = total_budget
        self.debug = debug
        self.use_stoploss = use_stoploss
        self.empty_months = empty_months or []

        self.log = make_logger(name)
        self.state = StateManager(state_file, defaults={
            'monthly_adjusted_month': -1,
            'weekly_adjusted_week': -1,
            'stoploss_date': '',
            'trade_date': '',
        })
        self.ledger = StrategyLedger(ledger_file)
        self.pusher = MessagePusher()

    # ── 状态属性 ──────────────────────────────

    @property
    def monthly_adjusted_month(self) -> int:
        return self.state.get('monthly_adjusted_month')

    @monthly_adjusted_month.setter
    def monthly_adjusted_month(self, v):
        self.state.set('monthly_adjusted_month', v)

    @property
    def weekly_adjusted_week(self) -> int:
        return self.state.get('weekly_adjusted_week')

    @weekly_adjusted_week.setter
    def weekly_adjusted_week(self, v):
        self.state.set('weekly_adjusted_week', v)

    @property
    def stoploss_date(self) -> str:
        return self.state.get('stoploss_date')

    @stoploss_date.setter
    def stoploss_date(self, v: str):
        self.state.set('stoploss_date', v)

    @property
    def trade_date(self) -> str:
        return self.state.get('trade_date')

    @trade_date.setter
    def trade_date(self, v: str):
        self.state.set('trade_date', v)

    # ── 止损相关 ──────────────────────────────

    def _in_stoploss_silence(self) -> bool:
        if not self.stoploss_date:
            return False
        sl = datetime.datetime.strptime(self.stoploss_date, '%Y%m%d').date()
        return (datetime.datetime.now(BEIJING_TZ).date() - sl).days < self.STOPLOSS_SILENCE_DAYS

    def check_stoploss(self):
        """检查持仓是否触发止损，触发则清仓并进入静默期"""
        if not self.use_stoploss or self._in_stoploss_silence():
            return
        positions = self.trader.query_stock_positions(self.account)
        if not positions:
            return
        triggered = []
        for pos in positions:
            if not self.ledger.is_in_ledger(pos.stock_code):
                continue
            if pos.volume <= 0:
                continue
            prices = get_latest_prices([pos.stock_code])
            cur = prices.get(pos.stock_code, 0)
            if cur > 0 and pos.open_price > 0:
                drop = (pos.open_price - cur) / pos.open_price
                if drop >= self.STOPLOSS_LEVEL:
                    triggered.append(pos.stock_code)
                    self.log.warning(
                        f"[止损] {pos.stock_code} 均价{pos.open_price:.2f} 现价{cur:.2f} "
                        f"跌幅{drop:.1%} ≥ {self.STOPLOSS_LEVEL:.0%}"
                    )

        if triggered:
            self._sell_stocks(triggered, tag='止损清仓')
            today = datetime.datetime.now(BEIJING_TZ).strftime('%Y%m%d')
            self.stoploss_date = today
            self.log.warning(f"[止损] 触发止损，进入 {self.STOPLOSS_SILENCE_DAYS} 天静默期")
            self.pusher.send_strategy_report(
                self.name,
                sells=[f"{s} 止损卖出" for s in triggered],
                extra_msg=f"静默期至 {(datetime.datetime.now(BEIJING_TZ) + datetime.timedelta(days=self.STOPLOSS_SILENCE_DAYS)).strftime('%Y-%m-%d')}"
            )

    def sell_when_limit_up_opened(self):
        """14:00 / 14:50 巡检：昨日涨停今日涨停打开则卖出"""
        positions = self.trader.query_stock_positions(self.account)
        if not positions:
            return
        hold_codes = [p.stock_code for p in positions
                      if self.ledger.is_in_ledger(p.stock_code) and p.volume > 0]
        if not hold_codes:
            return

        # 获取昨日是否涨停
        data = xtdata.get_market_data_ex(['close'], hold_codes, period='1d', count=2)
        yesterday_limit_up = []
        for code in hold_codes:
            if code not in data:
                continue
            closes = data[code]['close']
            if len(closes) < 2:
                continue
            d = xtdata.get_instrument_detail(code)
            if not d:
                continue
            # 昨日涨停价估算：前收 × 1.1
            prev_close = closes.iloc[-2]
            est_high_lim = round(prev_close * 1.1, 2)
            if abs(closes.iloc[-1] - est_high_lim) / est_high_lim < 0.005:
                yesterday_limit_up.append(code)

        if not yesterday_limit_up:
            return

        prices = get_latest_prices(yesterday_limit_up)
        to_sell = []
        for code in yesterday_limit_up:
            d = xtdata.get_instrument_detail(code)
            if not d:
                continue
            high_lim = d.get('UpStopPrice', 0)
            cur = prices.get(code, 0)
            if high_lim > 0 and cur < high_lim:
                to_sell.append(code)
                self.log.info(f"[涨停打开] {code} 昨日涨停，今日打开，现价{cur:.2f}，卖出")

        if to_sell:
            self._sell_stocks(to_sell, tag='涨停打开卖出')

    # ── 通用买卖逻辑 ─────────────────────────

    def _sell_stocks(self, codes: list, tag: str = '卖出') -> dict:
        """卖出指定股票列表，返回 {code: pre_sell_market_value}"""
        sold = {}
        positions = self.trader.query_stock_positions(self.account)
        pos_map = {p.stock_code: p for p in positions} if positions else {}
        for code in codes:
            pos = pos_map.get(code)
            if pos is None or pos.can_use_volume <= 0:
                continue
            self.log.info(f"[{tag}] → 卖出 {code}  数量:{pos.can_use_volume}  "
                          f"市值:{pos.market_value:.0f}")
            seq = -1
            if not self.debug:
                seq = self.trader.order_stock(
                    self.account, code, xtconstant.STOCK_SELL,
                    pos.can_use_volume, xtconstant.LATEST_PRICE, 0,
                    f'kj202512_{self.name}', tag
                )
            if self.debug or seq != -1:
                self.ledger.remove(code)
                sold[code] = pos.market_value
        return sold

    def _buy_stocks(self, target_list: list, max_hold: int):
        """按等权买入 target_list 前 max_hold 只（扣除已持仓）"""
        positions = self.trader.query_stock_positions(self.account)
        hold_codes = {p.stock_code for p in positions
                      if self.ledger.is_in_ledger(p.stock_code) and p.volume > 0} \
            if positions else set()

        buy_list = [c for c in target_list[:max_hold] if c not in hold_codes]
        slots = max_hold - len(hold_codes)
        if slots <= 0 or not buy_list:
            self.log.info("[买入] 持仓已满或无新标的，跳过买入")
            return

        buy_list = buy_list[:slots]
        asset = self.trader.query_stock_asset(self.account)
        if not asset:
            self.log.warning("[买入] 获取资产失败，跳过")
            return

        budget = min(asset.cash, self.total_budget)
        per_stock = budget * 0.98 / len(buy_list)
        self.log.info(f"[买入] 可用现金:{asset.cash:.0f}  本次预算:{budget:.0f}  "
                      f"标的数:{len(buy_list)}  每只:{per_stock:.0f}")

        prices = get_latest_prices(buy_list)
        for code in buy_list:
            price = prices.get(code, 0)
            if price <= 0:
                self.log.warning(f"[买入] {code} 价格异常({price})，跳过")
                continue
            volume = int(per_stock / price / 100) * 100
            if volume < 100:
                self.log.warning(f"[买入] {code} 计算手数 < 100，跳过（per={per_stock:.0f} price={price:.2f}）")
                continue
            self.log.info(f"[买入] → {code}  价格:{price:.2f}  数量:{volume}  "
                          f"预估金额:{volume * price:.0f}")
            seq = -1
            if not self.debug:
                seq = self.trader.order_stock(
                    self.account, code, xtconstant.STOCK_BUY,
                    volume, xtconstant.LATEST_PRICE, 0,
                    f'kj202512_{self.name}', '调仓买入'
                )
            if self.debug or seq != -1:
                self.ledger.add(code)

    def adjust(self, target_list: list, max_hold: int):
        """
        标准调仓流程：先卖不在目标中的，等成交后买新标的。
        target_list 已按优先级排序，取前 max_hold 只为目标持仓。
        """
        positions = self.trader.query_stock_positions(self.account)
        pos_map = {p.stock_code: p for p in positions} if positions else {}

        # 1. 决定需要卖出的持仓
        sell_codes = [
            code for code in pos_map
            if self.ledger.is_in_ledger(code)
            and code not in target_list[:max_hold]
            and pos_map[code].can_use_volume > 0
        ]

        sells_info = [f"{c} 市值{pos_map[c].market_value:.0f}" for c in sell_codes]
        buys_info = [c for c in target_list[:max_hold]
                     if c not in pos_map or not self.ledger.is_in_ledger(c)]

        self.log.info(f"[调仓计划] 卖出: {sell_codes}  买入: {buys_info}")

        if not sell_codes and not buys_info:
            self.log.info("[调仓] 持仓无需变动")
            return

        sold = self._sell_stocks(sell_codes, tag='调仓卖出')

        if not self.debug and sold:
            TradeMgr.wait_for_sells(self.trader, self.account, sold, timeout=120, interval=5)

        self._buy_stocks(target_list, max_hold)

        # 推送调仓报告
        self.pusher.send_strategy_report(
            self.name,
            buys=buys_info,
            sells=sells_info
        )
