# -*- coding: utf-8 -*-
"""
kj202590 — 固收+ ETF 再平衡策略 (QMT Mini 版)

原始来源：聚宽 https://www.joinquant.com/post/50559
作者：开心果 / QMT 移植：本项目

策略逻辑：
  固定权重配置国债/黄金/红利/纳指四只 ETF，每日 09:35 检查持仓偏差。
  当某只 ETF 的实际市值偏离目标权重超过 rebalance_threshold（默认 15%）
  且偏差份数超过 rebalance_min_shares（默认 100 份）时，触发再平衡下单。
  卖出先于买入，以释放资金给后续买入操作。
"""

import time
import datetime
from datetime import timezone, timedelta
import argparse
import sys
import os

# ── 路径初始化：将项目根目录加入 sys.path ──────────────────────────────────
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
import xtquant.xtconstant as xtconstant

from utils.utilities import StrategyLedger, MessagePusher
from utils.stockmgr import StockMgr
from utils.trademgr import TradeMgr

# ================= 1. 全局配置 =================

BEIJING_TZ = timezone(timedelta(hours=8))


class Config:
    account_id    = '47601131'                                    # 【必改】资金账号
    mini_qmt_path = r'D:\光大证券金阳光QMT实盘\userdata_mini'    # 【必改】极简模式路径

    # ── 策略资金预算 ──────────────────────────────────────────────
    # 本策略管理的总资金量（元）。调整此参数即可缩放整体仓位。
    total_capital: float = 100_000.0

    # ── 目标权重 ─────────────────────────────────────────────────
    # 代码格式：QMT 格式（.SH/.SZ）
    weights: dict = {
        '511010.SH': 0.70,   # 国债ETF  —— 底仓，提供稳定固息收益
        '518880.SH': 0.14,   # 黄金ETF  —— 对冲风险资产
        '510880.SH': 0.08,   # 红利ETF  —— A 股分红收益增强
        '513100.SH': 0.08,   # 纳指ETF  —— 全球权益弹性暴露
    }

    # ── 再平衡触发阈值 ────────────────────────────────────────────
    rebalance_threshold: float  = 0.15   # 实际市值偏离目标超过 15% 才触发
    rebalance_min_shares: int   = 100    # 偏差折算成份数不足 100 份时跳过（避免佣金损耗）

    # ── 交易成本参数（用于估算可用资金，避免资金不足导致拒单）────
    slippage:        float = 0.002   # 单边滑点率（买/卖各约 0.2%）
    commission:      float = 0.0002  # 单边佣金率（双向，万分之二）
    min_commission:  float = 5.0     # 最低佣金（元）

    # ── 权益类 ETF 止损 ───────────────────────────────────────────
    # 仅对权益类 ETF 设止损；债券/黄金 ETF 不参与，让其正常持有。
    equity_etfs:      tuple = ('510880.SH', '513100.SH')
    stoploss_pct:     float = 0.12   # 持仓成本跌幅超过 12% 触发清仓


# ================= 2. 运行时全局变量 =================

class GlobalVar:
    # StrategyLedger：记录本策略买入的 ETF，防止与账户中手动持有的相同 ETF 混淆
    strategy_ledger = StrategyLedger('kj202590_holdings.json')


# ================= 3. 交易回调 =================

class MyCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        print("警告：交易服务器连接断开！")

    def on_stock_order(self, order):
        print(
            f"[订单] {order.stock_code} | 状态: {order.order_status_msg} "
            f"| 成交均价: {order.traded_price:.4f} | 成交量: {order.traded_volume}"
        )

    def on_stock_trade(self, trade):
        print(f"[成交] {trade.stock_code} | 数量: {trade.traded_volume} | 价格: {trade.traded_price:.4f}")


# ================= 4. 工具函数 =================

def get_latest_price(stock_code: str) -> float:
    """
    获取最新成交价，两级容错：
      1. get_full_tick  — 实时 tick，正常交易时间首选
      2. 日线收盘价     — tick 不可用时的 fallback
    两者均失败则返回 0.0，调用方需自行跳过该 ETF。
    """
    # ── 第一优先：实时 tick ───────────────────────────────────────
    try:
        tick = xtdata.get_full_tick([stock_code])
        if stock_code in tick:
            price = tick[stock_code].get('lastPrice', 0.0)
            if price > 0:
                return price
    except Exception as e:
        print(f"[警告] get_full_tick 失败 ({stock_code}): {e}")

    # ── Fallback：最近一根日线收盘价 ─────────────────────────────
    print(f"[警告] {stock_code} tick 价格无效，回落到日线收盘价...")
    try:
        df = xtdata.get_market_data_ex(['close'], [stock_code], period='1d', count=1)
        if stock_code in df and not df[stock_code].empty:
            price = float(df[stock_code]['close'].iloc[-1])
            if price > 0:
                print(f"[Fallback] {stock_code} 使用日线收盘价: {price:.4f}")
                return price
    except Exception as e:
        print(f"[错误] 日线收盘价获取失败 ({stock_code}): {e}")

    print(f"[错误] {stock_code} 无法获取任何有效价格，本轮跳过。")
    return 0.0


def get_strategy_total_value(trader: XtQuantTrader, account: StockAccount) -> float:
    """
    动态计算本策略实际管理的总资产：
      策略 ETF 持仓市值 + 账户可用现金
    结果以 Config.total_capital 为上限，防止策略规模超出预期。
    """
    positions = trader.query_stock_positions(account)
    asset     = trader.query_stock_asset(account)

    strategy_codes  = set(Config.weights.keys())
    holding_value   = sum(
        p.market_value for p in positions
        if p.stock_code in strategy_codes and p.volume > 0
    )
    cash            = asset.cash if asset else 0.0
    dynamic_total   = holding_value + cash

    # 只限制「新增现金的投入量」，不压缩已有持仓盈利
    # 这样策略 ETF 涨过 total_capital 后不会触发错误的卖出指令
    needed_cash = max(0.0, Config.total_capital - holding_value)
    usable_cash = min(cash, needed_cash)
    result      = holding_value + usable_cash
    print(f"[基准] 策略持仓市值: {holding_value:,.0f} | 可用现金: {cash:,.0f} "
          f"| 可投现金(上限{needed_cash:,.0f}): {usable_cash:,.0f} | 再平衡基准: {result:,.0f}")
    return result


def download_etf_data():
    """预下载策略关注的 ETF 历史行情（保证 QMT 内部 cache 最新）"""
    etf_list  = list(Config.weights.keys())
    start_str = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=5)).strftime('%Y%m%d')
    print(f"[行情] 下载 ETF 历史数据: {etf_list}")
    try:
        StockMgr.download_history(etf_list, start_time=start_str, period='1d')
    except Exception as e:
        print(f"[警告] 行情下载异常（非致命）: {e}")


def estimated_buy_cost(shares: int, price: float) -> float:
    """估算买入 shares 份 ETF 的实际资金占用（含滑点与佣金）"""
    raw_amount = shares * price
    commission = max(raw_amount * Config.commission, Config.min_commission)
    slippage   = raw_amount * Config.slippage
    return raw_amount + commission + slippage


def estimated_sell_proceeds(shares: int, price: float) -> float:
    """估算卖出 shares 份 ETF 的实际到手资金（含滑点与佣金）"""
    raw_amount = shares * price
    commission = max(raw_amount * Config.commission, Config.min_commission)
    slippage   = raw_amount * Config.slippage
    return raw_amount - commission - slippage


# ================= 5. 核心再平衡逻辑 =================

def check_equity_stoploss(trader: XtQuantTrader, account: StockAccount,
                          pos_map: dict) -> set:
    """
    检查权益类 ETF 是否触发止损线，触及则立即清仓。
    :param pos_map: {stock_code: position} 当前持仓 Map
    :return: 本轮已触发止损、需从再平衡中剔除的 ETF 代码集合
    """
    stopped = set()

    for stock in Config.equity_etfs:
        pos = pos_map.get(stock)
        if pos is None or pos.volume <= 0:
            continue   # 未持有，跳过

        cost          = pos.open_price          # 持仓均价（成本价）
        latest_price  = get_latest_price(stock)
        if cost <= 0 or latest_price <= 0:
            continue

        drawdown = (latest_price - cost) / cost  # 负值=亏损

        if drawdown <= -Config.stoploss_pct:
            print(
                f"  [止损] {stock} 成本: {cost:.4f} | 现价: {latest_price:.4f} "
                f"| 跌幅: {drawdown:.1%}  ≥ -{Config.stoploss_pct:.0%}，触发清仓！"
            )
            can_use = pos.can_use_volume
            if can_use > 0:
                if not DEBUG:
                    trader.order_stock(
                        account, stock,
                        xtconstant.STOCK_SELL, can_use,
                        xtconstant.LATEST_PRICE, latest_price,
                        'strategy', 'stoploss'
                    )
            else:
                print(f"  [止损] {stock} 可用份数为 0（T+1），今日无法卖出，下次再检查。"
                      f" 本轮仍将其排除出再平衡，防止继续补仓。")
            stopped.add(stock)
        else:
            print(f"  [止损检查] {stock} 跌幅 {drawdown:.1%}，未触发（阈值 -{Config.stoploss_pct:.0%}）")

    return stopped

def rebalance(trader: XtQuantTrader, account: StockAccount):
    """
    计算各 ETF 目标市值与实际市值的偏差，满足阈值条件时触发再平衡。
    执行顺序：止损检查 → 卖出超配（轮询等待成交）→ 买入欠配。
    """
    print("\n" + "=" * 50)
    print(f"[再平衡] 开始检查持仓偏差 @ {datetime.datetime.now(BEIJING_TZ).strftime('%H:%M:%S')}")

    # ── 获取持仓与资产 ────────────────────────────────────────────
    positions = trader.query_stock_positions(account)
    asset     = trader.query_stock_asset(account)

    if positions is None:
        print("[错误] 无法获取账户持仓（query_stock_positions 返回 None），跳过本次再平衡。")
        return
    if asset is None:
        print("[错误] 无法获取账户资产（query_stock_asset 返回 None），跳过本次再平衡。")
        return

    # ── 账本同步：清理已被外部卖出但仍在账本中的 ETF ──────────────
    hold_codes = {p.stock_code for p in positions if p.volume > 0}
    for code in list(GlobalVar.strategy_ledger.get_all()):
        if code not in hold_codes:
            print(f"[账本] {code} 已不在持仓中，从策略账本移除。")
            if not DEBUG:
                GlobalVar.strategy_ledger.remove(code)

    # ── 构建持仓 Map ──────────────────────────────────────────────
    pos_map = {p.stock_code: p for p in positions if p.volume > 0}

    # ── 动态计算本次再平衡的基准总资产 ───────────────────────────
    total_value = get_strategy_total_value(trader, account)

    # ── 止损检查（仅权益类 ETF）─────────────────────────────────
    print("\n[止损检查] 权益类 ETF 成本回撤检查...")
    stopped_etfs = check_equity_stoploss(trader, account, pos_map)

    # ── 计算各 ETF 偏差 ───────────────────────────────────────────
    print(f"\n{'ETF代码':<14} {'目标市值':>10} {'实际市值':>10} {'偏差':>10} {'偏差率':>8}")
    print("-" * 58)

    balances = {}
    for stock, weight in Config.weights.items():
        if stock in stopped_etfs:
            print(f"{stock:<14}  已触发止损，本轮跳过再平衡")
            continue
        target_value  = total_value * weight
        pos           = pos_map.get(stock)
        current_value = pos.market_value if pos else 0.0
        diff_value    = target_value - current_value   # 正=欠配(需买), 负=超配(需卖)
        dev_pct       = diff_value / target_value if target_value > 0 else 0.0

        print(f"{stock:<14} {target_value:>10,.0f} {current_value:>10,.0f} {diff_value:>+10,.0f} {dev_pct:>7.1%}")

        balances[stock] = {
            'target_value':  target_value,
            'current_value': current_value,
            'diff_value':    diff_value,
            'dev_pct':       dev_pct,
        }

    print()

    # ── 按偏差升序排列：负值（超配/需卖）排最前，确保卖出先执行 ────
    sorted_stocks = sorted(balances.items(), key=lambda x: x[1]['diff_value'])

    # ── 推送记录（实盘模式下收集，结束后统一发送）────────────────
    sell_log: list = []
    buy_log:  list = []

    # ── 第一轮：执行卖出 ──────────────────────────────────────────
    has_sell    = False
    sold_targets: dict = {}   # {stock: current_value}，用于轮询确认成交
    print("[再平衡] 第一轮：检查是否需要卖出（超配品种）")

    for stock, info in sorted_stocks:
        diff_value   = info['diff_value']
        target_value = info['target_value']

        if diff_value >= 0:
            continue   # 欠配品种在买入轮处理

        latest_price = get_latest_price(stock)
        if latest_price <= 0:
            print(f"  [{stock}] 无法获取价格，跳过。")
            continue

        diff_shares = abs(diff_value) / latest_price   # 需卖出的估算份数

        # ── 触发条件检查 ─────────────────────────────────────────
        if not (abs(diff_value) > target_value * Config.rebalance_threshold
                and diff_shares > Config.rebalance_min_shares):
            print(f"  [{stock}] 偏差 {info['dev_pct']:.1%}，未达阈值，跳过。")
            continue

        # 向下取整到 100 份整数倍
        sell_shares = int(diff_shares / 100) * 100
        if sell_shares <= 0:
            print(f"  [{stock}] 取整后份数为 0，跳过。")
            continue

        # 受限于 T+1 可用份数
        pos         = pos_map.get(stock)
        can_use     = pos.can_use_volume if pos else 0
        sell_shares = min(sell_shares, can_use)

        if sell_shares <= 0:
            print(f"  [{stock}] 可用份数为 0（T+1 锁仓或冻结），跳过。")
            continue

        proceeds = estimated_sell_proceeds(sell_shares, latest_price)
        print(
            f"  --> [卖出] {stock} | {sell_shares} 份 @ ~{latest_price:.4f} "
            f"| 预计到手: {proceeds:,.0f} 元 | 偏差: {info['dev_pct']:.1%}"
        )

        if not DEBUG:
            # 使用 LATEST_PRICE 类型，QMT 以最新价撮合
            trader.order_stock(
                account, stock,
                xtconstant.STOCK_SELL, sell_shares,
                xtconstant.LATEST_PRICE, latest_price,
                'strategy', 'rebalance_sell'
            )
            sell_log.append(f"{stock} {sell_shares}份 @{latest_price:.4f} 预计到手{proceeds:,.0f}元")
            sold_targets[stock] = info['current_value']
        has_sell = True

    if has_sell:
        print("\n[再平衡] 已发送卖出指令，轮询等待成交确认...")
        if not DEBUG:
            TradeMgr.wait_for_sells(trader, account, sold_targets, timeout=120, interval=5)

    # ── 第二轮：执行买入 ──────────────────────────────────────────
    # 重新查询可用资金（卖出可能已到账）
    if not DEBUG:
        asset = trader.query_stock_asset(account)
    available_cash = asset.cash if asset else 0.0

    print(f"\n[再平衡] 第二轮：检查是否需要买入（欠配品种），可用资金: {available_cash:,.0f} 元")

    for stock, info in sorted_stocks:
        diff_value   = info['diff_value']
        target_value = info['target_value']

        if diff_value <= 0:
            continue   # 超配品种已在卖出轮处理

        latest_price = get_latest_price(stock)
        if latest_price <= 0:
            print(f"  [{stock}] 无法获取价格，跳过。")
            continue

        diff_shares = diff_value / latest_price

        # ── 触发条件检查 ─────────────────────────────────────────
        if not (abs(diff_value) > target_value * Config.rebalance_threshold
                and diff_shares > Config.rebalance_min_shares):
            print(f"  [{stock}] 偏差 {info['dev_pct']:.1%}，未达阈值，跳过。")
            continue

        # 考虑成本因子，计算实际能买到的最大份数
        cost_factor  = 1 + Config.slippage + Config.commission
        affordable_value = available_cash / cost_factor
        buy_value    = min(diff_value, affordable_value)
        buy_shares   = int(buy_value / latest_price / 100) * 100

        if buy_shares <= 0:
            print(f"  [{stock}] 资金不足或取整后为 0，跳过。偏差: {info['dev_pct']:.1%}")
            continue

        actual_cost = estimated_buy_cost(buy_shares, latest_price)
        if actual_cost > available_cash:
            # 精确校验：再砍一档（减 100 份）
            buy_shares -= 100
            if buy_shares <= 0:
                print(f"  [{stock}] 买入份数修正后仍不足，跳过。")
                continue
            actual_cost = estimated_buy_cost(buy_shares, latest_price)

        print(
            f"  --> [买入] {stock} | {buy_shares} 份 @ ~{latest_price:.4f} "
            f"| 预计花费: {actual_cost:,.0f} 元 | 偏差: {info['dev_pct']:.1%}"
        )

        if not DEBUG:
            seq = trader.order_stock(
                account, stock,
                xtconstant.STOCK_BUY, buy_shares,
                xtconstant.LATEST_PRICE, latest_price,
                'strategy', 'rebalance_buy'
            )
            if seq != -1:
                GlobalVar.strategy_ledger.add(stock)
                available_cash -= actual_cost
                buy_log.append(f"{stock} {buy_shares}份 @{latest_price:.4f} 预计花费{actual_cost:,.0f}元")
            else:
                print(f"  [拒单] {stock} 买入报单失败，跳过（资金不足或涨停）。")
        else:
            # DEBUG 模式：只做日志，不修改账本，不扣减 available_cash
            pass

    # ── 推送再平衡报告（实盘有实际下单时才推送）────────────────────
    if not DEBUG and (sell_log or buy_log):
        MessagePusher().send_strategy_report(
            strategy_name='固收+ ETF再平衡 (kj202590)',
            buys=buy_log,
            sells=sell_log,
            extra_msg=f"基准总值: {total_value:,.0f} 元"
        )

    print("\n[再平衡] 本次检查完成。")
    print("=" * 50)


# ================= 6. 策略主循环 =================

def is_trading_day() -> bool:
    """判断今天是否是 A 股交易日（查 QMT 上交所日历）"""
    today = datetime.datetime.now(BEIJING_TZ).strftime('%Y%m%d')
    try:
        days = xtdata.get_trading_dates('SH', today, today)
        return len(days) > 0
    except Exception as e:
        print(f"[警告] 交易日查询失败: {e}，默认视为交易日继续运行。")
        return True


def run_strategy():
    """初始化 QMT 交易接口并进入每日定时任务循环"""

    # ── 初始化交易接口 ────────────────────────────────────────────
    session_id = int(time.time())
    trader     = XtQuantTrader(Config.mini_qmt_path, session_id)
    account    = StockAccount(Config.account_id)

    trader.register_callback(MyCallback())
    trader.start()
    trader.connect()
    trader.subscribe(account)
    print("====== QMT 交易接口连接成功，固收+ 策略启动 ======")
    print(f"  账号: {Config.account_id}")
    print(f"  总资金预算: {Config.total_capital:,.0f} 元")
    print(f"  目标配置: { {k: f'{v:.0%}' for k, v in Config.weights.items()} }")
    print(f"  再平衡阈值: {Config.rebalance_threshold:.0%} / 最小份数: {Config.rebalance_min_shares}")
    print(f"  DEBUG 模式: {DEBUG}\n")

    # ── 预下载行情 ────────────────────────────────────────────────
    download_etf_data()

    # ── 每日任务执行标记 ─────────────────────────────────────────
    task_done = {'09:35': False}

    while True:
        now      = datetime.datetime.now(BEIJING_TZ)
        time_str = now.strftime('%H:%M')

        # 午夜重置任务标记
        if time_str == '00:00':
            for k in task_done:
                task_done[k] = False
            time.sleep(1)

        # ── 09:35 每日再平衡 ──────────────────────────────────────
        # 选在开盘 5 分钟后执行，避开集合竞价的价格异常波动
        if DEBUG or (time_str == '09:35' and not task_done['09:35'] and is_trading_day()):
            print(f"\n[{time_str}] 触发每日再平衡任务...")
            download_etf_data()   # 保证行情最新
            rebalance(trader, account)
            task_done['09:35'] = True

        if DEBUG:
            break
        else:
            time.sleep(1)


# ================= 7. 入口 =================

DEBUG = True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='固收+ ETF 再平衡策略 (kj202590)')
    parser.add_argument('-m', '--mode', type=str, default='DEBUG',
                        help='运行模式: REAL（实盘）或 DEBUG（调试，仅输出日志）')
    args = parser.parse_args()

    if args.mode == 'REAL':
        print('>>> 当前处于 [REAL 实盘模式]：将真实下单，请确认参数后继续！')
        DEBUG = False
    else:
        print('>>> 当前处于 [DEBUG 调试模式]：仅输出日志，不触发真实报单。')
        DEBUG = True

    run_strategy()
