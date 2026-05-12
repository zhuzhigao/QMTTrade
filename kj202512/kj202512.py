# -*- coding: utf-8 -*-
"""
kj202512.py — 多策略分仓主入口（对应原聚宽 initialize + 所有 run_daily/weekly/monthly）

架构说明：
  ONE QMT 连接 + ONE 事件循环，同时驱动 4 个子策略。
  子策略之间通过各自的 StrategyLedger 实现持仓隔离，互不干扰。
  对应原聚宽 set_subportfolios() 的隔离语义。

子策略与资金分配：
  ETF 轮动   (kj202512_etf.py)   16,000 元  20%  每日
  PB 低估值  (kj202512_pb.py)   16,000 元  20%  每月
  小市值     (kj202512_xsz.py)  24,000 元  30%  每周
  菜场大妈   (kj202512_dama.py) 24,000 元  30%  每月

运行方式：
  python kj202512.py           # DEBUG 模式，完整跑逻辑，不发真实报单
  python kj202512.py -m REAL   # 实盘模式，谨慎！
"""

import os
import sys
import time
import datetime
import argparse
import logging
from datetime import timezone, timedelta
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 导入四个子策略类
from kj202512.kj202512_etf  import ETFStrategy
from kj202512.kj202512_pb   import PBStrategy
from kj202512.kj202512_xsz  import XSZStrategy
from kj202512.kj202512_dama import DaMaStrategy

BEIJING_TZ = timezone(timedelta(hours=8))
DEBUG = True

# ── 日志 ─────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(name)s] %(levelname)s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
LOG = logging.getLogger('kj202512-MAIN')

# ────────────────────────────────────────────────────
# QMT 回调
# ────────────────────────────────────────────────────

class MainCallback:
    def on_disconnected(self):
        LOG.error("!! 与 QMT 终端连接断开，请检查极简模式是否仍在运行 !!")

    def on_stock_order(self, order):
        LOG.info(f"[委托] {order.stock_code}  状态:{order.order_status}  价格:{order.price}")

    def on_stock_trade(self, trade):
        LOG.info(f"[成交] {trade.stock_code}  数量:{trade.traded_volume}  价格:{trade.traded_price}")


# ────────────────────────────────────────────────────
# 主策略编排器
# ────────────────────────────────────────────────────

class StrategyOrchestrator:
    """
    对应聚宽原版的 initialize()。
    持有 4 个子策略实例，每轮循环依次调用各子策略的 handlebar()。
    子策略内部自行判断时间节点是否触发，互不阻塞。
    """

    def __init__(self, trader: XtQuantTrader, account, debug: bool):
        LOG.info("=" * 60)
        LOG.info("  kj202512 多策略分仓系统启动")
        LOG.info(f"  模式: {'[调试] 不发真实报单' if debug else '[实盘] 注意风险！'}")
        LOG.info(f"  总预算: 80,000 元（ETF 16k + PB 16k + 小市值 24k + 大妈 24k）")
        LOG.info("=" * 60)

        # 实例化 4 个子策略——共用同一个 trader / account
        self.strategies = [
            ETFStrategy (trader, account, debug),
            PBStrategy  (trader, account, debug),
            XSZStrategy (trader, account, debug),
            DaMaStrategy(trader, account, debug),
        ]

        LOG.info(f"已加载 {len(self.strategies)} 个子策略，进入事件循环...")

    def handlebar(self):
        """每轮循环依次驱动各子策略，对应聚宽平台的定时调度"""
        for strategy in self.strategies:
            try:
                strategy.handlebar()
            except Exception as e:
                # 单个子策略异常不影响其他子策略继续运行
                LOG.exception(f"[{strategy.name}] handlebar 异常，已跳过本轮: {e}")


# ────────────────────────────────────────────────────
# 主程序入口
# ────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='kj202512 多策略分仓主入口')
    parser.add_argument('-m', '--mode', type=str, default='DEBUG',
                        help='运行模式: REAL 或 DEBUG（默认 DEBUG）')
    args = parser.parse_args()

    if args.mode.upper() == 'REAL':
        LOG.info(">>> [实盘模式] 注意风险！")
        DEBUG = False
    else:
        LOG.info(">>> [调试模式] 完整跑逻辑，不发真实报单")

    # ── 请根据实际环境修改 ──────────────────────────
    qmt_path   = r'D:\光大证券金阳光QMT实盘\userdata_mini'
    account_id = '47601131'
    # ────────────────────────────────────────────────

    session_id = int(time.time())
    trader = XtQuantTrader(qmt_path, session_id)
    acc    = StockAccount(account_id)

    trader.register_callback(MainCallback())
    trader.start()

    if trader.connect() == 0:
        LOG.info(f"连接 QMT 成功，订阅账号 {account_id}")
        trader.subscribe(acc)
    else:
        LOG.error("连接 QMT 失败，请检查极简模式是否已启动并登录，以及 qmt_path 是否正确")
        sys.exit(1)

    orchestrator = StrategyOrchestrator(trader, acc, debug=DEBUG)

    LOG.info("进入主事件循环，按 Ctrl+C 退出...")
    try:
        while True:
            orchestrator.handlebar()
            time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("收到停止信号，断开连接，退出程序...")
        trader.stop()
