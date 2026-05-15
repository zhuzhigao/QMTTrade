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
  python kj202512.py           # DEBUG 模式：完整跑逻辑，不发真实报单
  python kj202512.py -m REAL   # 实盘模式，谨慎！
"""

import os
import sys
import time
import argparse
import logging
import datetime
from datetime import timezone, timedelta
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)
# current_dir 加入 sys.path，让同目录的兄弟文件可以直接 import
# parent_dir  加入 sys.path，让 utils.* 可以正常引用
for _p in (current_dir, parent_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── 先解析命令行参数，让 DEBUG 在所有模块导入前就确定 ─────────────
# （避免日志初始化和子策略实例化时模式不明确）
_parser = argparse.ArgumentParser(description='kj202512 多策略分仓主入口', add_help=False)
_parser.add_argument('-m', '--mode', type=str, default='DEBUG',
                     help='运行模式: REAL 或 DEBUG（默认 DEBUG）')
_args, _ = _parser.parse_known_args()
DEBUG = (_args.mode.upper() != 'REAL')

# ── 日志：每条消息都带模式标签，明确区分调试/实盘 ────────────────
_MODE_TAG = '[调试]' if DEBUG else '[实盘]'

logging.basicConfig(
    level=logging.DEBUG,
    format=f'%(asctime)s {_MODE_TAG} [%(name)s] %(levelname)s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
LOG = logging.getLogger('kj202512-MAIN')

# ── 导入四个子策略类：直接按文件名 import，避免循环引用 ───────────
# 不使用 from kj202512.kj202512_etf import … 的原因：
#   kj202512.py 与 kj202512/ 目录同名，Python 会把当前文件误认成
#   kj202512 package，导致 "kj202512 is not a package" 循环引用错误。
from kj202512_etf  import ETFStrategy
from kj202512_pb   import PBStrategy
from kj202512_xsz  import XSZStrategy
from kj202512_dama import DaMaStrategy

BEIJING_TZ = timezone(timedelta(hours=8))


# ────────────────────────────────────────────────────
# QMT 回调
# ────────────────────────────────────────────────────

class MainCallback:
    def on_disconnected(self):
        LOG.error("!! 与 QMT 终端连接断开，请检查极简模式是否仍在运行 !!")

    def on_stock_order(self, order):
        LOG.info(f"[委托回报] {order.stock_code}  状态:{order.order_status}  价格:{order.price}")

    def on_stock_trade(self, trade):
        LOG.info(f"[成交回报] {trade.stock_code}  数量:{trade.traded_volume}  价格:{trade.traded_price}")


# ────────────────────────────────────────────────────
# 主策略编排器
# ────────────────────────────────────────────────────

class StrategyOrchestrator:
    """
    对应聚宽原版的 initialize()。
    持有 4 个子策略实例，每轮循环依次调用各子策略的 handlebar()。
    子策略内部自行判断时间节点是否触发，互不阻塞。
    """

    # DEBUG 模式下每隔 N 分钟打印一次"心跳"，确认系统仍在运行
    _HEARTBEAT_INTERVAL = 10 * 60   # 10 分钟（秒）

    def __init__(self, trader: XtQuantTrader, account, debug: bool):
        self.debug = debug
        self._last_heartbeat = 0.0

        border = '=' * 62
        LOG.info(border)
        LOG.info('  kj202512 多策略分仓系统')
        if debug:
            LOG.info('  ★ 当前模式：DEBUG（完整跑逻辑，不发真实报单）')
            LOG.info('    - 选股/过滤/排序 全部执行')
            LOG.info('    - 所有下单 API 调用被拦截')
            LOG.info('    - ledger / state 文件照常更新（模拟状态）')
        else:
            LOG.warning('  ★ 当前模式：REAL（实盘）—— 真实资金，谨慎操作！')
        LOG.info('  总预算: 80,000 元  ETF 16k | PB 16k | 小市值 24k | 大妈 24k')
        LOG.info(border)

        # 实例化 4 个子策略——共用同一个 trader / account，debug 统一传入
        self.strategies = [
            ETFStrategy (trader, account, debug),
            PBStrategy  (trader, account, debug),
            XSZStrategy (trader, account, debug),
            DaMaStrategy(trader, account, debug),
        ]

        names = [s.name for s in self.strategies]
        LOG.info(f"已加载 {len(self.strategies)} 个子策略: {names}")
        LOG.info("进入事件循环...")

    def handlebar(self):
        """每轮循环依次驱动各子策略，对应聚宽平台的定时调度"""
        # DEBUG 心跳：每 10 分钟提示一次当前仍处于调试模式
        if self.debug:
            now_ts = time.time()
            if now_ts - self._last_heartbeat >= self._HEARTBEAT_INTERVAL:
                LOG.debug("[心跳] DEBUG 模式运行中，所有报单均被拦截")
                self._last_heartbeat = now_ts

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
    # 重新用完整 parser（含 -h）输出帮助，_parser 只用于模块级提前解析
    parser = argparse.ArgumentParser(description='kj202512 多策略分仓主入口')
    parser.add_argument('-m', '--mode', type=str, default='DEBUG',
                        help='运行模式: REAL 或 DEBUG（默认 DEBUG）')
    args = parser.parse_args()
    # DEBUG 已在顶部由 _parser 确定，此处仅用于打印确认
    mode_str = 'DEBUG' if DEBUG else 'REAL'
    LOG.info(f"启动参数: -m {mode_str}")

    if not DEBUG:
        # 实盘模式额外确认，防止误启动
        LOG.warning("=" * 62)
        LOG.warning("  警告：即将以【实盘模式】启动，将发送真实报单！")
        LOG.warning("  如需调试，请使用默认 DEBUG 模式（不加 -m REAL）")
        LOG.warning("=" * 62)

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

    LOG.info("主事件循环已启动，按 Ctrl+C 退出")
    try:
        while True:
            orchestrator.handlebar()
            time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("收到停止信号，断开连接，退出程序...")
        trader.stop()
