__all__ = ['TradeMgr']

import time
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount


class TradeMgr:
    """交易执行辅助工具，封装与下单流程相关的通用逻辑"""

    @staticmethod
    def wait_for_sells(trader: XtQuantTrader, account: StockAccount,
                       sold_targets: dict, timeout: int = 120, interval: int = 5):
        """
        轮询持仓，直到所有卖出标的的市值已低于卖前水位（视为成交），或超时退出。

        :param trader:       XtQuantTrader 实例
        :param account:      StockAccount 实例
        :param sold_targets: {stock_code: pre_sell_market_value} 卖出前各标的市值
        :param timeout:      最长等待秒数（默认 120 秒）
        :param interval:     轮询间隔秒数（默认 5 秒）
        """
        deadline = time.time() + timeout
        pending  = set(sold_targets.keys())

        while pending and time.time() < deadline:
            time.sleep(interval)
            positions = trader.query_stock_positions(account)
            pos_map   = {p.stock_code: p for p in positions if p.volume > 0}

            confirmed = set()
            for code in pending:
                pre_value = sold_targets[code]
                cur_value = pos_map[code].market_value if code in pos_map else 0.0
                # 市值下降超过 5%，认为卖单已部分或全部成交
                if cur_value < pre_value * 0.95:
                    print(f"  [✓ 成交确认] {code} 市值: {pre_value:,.0f} → {cur_value:,.0f}")
                    confirmed.add(code)

            pending -= confirmed

        if pending:
            print(f"  [超时警告] 以下品种卖单在 {timeout}s 内未完全确认成交，继续执行买入: {pending}")
        else:
            print(f"  [轮询完成] 所有卖出均已确认成交。")
