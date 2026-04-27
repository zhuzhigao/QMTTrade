__all__ = ['StockInfo', 'StockMgr']

from datetime import time
from dataclasses import dataclass
from typing import Optional
import pandas as pd
from xtquant import xtdata


@dataclass
class StockInfo:
    """单只股票的基本面快照，纯数据容器"""
    stock_code: str
    stock_name: Optional[str] = None      # 股票名称
    roe: Optional[float] = None           # 加权净资产收益率 (%) — PershareIndex.equity_roe
    pe_ttm: Optional[float] = None        # 市盈率 TTM           — 当前价 / EPS
    eps: Optional[float] = None           # 每股收益             — PershareIndex.s_fa_eps_basic
    market_cap: Optional[float] = None    # 总市值 (元)          — 当前价 × Capital.m_nTotalShares
    dedu_np: Optional[float] = None       # 扣非净利润 (元)      — Income.net_profit_incl_min_int_inc_after

    def is_valid(self) -> bool:
        """所有核心字段均有值才视为有效"""
        return all(v is not None for v in (self.roe, self.pe_ttm, self.eps, self.market_cap, self.dedu_np))


class StockMgr:
    """从 QMT 数据源查询并构造 StockInfo"""


    @staticmethod
    def query_stock(stock: str) -> Optional[StockInfo]:
        """查询单只股票的基本面快照，失败返回 None"""
        try:
            fin_data = xtdata.get_financial_data([stock], table_list=['PershareIndex', 'Income', 'Capital'], start_time='20250930', report_type='announce_time')
            pershare = fin_data.get(stock)['PershareIndex']
            income   = fin_data.get(stock)['Income']
            #capital  = fin_data.get(stock)['Capital']
            detail = xtdata.get_instrument_detail(stock)
            if pershare is None or income is None:
                return None

            last_pershare_report = pershare.iloc[-1]
            last_income_report   = income.iloc[-1]
            #last_capital_report   = capital.iloc[-1]

            eps = last_pershare_report.get('s_fa_eps_basic', -9999)
            if pd.isna(eps):
                eps = 0

            tick = xtdata.get_full_tick([stock])
            current_price = tick[stock].get('lastPrice', -9999)

            pe = (current_price / eps) if eps != 0 else -9999
            #total_shares = last_capital_report.get('total_capital', -9999)
            total_shares = detail.get('TotalVolume', -9999)
            if total_shares != -9999:
                marketcap = current_price * total_shares
            else:
                marketcap = -9999
            dedunp = last_income_report.get('net_profit_incl_min_int_inc_after', -9999)

            return StockInfo(
                stock_code = stock,
                stock_name = detail.get('InstrumentName', '未知') ,
                roe        = last_pershare_report.get('equity_roe', -9999),
                pe_ttm     = pe,
                eps        = eps,
                market_cap = marketcap,
                dedu_np    = dedunp,
            )
        except Exception as e:
            print(f"错误: {e}")
            return None
    
    @staticmethod
    def download_history(codes: list, start_time: str, end_time: str = '',
                         period: str = '1d', pause=False, showprogress=False) -> None:
        """逐只下载指定周期的历史数据"""
        total = len(codes)
        for i, code in enumerate(codes, 1):
            if showprogress:
                print(f"[{i}/{total}] 下载 {code} {period} {start_time}~{end_time or 'now'} ...", end=' ', flush=True)
            callback = (lambda res: print(f"进度: {res}")) if showprogress else None
            xtdata.download_history_data(code, period=period, start_time=start_time, end_time=end_time, callback=callback)
            if showprogress:
                print("完成")
            if pause:
                time.sleep(1)

    @staticmethod
    def query_stocks_in_sector(sector) -> list:
        weights = xtdata.get_index_weight(sector)
        if not weights:
            xtdata.download_index_weight()
            weights = xtdata.get_index_weight(sector)
        return list(weights.keys()) if weights else []
    
    