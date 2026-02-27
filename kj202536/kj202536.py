# -*- coding: utf-8 -*-
"""
策略名称：36号策略 - 全天候多资产轮动 (xtquant 极简模式)
核心目标：中期持股、风险可控、非赌博
适用环境：光大证券金阳光QMT极速版 (后台需启动)
资金账号：47601131
"""

import time
import datetime
import numpy as np
import pandas as pd
from scipy import stats
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader

class XtStockAccount:
    """
    手动定义账户类。
    针对报错：将 account_type 从 'STOCK' 改为整数 2
    """
    def __init__(self, account_id, account_type=2): # 这里改为 2
        self.account_id = account_id
        self.account_type = account_type

# ======================== 1. 策略配置 ========================
class Config:
    # --- 账号配置 ---
    acc_id = '47601131'
    qmt_path = r'D:\光大证券金阳光QMT实盘\userdata_mini' # 请根据实际安装路径修改
    session_id = int(time.time())
    
    # --- 资产池配置 (分组逻辑) ---
    etf_groups = {
        'Commodity': ['159985.SZ', '159981.SZ', '159980.SZ', '518880.SH'], # 豆粕, 能化, 有色, 黄金
        'Dividend':  ['510880.SH', '512890.SH'],                        # 红利, 红利低波
        'Core':      ['510150.SH', '159967.SZ', '588000.SH'],            # 上证50, 创蓝筹, 科创50
        'Global':    ['513100.SH', '513500.SH', '513030.SH'],             # 纳指, 标普, 德国30
        'Stock':     ['300274.SZ', '600050.SH']
    }
    bond_etf = '511010.SH'      # 避险：活跃国债ETF
    index_code = '000300.SH'    # 择时基准
    
    # --- 算法参数 ---
    rsrs_n = 18                 # RSRS回归窗口
    rsrs_m = 600                # 标准化基准天数
    buy_threshold = 0.7         # 看多阈值
    sell_threshold = -0.7       # 看空阈值
    rank_days = 20              # 动量回看期
    target_num = 3              # 同时持有ETF类别数
    
    # --- 交易设置 ---
    check_time = "09:35:00"     # 每日调仓检查时间

# ======================== 2. 核心算法逻辑 ========================

def get_rsrs_signal():
    """计算大盘RSRS择时信号"""
    print(f"正在计算 {Config.index_code} 的 RSRS 信号...")
    # 获取历史数据
    start_date = (datetime.datetime.now() - datetime.timedelta(days=Config.rsrs_m + Config.rsrs_n)).strftime("%Y%m%d")

    xtdata.download_history_data(Config.index_code, period='1d', start_time=start_date, end_time='')
    data = xtdata.get_market_data_ex(['high', 'low'], [Config.index_code], period='1d', count=Config.rsrs_m + Config.rsrs_n, dividend_type='front')[Config.index_code]
    
    highs = data['high'].values
    lows = data['low'].values
    
    slopes = []
    for i in range(len(highs) - Config.rsrs_n + 1):
        y = highs[i : i + Config.rsrs_n]
        x = lows[i : i + Config.rsrs_n]
        slope, _, _, _, _ = stats.linregress(x, y)
        slopes.append(slope)
    
    # 标准化
    current_slope = slopes[-1]
    history_slopes = slopes[:-1]
    z_score = (current_slope - np.mean(history_slopes)) / np.std(history_slopes)
    return z_score

def filter_audit_opinion(pool):
    """审计意见防火墙 (兼容股票)"""
    # 提取非ETF标的 (A股代码通常不以51, 15, 58开头)
    stocks = [s for s in pool if not (s.startswith('51') or s.startswith('15') or s.startswith('58'))]
    etfs = [s for s in pool if s not in stocks]
    
    if not stocks: return etfs
    
    # 查询最近一年审计意见
    report_date = f"{datetime.datetime.now().year - 1}1231"
    end_date = (datetime.datetime.now()).strftime("%Y%m%d")
    try:
        # xtdata 接口获取财务数据
        # 字段映射通常为 STK_AUDIT_OPINION.audit_opinion_type
        fin_data = xtdata.get_financial_data(
            stocks, 
            table_list=['PershareIndex'], 
            report_type='announce_time' # 避免未来函数
        )

        valid_stocks = []
        for s in stocks:
            try:
                fin_df = fin_data.get(s)['PershareIndex']
                if fin_df is not None and isinstance(fin_df, pd.DataFrame) and not fin_df.empty:
                    last_report = fin_df.iloc[-1]
                    # s_fa_eps_basic: 基本每股收益
                    # s_fa_bps:       每股净资产 (备用)
                    # 'adjusted_earnings_per_share', # 扣非每股收益
                    # 's_fa_ocfps',                  # 每股经营现金流
                    # 's_fa_bps',                    # 每股净资产
                    # 's_fa_undistributedps',        # 每股未分配利润
                    # 'gear_ratio'                   # 资产负债率
                    # adjusted_earnings_per_share > 0 (扣非每股收益为正)
                    eps = last_report.get('s_fa_eps_basic', -1)
                    bps = last_report.get('s_fa_bps', -1) 
                    aes = last_report.get('adjusted_earnings_per_share', -1) 
                    cfs = last_report.get('s_fa_ocfps', -1) 
                    undist = last_report.get('s_fa_undistributedps', -1) 
                    gratio = last_report.get('gear_ratio', -1) 

                    if pd.isna(aes):
                        aes = eps

                    if bps > 0 and aes > 0 and (eps > 0 or  cfs > 0 or undist > 0 or gratio < 0.85): 
                        valid_stocks.append(s)
            except: continue
        return etfs + valid_stocks
    except:
        return etfs

def get_momentum_score(code):
    """计算动量平稳度评分: Return * R2"""
    xtdata.download_history_data(code, period='1d', count=Config.rank_days)
    data = xtdata.get_market_data_ex(['close'], [code], period='1d', count=Config.rank_days)[code]
    prices = data['close'].values
    if len(prices) < Config.rank_days: return -999
    
    y = np.log(prices)
    x = np.arange(len(y))
    slope, _, r_val, _, _ = stats.linregress(x, y)
    return (np.exp(slope * 250) - 1) * (r_val ** 2)

# ======================== 3. 交易执行引擎 ========================

class RobotTrader:
    def __init__(self):
        self.trader = XtQuantTrader(Config.qmt_path, Config.session_id)
        self.acc = XtStockAccount(Config.acc_id)
        
    def connect(self):
        self.trader.start()
        res = self.trader.connect()
        if res == 0:
            print(f">>> 已连接金阳光终端 | 账号: {self.acc}")
            self.trader.subscribe(self.acc)
            return True
        else:
            print(">>> 连接失败，请检查QMT是否开启极简模式交易！")
            return False

    def execute_logic(self):
        print(f"\n--- 触发例行检查: {datetime.datetime.now()} ---")
        
        # 1. 计算择时
        z = get_rsrs_signal()
        print(f"当前 RSRS Z-Score: {z:.2f}")

        # 2. 选股与过滤
        full_pool = [item for sublist in Config.etf_groups.values() for item in sublist]
        safe_pool = filter_audit_opinion(full_pool)
        
        target_list = []
        if z > Config.buy_threshold:
            # 进攻模式
            scores = []
            for code in safe_pool:
                s = get_momentum_score(code)
                if s > 0: scores.append({'code': code, 'score': s})
            
            if scores:
                df = pd.DataFrame(scores).sort_values('score', ascending=False)
                used_grp = set()
                for _, row in df.iterrows():
                    code = row['code']
                    grp = next((k for k, v in Config.etf_groups.items() if code in v), 'Other')
                    if grp not in used_grp:
                        target_list.append(code)
                        used_grp.add(grp)
                    if len(target_list) >= Config.target_num: break
            
            if not target_list: target_list = [Config.bond_etf]
            print(f"信号: 进攻 | 目标: {target_list}")

        elif z < Config.sell_threshold:
            # 防御模式
            target_list = [Config.bond_etf]
            print(f"信号: 防御 | 撤离至避险资产")
        else:
            # 维持现有持仓 (不调仓)
            print(f"信号: 震荡 | 维持现状，跳过下单")
            return

        # 3. 报单执行
        self.sync_orders(target_list)

    def sync_orders(self, target_list):
        # 获取当前持仓
        positions = self.trader.query_stock_positions(self.acc)
        current_holdings = {p.stock_code: p.volume for p in positions if p.volume > 0}
        
        # 获取资产总额
        asset = self.trader.query_stock_asset(self.acc)
        total_asset = asset.total_asset
        
        # A. 卖出不再目标的标的
        for code in current_holdings.keys():
            if code not in target_list:
                print(f"【卖出】{code} | 逻辑: 不在目标列表")
                #self.trader.order_stock(self.acc, code, 24, 0, 11, -1, "36_Strategy_Sell")

        # B. 买入目标 (按总资产比例)
        weight = 1.0 / len(target_list)
        for code in target_list:
            target_value = total_asset * weight
            print(f"【调整】{code} | 目标价值: {target_value:.2f}")
            # order_value 会根据目标价值自动计算所需买入/卖出量
            #self.trader.order_value(self.acc, code, 23, target_value, 11, -1, "36_Strategy_Rebalance")

    def loop(self):
        print(">>> 交易机器人已启动，等待定时任务...")
        while True:
            now_str = datetime.datetime.now().strftime("%H:%M:%S")
            if True: #now_str == Config.check_time:
                try:
                    self.execute_logic()
                except Exception as e:
                    print(f"运行时发生错误: {e}")
                time.sleep(2) # 避开同一秒多次触发
            time.sleep(1)

if __name__ == "__main__":
    bot = RobotTrader()
    if bot.connect():
        bot.loop()