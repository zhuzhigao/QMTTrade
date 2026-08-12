# -*- coding: utf-8 -*-
"""
策略名称：36号策略 - 全天候多资产轮动 (xtquant 极简模式)
核心目标：中期持股、风险可控、非赌博
适用环境：光大证券金阳光QMT极速版 (后台需启动)
资金账号：47601131
"""
"运行日：每天（主动交易日是周一，其他日子被动保护）"

import time
import datetime
import os, sys
import argparse
from datetime import timezone, timedelta
import numpy as np
import pandas as pd
from scipy import stats
from xtquant import xtdata,xtconstant
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback

# 获取当前脚本的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取当前脚本的上一级目录（即 QMTTrade 根目录）
parent_dir = os.path.dirname(current_dir)

# 如果根目录不在搜索路径里，就把它加进去
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.utilities import MessagePusher, StrategyVolumeLedger, SingleInstanceLock
from utils.marketmgr import MarketMgr
from utils.stockmgr import StockMgr

BEIJING_TZ = timezone(timedelta(hours=8))
DEBUG = False

STRATEGY_BUY_TAG = "36_Strategy_Buy"
STRATEGY_SELL_TAG = "36_Strategy_Sell"

class XtStockAccount:
    """
    手动定义账户类。
    针对报错：将 account_type 从 'STOCK' 改为整数 2
    """
    def __init__(self, account_id, account_type=2): # 这里改为 2
        self.account_id = account_id
        self.account_type = account_type


class Strategy36Callback(XtQuantTraderCallback):
    """只根据 36 号订单的实际成交回报更新独立份数账本。"""

    def __init__(self, ledger):
        super().__init__()
        self.ledger = ledger

    def on_disconnected(self):
        print(">>> 警告：与 QMT 交易服务器连接断开")

    def on_stock_trade(self, trade):
        strategy_name = getattr(trade, 'strategy_name', '')
        stock_code = getattr(trade, 'stock_code', '')
        traded_volume = int(getattr(trade, 'traded_volume', 0) or 0)

        if not stock_code or traded_volume <= 0:
            return
        if strategy_name == STRATEGY_BUY_TAG:
            self.ledger.record_buy(stock_code, traded_volume)
            print(f">>> [36号账本] 买入成交 {stock_code} +{traded_volume}，策略持仓 {self.ledger.get(stock_code)}")
        elif strategy_name == STRATEGY_SELL_TAG:
            self.ledger.record_sell(stock_code, traded_volume)
            print(f">>> [36号账本] 卖出成交 {stock_code} -{traded_volume}，策略持仓 {self.ledger.get(stock_code)}")

# ======================== 1. 策略配置 ========================
class Config:
    # --- 账号配置 ---
    acc_id = '47601131'
    qmt_path = r'D:\光大证券金阳光QMT实盘\userdata_mini' # 请根据实际安装路径修改
    session_id = int(time.time())
    
    # --- 资产池配置 (分组逻辑) ---
    # etf_groups = {
    #     'Commodity': {
    #         '159985.SZ': '豆粕ETF', 
    #         '159981.SZ': '能化ETF', 
    #         '159980.SZ': '有色ETF', 
    #         '518880.SH': '黄金ETF'
    #     },
    #     'Dividend':  {
    #         '510880.SH': '红利ETF', 
    #         '512890.SH': '红利低波ETF'
    #     },
    #     'Core':      {
    #         '510050.SH': '上证50',
    #         '159949.SZ': '创蓝筹',
    #         '588000.SH': '科创50'
    #     },
    #     'Global':    {
    #         '513100.SH': '纳指ETF', 
    #         '513500.SH': '标普500', 
    #         '513030.SH': '德国30'
    #     }
    # }

    # etf_groups = {
    #     'Commodity': {
    #         '518880.SH': '黄金ETF', 
    #         '159980.SZ': '有色ETF', 
    #         '159981.SZ': '能化ETF', 
    #         '159985.SZ': '豆粕ETF'
    #     },
    #     'Dividend_Value':  { # 红利与价值
    #         '512890.SH': '红利低波ETF',
    #         '561560.SH': '电力ETF',
    #         '511260.SH': '十年国债ETF' # 也可以把长债放入此处作为防御轮动
    #     },
    #     'Core_Growth': { # 境内核心宽基
    #         '510050.SH': '上证50',
    #         '588000.SH': '科创50', 
    #         '159845.SZ': '中证1000', # 捕捉小盘行情
    #         '159949.SZ': '创蓝筹'
    #     },
    #     'Global_Market': { # 全球配置
    #         '513100.SH': '纳指ETF', 
    #         '513520.SH': '日经225',
    #         '513180.SH': '恒生科技'  # 新增：港股科技
    #     },
    #     'Sector_Alpha': { # 高贝塔行业主题 (新增组)
    #         '512480.SH': '半导体ETF',
    #         '159892.SZ': '恒生医疗', 
    #         '512660.SH': '军工ETF'
    #     }
    # }

    #--- 资产池配置 (已剔除高估值AI及半导体板块) ---
    etf_groups = {
        'Commodity': {
            '518880.SH': '黄金ETF',     # 抗通胀与地缘避险
            '159980.SZ': '有色ETF',     # 实物资产动量
            '159981.SZ': '能化ETF',     # 保留能化作为大宗商品趋势标的
            '159985.SZ': '豆粕ETF'      # 农产品低相关度标的
        },
        'Dividend_Value':  {
            '512890.SH': '红低波ETF',   # 红利低波防守
            '515080.SH': '中证红利ETF', # 分散高股息行业，规避单一股票抱团
            '561560.SH': '电力ETF',     # 高分红公用事业防守
            '511260.SH': '十年国债ETF'  # 固收防御
        },
        'Core_Growth': { 
            '510050.SH': '上证50',     # 超大盘价值蓝筹
            '510300.SH': '沪深300',    # 【优化】替代科创50。A股核心资产，极度稳健，兼顾各行业龙头
            '159845.SZ': '中证1000',    # 小盘宽基（用于捕捉风格切换）
            '159949.SZ': '创蓝筹'       # 华安创业板50ETF
        },
        'Global_Market': { 
            '513500.SH': '标普500',    # 替代纳指，保留美股科技龙头同时大幅稀释AI权重
            '513520.SH': '日经225'      # 已删除恒生科技，不与境外持仓重复
        },
        'Sector_Alpha': { 
            '515260.SH': '电子ETF',     # 替代纯半导体，泛科技化以平滑芯片周期波动
            '561560.SH': '恒生医疗',
            '512660.SH': '军工ETF',
        }
    }

    bond_etf = '511010.SH'
    bond_name = '国债ETF' # 避险资产的名字也存一下

    # 自动生成一个扁平化的代码列表（用于获取数据）
    all_symbols = [code for group in etf_groups.values() for code in group.keys()] + [bond_etf]
    
    # 自动生成一个扁平化的名称映射表（用于下单备注查询）
    symbol_to_name = {code: name for group_dict in etf_groups.values() for code, name in group_dict.items()}
    symbol_to_name[bond_etf] = bond_name

    index_code = '000300.SH'    # 择时基准
    
    # --- 算法参数 ---
    rsrs_n = 18                 # RSRS回归窗口
    rsrs_m = 600                # 标准化基准天数
    buy_threshold = 0.5         # 看多阈值
    sell_threshold = -0.5       # 看空阈值
    rank_days = 20              # 动量回看期
    target_num = 3              # 同时持有ETF类别数
    
    # --- 交易设置 ---
    check_time = "14:50:00"     # 每日调仓检查时间

    policy_asset = 60000



# ======================== 2. 核心算法逻辑 ========================

def filter_audit_opinion(pool):
    """审计意见防火墙 (兼容股票)"""
    # 提取非ETF标的 (A股代码通常不以51, 15, 58开头)
    stocks = [s for s in pool if not (s.startswith('51') or s.startswith('15') or s.startswith('58') or s.startswith('56'))]
    etfs = [s for s in pool if s not in stocks]
    
    if not stocks: return etfs
    
    # 查询最近一年审计意见
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
    """动量质量评分 = (年化收益率 / 年化波动率) × R²（平滑夏普比率）"""
    data = xtdata.get_market_data_ex(['close'], [code], period='1d', count=Config.rank_days)[code]
    prices = data['close'].values
    if len(prices) < Config.rank_days: return -999

    y = np.log(prices)
    x = np.arange(len(y))
    slope, _, r_val, _, _ = stats.linregress(x, y)
    annualized_return = np.exp(slope * 250) - 1
    annualized_vol = np.diff(y).std() * np.sqrt(250) + 1e-9
    return float((annualized_return / annualized_vol) * (r_val ** 2))

# ======================== 3. 交易执行引擎 ========================

class RobotTrader:
    def __init__(self, ledger=None):
        self.trader = XtQuantTrader(Config.qmt_path, Config.session_id)
        self.acc = XtStockAccount(Config.acc_id)
        self.pusher = MessagePusher()
        self.ledger = ledger or StrategyVolumeLedger(os.path.join(current_dir, 'kj202536_holdings.json'))
        self.trader.register_callback(Strategy36Callback(self.ledger))
        
    def connect(self):
        self.trader.start()
        res = self.trader.connect()
        if res == 0:
            print(f">>> 已连接金阳光终端 | 账号: {self.acc}")
            self.trader.subscribe(self.acc)
            print(f">>> 36号独立持仓账本: {self.ledger.get_all()}")
            return True
        else:
            print(">>> 连接失败，请检查QMT是否开启极简模式交易！")
            return False

    def execute_logic(self):
        bj_now = datetime.datetime.now(BEIJING_TZ)
        print(f"\n--- 触发例行检查 (北京时间): {bj_now.strftime('%Y-%m-%d %H:%M:%S')} ---")
       
       # 判断今天是不是周一 (0代表周一)
        is_monday = (bj_now.weekday() == 0)

        # 1. 计算择时
        z = MarketMgr.get_rsrs_signal(Config.index_code, Config.rsrs_n, Config.rsrs_m)
        print(f"当前 RSRS Z-Score: {z:.2f}")

        # 2. 选股与过滤
        full_pool = [item for sublist in Config.etf_groups.values() for item in sublist]
        safe_pool = filter_audit_opinion(full_pool)
        
        target_list = []
        if z > Config.buy_threshold:
            # 进攻模式
            if not is_monday and not DEBUG:
                print(f"信号: 进攻 | 今日非周一调仓日，动量不变，保持当前持仓装死。")
                return # 不是周一，直接退出函数，不进行换仓
            
            
            # 逐只下载ETF历史数据
            start_date = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=Config.rsrs_m + Config.rsrs_n)).strftime("%Y%m%d")
            today_str_dl = datetime.datetime.now(BEIJING_TZ).strftime("%Y%m%d")
            StockMgr.download_history(safe_pool, start_time=start_date, period='1d', showprogress=True)
            StockMgr.download_history(safe_pool, start_time=today_str_dl, period='1m', showprogress=True)

            scores = []

            print("\n>>> 资产池动量评分明细表:")
            print(f"{'代码':<10} | {'名称':<12} | {'动量得分':<10}")
            
            print("-" * 40)
            for code in safe_pool:
                s = get_momentum_score(code)
                name = Config.symbol_to_name.get(code, "未知")
                print(f"{code:<10} | {name:<12} | {s:10.4f}")

                if s > 0: scores.append({'code': code, 'score': s})
            print("-" * 40)

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
        positions = self.trader.query_stock_positions(self.acc)
        if positions is None:
            print(">>> 无法查询账户持仓，为保护手工及其他策略仓位，本轮停止下单")
            return
        account_holdings = {
            p.stock_code: p
            for p in positions
            if p.volume > 0
        }
        strategy_holdings = self.ledger.get_all()

        buy_records = []
        sell_records = []
        today_str = datetime.datetime.now(BEIJING_TZ).strftime("%Y%m%d")

        # 每个仓位的目标金额，驱动全量再平衡
        target_value_per_slot = Config.policy_asset / len(target_list)
        rebalance_tolerance = 0.05  # 偏差在 5% 以内不触发微调

        print(f"\n本策略分配额度: {Config.policy_asset:.2f} | 目标持仓数: {len(target_list)} | 每仓目标金额: {target_value_per_slot:.2f}")

        # ===== A. 卖出（调出目标池 + 超配减仓）=====
        full_sell_codes = set()  # 用于轮询等待全仓清零
        for code, owned_volume in strategy_holdings.items():
            sell_vol = 0
            reason = ""
            try:
                pos = account_holdings.get(code)
                if pos is None:
                    print(f"  !! {code} 账本持有 {owned_volume} 股，但账户已无该持仓；为避免误卖，本轮跳过并保留账本待核对")
                    continue
                if pos.volume < owned_volume:
                    print(
                        f"  !! {code} 账本持有 {owned_volume} 股，但账户合计仅 {pos.volume} 股；"
                        "归属已不一致，为避免卖到手工或其他策略仓位，本轮禁止卖出"
                    )
                    continue
                tick = xtdata.get_full_tick([code])
                if code not in tick or tick[code]['lastPrice'] <= 0:
                    print(f"  -> 获取 {code} 最新价失败，跳过卖出检查")
                    continue
                current_price = tick[code]['lastPrice']

                if code not in target_list:
                    sell_vol = min(owned_volume, pos.can_use_volume)
                    reason = "调出目标池"
                else:
                    current_value = owned_volume * current_price
                    if current_value > target_value_per_slot * (1 + rebalance_tolerance):
                        excess_value = current_value - target_value_per_slot
                        sell_vol = min(
                            int(excess_value / current_price / 100) * 100,
                            owned_volume,
                            pos.can_use_volume
                        )
                        reason = f"超配减仓(市值:{current_value:.0f} 目标:{target_value_per_slot:.0f})"

                if sell_vol <= 0:
                    if reason:
                        print(f"  -> {code} {reason}，但可卖数量为0（今日买入），跳过")
                    continue

                name = Config.symbol_to_name.get(code, code)
                remark = f"Sell_{name}_{today_str}"
                print(f"【准备卖出】{code} | 单价: {current_price} | 数量: {sell_vol}股 | 逻辑: {reason}")
                if not DEBUG:
                    seq = self.trader.order_stock(self.acc, code, xtconstant.STOCK_SELL, sell_vol, xtconstant.FIX_PRICE, current_price, STRATEGY_SELL_TAG, remark)
                    if seq != -1:
                        sell_records.append(f"{name}({code}) | 数量: {sell_vol} | {reason}")
                        if code not in target_list:
                            full_sell_codes.add(code)
            except Exception as e:
                print(f"  -> {code} 卖出处理报错: {e}")

        # 等待 36 号账本中的目标持仓卖出成交（最多 120 秒）。
        # 不能等待账户持仓清零，因为同代码可能还属于手工或其他策略。
        if full_sell_codes and not DEBUG:
            deadline = datetime.datetime.now(BEIJING_TZ) + datetime.timedelta(seconds=120)
            print(f"  -> 等待全仓卖出成交: {full_sell_codes}")
            while datetime.datetime.now(BEIJING_TZ) < deadline:
                time.sleep(3)
                still_holding = {code for code in full_sell_codes if self.ledger.get(code) > 0}
                if not still_holding:
                    print(f"  -> 卖出已全部成交。")
                    break
                print(f"  -> 仍有持仓未清: {still_holding}，继续等待...")
            else:
                print(f"  !! 超时 120 秒仍有未成交卖单，继续执行买入（可用现金以实际为准）。")

        # ===== B. 买入（新标的 + 欠配补仓）=====
        # 买入计算只读取 36 号独立账本，不读取账户聚合持仓。
        strategy_holdings = self.ledger.get_all()
        asset = self.trader.query_stock_asset(self.acc)
        if asset is None:
            print(">>> 无法查询账户资产，为避免错误下单，本轮停止买入")
            return
        print(f"账户总资产: {asset.total_asset:.2f} | 可用现金: {asset.cash:.2f}")

        for code in target_list:
            try:
                tick = xtdata.get_full_tick([code])
                if code not in tick or tick[code]['lastPrice'] <= 0:
                    print(f"  -> 获取 {code} 最新价失败，跳过买入")
                    continue
                current_price = tick[code]['lastPrice']
                name = Config.symbol_to_name.get(code, code)

                owned_volume = strategy_holdings.get(code, 0)
                if owned_volume > 0:
                    current_value = owned_volume * current_price
                    if current_value < target_value_per_slot * (1 - rebalance_tolerance):
                        diff_value = target_value_per_slot - current_value
                        buy_vol = int(diff_value / current_price / 100) * 100
                        reason = f"欠配补仓(市值:{current_value:.0f} 目标:{target_value_per_slot:.0f})"
                    else:
                        print(f"  -> {code} 持仓均衡(市值:{current_value:.0f} ≈ 目标:{target_value_per_slot:.0f})，无需调整")
                        continue
                else:
                    buy_vol = int(target_value_per_slot / current_price / 100) * 100
                    reason = "新标的"

                remark = f"Buy_{name}_{today_str}"
                if buy_vol > 0:
                    print(f"【实际买入】{code} {name} | 单价: {current_price} | 数量: {buy_vol}股 | 逻辑: {reason}")
                    if not DEBUG:
                        seq = self.trader.order_stock(self.acc, code, xtconstant.STOCK_BUY, buy_vol, xtconstant.FIX_PRICE, current_price, STRATEGY_BUY_TAG, remark)
                        if seq != -1:
                            buy_records.append(f"{name}({code}) | 价格: {current_price} | 数量: {buy_vol} | {reason}")
                else:
                    print(f"  -> {code} 计算出的买入股数不足1手，无法下单")
            except Exception as e:
                print(f"  -> {code} 订单生成时报错: {e}")

        self.pusher.send_strategy_report("36号策略", buys=buy_records, sells=sell_records)

    def loop(self):
        print(f">>> 交易机器人已启动 (当前北京时间: {datetime.datetime.now(BEIJING_TZ).strftime('%H:%M:%S')})")

        z = MarketMgr.get_rsrs_signal(Config.index_code, Config.rsrs_n, Config.rsrs_m)
        print(f"当前 RSRS Z-Score: {z:.2f}")

        last_run_date = ""
        while True:
            now_dt = datetime.datetime.now(BEIJING_TZ)
            now_time = now_dt.strftime('%H:%M:%S')
            today = now_dt.strftime('%Y%m%d')
            if DEBUG or (now_time >= Config.check_time and last_run_date != today):
                try:
                    self.execute_logic()
                except Exception as e:
                    print(f"运行时发生错误: {e}")
                last_run_date = today
            if DEBUG:
                break
            time.sleep(1)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="金阳光 QMT 极简模式策略36 启动器")
    
    parser.add_argument('-m', '--mode', type=str, help='运行模式: REAL 或 DEBUG')
    parser.add_argument(
        '--init-ledger', metavar='POSITIONS',
        help='初始化份数账本并退出，例如 "518880.SH=1000,511010.SH=500"；空账本用 empty'
    )
    parser.add_argument(
        '--force-init-ledger', action='store_true',
        help='允许 --init-ledger 覆盖已有账本（请谨慎使用）'
    )
    
    # 3. 解析命令行参数
    args = parser.parse_args()
    
    # 4. 根据参数逻辑设置 DEBUG 状态
    if args.mode == 'REAL':
        print(">>> 当前处于 [REAL 实盘模式]：请注意风险！")
        DEBUG = False
    else:
        print(">>> 当前处于 [DEBUG 调试模式]：仅输出日志，不触发真实报单。")
        DEBUG = True
    ledger = StrategyVolumeLedger(os.path.join(current_dir, 'kj202536_holdings.json'))
    instance_lock = SingleInstanceLock(os.path.join(current_dir, 'kj202536.lock'))
    if not instance_lock.acquire():
        print('>>> 启动失败：36号策略已有一个实例正在运行。')
        sys.exit(2)

    try:
        if args.init_ledger is not None:
            try:
                positions = StrategyVolumeLedger.parse_positions(args.init_ledger)
                unknown_codes = set(positions) - set(Config.all_symbols)
                if unknown_codes:
                    raise ValueError(f"以下代码不在36号资产池中: {sorted(unknown_codes)}")
                ledger.initialize(positions, overwrite=args.force_init_ledger)
                print(f">>> 36号账本初始化完成: {ledger.filepath} | {ledger.get_all()}")
            except (ValueError, FileExistsError) as e:
                print(f">>> 36号账本初始化失败: {e}")
                sys.exit(2)
            sys.exit(0)

        if not DEBUG and not ledger.initialized:
            reason = f"账本损坏: {ledger.load_error}" if ledger.load_error else '账本尚未初始化'
            print(f">>> REAL 模式拒绝启动：{reason}。请先使用 --init-ledger 初始化。")
            sys.exit(2)

        bot = RobotTrader(ledger)
        if bot.connect():
            bot.loop()
    finally:
        instance_lock.release()
