# coding=utf-8
import time
import json
import os
import datetime
import pandas as pd
import numpy as np
from xtquant import xttrader, xtconstant, xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

# ==================== 用户配置区域 ====================
# [核心开关] True=模拟模式(读CSV), False=实盘模式(读账户)
SIMULATION = False  

MINI_QMT_PATH = r'D:\光大证券金阳光QMT实盘\userdata_mini'
ACCOUNT_ID = '47601131'

# 文件路径配置
CSV_INPUT_POS = 'siminput.csv'       # 模拟：初始持仓 / 实盘：重点关注池
CSV_CURRENT_POS = 'simcurrent.csv'   # 模拟：当前持仓（动态更新）
LOG_FILE_REAL = 'tradelog.csv'       # 实盘：交易日志
LOG_FILE_SIM = 'simlog.csv'       # 模拟：交易日志

# 1. 资金风控
MAX_DAILY_BUY_AMOUNT = 100000.0   
SINGLE_STOCK_LIMIT_PCT = 0.30     

# 2. 止盈止损参数
HOLD_PROFIT_PCT = 0.20     
HOLD_LOSS_PCT = -0.15      
TRAILING_DRAWDOWN = 0.005  

# 3. 抄底参数
BUY_DIP_PCT = -0.06        
REBOUND_PCT = 0.005  # 右侧交易确认：从最低点反弹幅度 (0.5%)

# 4. ATR 动态参数
ATR_MULTIPLIER = 2.0       
ATR_PERIOD = 14

# 5. 市场风控
BENCHMARK_INDEX = '000001.SH'
BENCHMARK_RISK_THRESH = -0.025  

# 6. 系统参数
BUY_QUOTA = 20000 
LOOP_INTERVAL = 5
BJ_TZ = datetime.timezone(datetime.timedelta(hours=8))
# ====================================================

class PositionManager:
    """
    持仓管理器：负责抹平【实盘】与【模拟】的数据差异
    """
    def __init__(self, trader, account):
        self.trader = trader
        self.account = account
        self.sim_positions = {} # 格式: {stock: {'volume': 100, 'cost': 10.5}}
        
        if SIMULATION:
            self._init_sim_data()
            
    def load_input_csv_stocks(self):
        """仅读取 siminput.csv 中的股票代码，用于实盘监控"""
        stocks = set()
        if os.path.exists(CSV_INPUT_POS):
            try:
                df = pd.read_csv(CSV_INPUT_POS, encoding='utf-8-sig')
                if 'stock_code' in df.columns:
                    # 确保是字符串并去重
                    stocks = set(df['stock_code'].astype(str).dropna().tolist())
            except Exception as e:
                print(f"!!! 读取 {CSV_INPUT_POS} 失败: {e}")
        return list(stocks)
        
    def _init_sim_data(self):
        """模拟模式：从 input.csv 或 current.csv 加载持仓"""
        # 优先读取 current.csv (上次运行状态)，如果没有则读取 input.csv (初始状态)
        load_file = CSV_CURRENT_POS if os.path.exists(CSV_CURRENT_POS) else CSV_INPUT_POS
        
        if os.path.exists(load_file):
            try:
                df = pd.read_csv(load_file, encoding='utf-8-sig')
                # 确保列名存在
                if not df.empty and all(col in df.columns for col in ['stock_code', 'cost', 'volume']):
                    for _, row in df.iterrows():
                        self.sim_positions[row['stock_code']] = {
                            'volume': int(row['volume']),
                            'cost': float(row['cost'])
                        }
                print(f">>> [模拟] 已加载持仓文件: {load_file}, 共 {len(self.sim_positions)} 只股票")
                self.download_historical_data()
            except Exception as e:
                print(f"!!! [模拟] 读取持仓文件失败: {e}")
        else:
             print(f"!!! [模拟]没有找到持仓文件")

    def download_historical_data(self):
        # 获取当前的北京时间
        # 无论服务器在伦敦还是纽约，这个 time_now 永远是北京时间
        now_bj = datetime.datetime.now(BJ_TZ)

        # --- 计算日期 ---

        days_to_look_back = ATR_PERIOD * 2 

        # 使用北京时间计算 start 和 end
        start_date = (now_bj - datetime.timedelta(days=days_to_look_back)).strftime('%Y%m%d')
        end_date = now_bj.strftime('%Y%m%d')

        print(f"准备下载数据范围: {start_date} ~ {end_date}")
        
        codes = set(self.get_all_positions_codes())
        for stock_code in list(codes):
            xtdata.download_history_data(stock_code, period='1d', start_time=start_date, end_time=end_date)
        print(f"!!! 数据下载完成，共 {len(codes)} 只股票")

    def get_position(self, stock_code):
        """
        获取单只股票持仓信息
        返回: (volume, avg_cost, market_value)
        """
        if SIMULATION:
            if stock_code in self.sim_positions:
                pos = self.sim_positions[stock_code]
                vol = pos['volume']
                cost = pos['cost']
                # 获取当前价格计算市值
                tick = xtdata.get_full_tick([stock_code])
                curr_price = tick[stock_code]['lastPrice'] if (tick and stock_code in tick) else cost
                return vol, cost, vol * curr_price
            return 0, 0.0, 0.0
        else:
            # 实盘模式
            positions = self.trader.query_stock_positions(self.account)
            for p in positions:
                if p.stock_code == stock_code:
                    return p.volume, p.open_price, p.market_value
            return 0, 0.0, 0.0

    def get_all_positions_codes(self):
        """获取所有持仓股票代码列表"""
        if SIMULATION:
            return [k for k, v in self.sim_positions.items() if v['volume'] > 0]
        else:
            positions = self.trader.query_stock_positions(self.account)
            codes = [p.stock_code for p in positions if p.volume > 0]
            # [修改] 实盘模式下，额外监控 siminput.csv 中的股票
            return set(codes) | set(self.load_input_csv_stocks())

    def get_cash_and_asset(self):
        """获取可用资金和总资产"""
        if SIMULATION:
            # 模拟模式下，假设资金无限或固定，这里主要返回持仓市值
            total_mkt_value = 0.0
            for s, info in self.sim_positions.items():
                tick = xtdata.get_full_tick([s])
                price = tick[s]['lastPrice'] if (tick and s in tick) else info['cost']
                total_mkt_value += info['volume'] * price
            return 10000000.0, 10000000.0 + total_mkt_value # 模拟给个大额现金
        else:
            asset = self.trader.query_stock_asset(self.account)
            if asset:
                return asset.cash, asset.total_asset
            return 0.0, 0.0

    def update_sim_position(self, stock, action_type, trade_vol, trade_price):
        """模拟模式：更新内存持仓并写入 CSV"""
        if not SIMULATION: return

        if stock not in self.sim_positions:
            self.sim_positions[stock] = {'volume': 0, 'cost': 0.0}
        
        curr = self.sim_positions[stock]
        
        if action_type == xtconstant.STOCK_BUY:
            # 买入：计算加权平均成本
            new_cost = (curr['volume'] * curr['cost'] + trade_vol * trade_price) / (curr['volume'] + trade_vol)
            curr['volume'] += trade_vol
            curr['cost'] = new_cost
        elif action_type == xtconstant.STOCK_SELL:
            # 卖出：成本不变，数量减少
            curr['volume'] = max(0, curr['volume'] - trade_vol)
            if curr['volume'] == 0:
                curr['cost'] = 0.0

        # 清理持仓为0的
        if curr['volume'] == 0:
            del self.sim_positions[stock]
        
        # 保存到 current.csv
        data_list = []
        for s, info in self.sim_positions.items():
            data_list.append({'stock_code': s, 'cost': info['cost'], 'volume': info['volume']})
        
        df = pd.DataFrame(data_list)
        df.to_csv(CSV_CURRENT_POS, index=False, encoding='utf-8-sig')

class RobustStrategy:
    def __init__(self):
        import random
        session_id = int(random.randint(100000, 999999))
        self.trader = XtQuantTrader(MINI_QMT_PATH, session_id)
        self.acc = StockAccount(ACCOUNT_ID)
        
        # 初始化日期状态
        self.current_date_str = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d")
        self.state_file = f"sim_v2_state_{self.current_date_str}.json"
        
        # 数据加载
        self.data = self.load_state()
        self.atr_map = {} 
        self.high_pct_map = {} 
        
        # [新增] 持仓管理器 (在 start 中连接后初始化)
        self.pos_mgr = None

    def load_state(self):
        default_data = {"daily_buy_total": 0.0, "stocks": {}}
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    if "daily_buy_total" not in content: return default_data
                    return content
            except:
                return default_data
        return default_data

    def save_state(self):
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=4, ensure_ascii=False)
    
    def get_stock_state(self, stock_code):
        if stock_code not in self.data['stocks']:
            self.data['stocks'][stock_code] = {'bought': 0, 'sold': 0}
        return self.data['stocks'][stock_code]

    def update_stock_state(self, stock_code, action, money_amount=0.0):
        self.get_stock_state(stock_code) # Ensure exists
        if action == 'buy':
            self.data['stocks'][stock_code]['bought'] = 1
            self.data['daily_buy_total'] += money_amount
        elif action == 'sell':
            self.data['stocks'][stock_code]['sold'] = 1
        self.save_state()

    def check_date_rotation(self):
        now_date = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d")
        if now_date != self.current_date_str:
            print(f"\n>>> [日期轮转] 检测到新日期 {now_date}")
            self.current_date_str = now_date
            self.state_file = f"sim_v2_state_{self.current_date_str}.json"
            self.data = {"daily_buy_total": 0.0, "stocks": {}}
            self.high_pct_map = {}
            if os.path.exists(self.state_file):
                self.data = self.load_state()
            else:
                self.save_state()
            self.pos_mgr.download_historical_data()

    def log_trade_csv(self, stock, action_str, volume, price, cost, pnl):
        """统一记录交易日志到 CSV"""
        filename = LOG_FILE_SIM if SIMULATION else LOG_FILE_REAL
        time_str = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")
        
        # 如果文件不存在，写入表头
        if not os.path.exists(filename):
            with open(filename, 'w', encoding='utf-8-sig') as f:
                f.write("股票,时间,操作,股数,成交价,持仓成本,盈亏\n")
        
        line = f"{stock},{time_str},{action_str},{volume},{price:.2f},{cost:.2f},{pnl:.2f}\n"
        with open(filename, 'a', encoding='utf-8-sig') as f:
            f.write(line)

    def execute_trade(self, stock, action_type, volume, price, remark=""):
        """统一执行交易：处理实盘下单、模拟更新、日志记录"""
        
        # 1. 获取当前成本 (用于计算卖出盈亏)
        curr_vol, curr_cost, _ = self.pos_mgr.get_position(stock)
        
        action_str = "买入" if action_type == xtconstant.STOCK_BUY else "卖出"
        
        # 2. 计算费用与滑点 [修改] 优化滑点为 0.5%
        trade_price = price * 1.005 if action_type == xtconstant.STOCK_BUY else price * 0.995
        amount = trade_price * volume
        
        # 3. 盈亏计算 (仅卖出时有意义)
        pnl = 0.0
        if action_type == xtconstant.STOCK_SELL and curr_vol > 0:
            pnl = (trade_price - curr_cost) * volume
        
        print(f"\n>>> [{'模拟' if SIMULATION else '实盘'}执行] {stock} {action_str} {volume}股 @ {trade_price:.2f} | 说明: {remark}")

        # 4. 执行逻辑
        if SIMULATION:
            # 模拟：更新内存和 current.csv
            self.pos_mgr.update_sim_position(stock, action_type, volume, trade_price)
        #else:
            # 实盘：发送订单
            #self.trader.order_stock(
            #    self.acc, stock, action_type, int(volume), xtconstant.FIX_PRICE, trade_price, f"策略:{remark}", "0"
            #)
            print(f"[实盘]模拟执行完成: {stock} {action_str} {volume}股 @ {trade_price:.2f}")

        # 5. 统一写日志
        self.log_trade_csv(stock, action_str, volume, trade_price, curr_cost, pnl)
        
        # 6. 更新每日风控状态
        st_action = 'buy' if action_type == xtconstant.STOCK_BUY else 'sell'
        self.update_stock_state(stock, st_action, money_amount=(amount if st_action=='buy' else 0))

    def calculate_atr_data(self, stock_list):
        # 1. 筛选需要计算的股票
        need_calc = [s for s in stock_list if s not in self.atr_map]
        if not need_calc: return

        # 2. 获取数据
        data_map = xtdata.get_market_data(
            field_list=['high', 'low', 'close'], 
            stock_list=need_calc, 
            period='1d', 
            count=ATR_PERIOD+10, 
            dividend_type='front'
        )

        # --- 【核心修复】检查并转置数据 ---
        # 如果列名是日期（长度为8，如'20251207'），说明数据是横着的，需要转置
        sample_col = data_map['close'].columns[0] if len(data_map['close'].columns) > 0 else ''
        if len(str(sample_col)) == 8 and str(sample_col).isdigit():
            # print("检测到数据格式为 [行=股票, 列=时间]，正在执行转置(.T)...")
            for field in ['high', 'low', 'close']:
                data_map[field] = data_map[field].T
        # --------------------------------

        # 3. 开始计算 ATR
        for stock in need_calc:
            # 现在数据已经转置，列名就是股票代码了，这个判断可以正常工作了
            if stock not in data_map['close'].columns:
                self.atr_map[stock] = None
                continue
                
            # 提取该股票的数据列
            try:
                df = pd.DataFrame({
                    'high':  data_map['high'][stock],
                    'low':   data_map['low'][stock],
                    'close': data_map['close'][stock]
                })
            except KeyError:
                self.atr_map[stock] = None
                continue
            
            # 去除空值（停牌日）
            df.dropna(inplace=True)
            if len(df) < ATR_PERIOD: 
                self.atr_map[stock] = None
                continue
                
            # 计算 TR 和 ATR
            tr = pd.concat([
                df['high'] - df['low'], 
                (df['high'] - df['close'].shift(1)).abs(), 
                (df['low'] - df['close'].shift(1)).abs()
            ], axis=1).max(axis=1)
            
            self.atr_map[stock] = tr.rolling(window=ATR_PERIOD).mean().iloc[-1]
            
        print(f"ATR计算完成，成功更新 {len(need_calc)} 只股票")
       

    def is_limit_down(self, tick):
        pct = (tick['lastPrice'] - tick['lastClose']) / tick['lastClose']
        if tick['bidVol'][0] == 0 and pct < -0.05: return True
        return False

    def check_benchmark_risk(self):
        tick = xtdata.get_full_tick([BENCHMARK_INDEX])
        if not tick or BENCHMARK_INDEX not in tick: return False, 0.0
        d = tick[BENCHMARK_INDEX]
        if d['lastClose'] == 0: return False, 0.0
        pct = (d['lastPrice'] - d['lastClose']) / d['lastClose']
        return (pct < BENCHMARK_RISK_THRESH), pct

    def start(self):
        mode_str = "模拟盘(Input/Current CSV)" if SIMULATION else "实盘(QMT账户 + Input CSV)"
        print(f">>> [启动策略] 模式: {mode_str}")
        
        self.trader.start()
        res = self.trader.connect()
        if res != 0:
            print(f"!!! 连接失败: {res}")
            return
        
        # 初始化持仓管理器
        self.pos_mgr = PositionManager(self.trader, self.acc)
        
        xtdata.subscribe_quote(BENCHMARK_INDEX, period='tick', count=1)

        while True:
            try:
                self.check_date_rotation()
                self.run_logic()
            except Exception as e:
                import traceback
                print(f"!!! 运行异常: {e}")
                traceback.print_exc()
            time.sleep(LOOP_INTERVAL)

    def run_logic(self):
        now_dt = datetime.datetime.now(BJ_TZ)
        now_time = now_dt.time()
        
        # 简单时间过滤
        if (now_time < datetime.time(9, 30)) or (now_time > datetime.time(15, 0)):
            # print("非交易时间...", end="\r")
            return

        is_crash, m_pct = self.check_benchmark_risk()
        
        # --- 数据源切换 ---
        cash, total_asset = self.pos_mgr.get_cash_and_asset()
        monitor_stocks = self.pos_mgr.get_all_positions_codes()

        monitor_set = set(monitor_stocks) | set(self.data['stocks'].keys()) 
        stock_list = list(monitor_set)
        
        if not stock_list: 
            print(f"\r[{now_time}] 空仓且无关注股票...", end="")
            return

        self.calculate_atr_data(stock_list)
        for s in stock_list: xtdata.subscribe_quote(s, period='tick', count=1)
        ticks = xtdata.get_full_tick(stock_list)
        
        quota_left = MAX_DAILY_BUY_AMOUNT - self.data['daily_buy_total']
        print(f"\r[{now_time}] 模式:{'SIM' if SIMULATION else 'REAL'} | 大盘:{m_pct:.2%} | 额度:{quota_left:.0f} | 监控:{len(stock_list)}只", end="")

        for stock in stock_list:
            if stock not in ticks: continue
            tick = ticks[stock]
            price = tick['lastPrice']
            pre = tick['lastClose']
            high_price = tick['high']
            low_price = tick['low'] 
            
            if price <= 0: continue
            
            day_pct = (price - pre) / pre
            day_high_pct = (high_price - pre) / pre
            
            # --- 统一持仓数据查询 ---
            vol, avg_cost, market_val = self.pos_mgr.get_position(stock)
            
            total_return_pct = 0.0
            if avg_cost > 0:
                total_return_pct = (price - avg_cost) / avg_cost
            
            atr = self.atr_map.get(stock, pre*0.03)
            if atr == None: atr = pre*0.03

            # 动态网格阈值
            dyn_prof_line = (2 * atr) / pre  
            dyn_loss_line = -(2 * atr) / pre 
            
            state = self.get_stock_state(stock)

            # --- 卖出逻辑 (仅当持有仓位时触发) ---
            if state['sold'] == 0 and vol > 0:
                reason = ""
                # 1. 总仓止盈/止损
                if total_return_pct > HOLD_PROFIT_PCT:
                    reason = f"总仓止盈(>{HOLD_PROFIT_PCT:.0%})"
                elif total_return_pct < HOLD_LOSS_PCT:
                    reason = f"总仓止损(<{HOLD_LOSS_PCT:.0%})"
                # 2. 移动止盈
                elif day_high_pct > dyn_prof_line:
                    drawdown = day_high_pct - day_pct
                    if drawdown >= TRAILING_DRAWDOWN:
                        reason = f"移动止盈(最高{day_high_pct:.1%} 回撤{drawdown:.1%})"
                # 3. ATR 日内止损
                elif (dyn_loss_line > day_pct > BUY_DIP_PCT):
                    reason = f"ATR止损(<{dyn_loss_line:.1%})"
                
                if reason:
                    self.execute_trade(stock, xtconstant.STOCK_SELL, vol, price, reason)
                    continue

            # --- 买入逻辑 (空仓且大盘正常时触发) ---
            if state['bought'] == 0 and not is_crash:
                # [修改] 增加右侧反弹逻辑：
                # 1. 跌幅足够深 (<-6%)
                # 2. 且当前价格比今日最低价反弹了 REBOUND_PCT (0.5%)，确认不是正在跳水
                rebound_ratio = 0.0
                if low_price > 0:
                     rebound_ratio = (price - low_price) / low_price
                
                if day_pct < BUY_DIP_PCT and rebound_ratio >= REBOUND_PCT:
                    if self.is_limit_down(tick): continue

                    # 计算买入量
                    buy_volume = int(BUY_QUOTA / price / 100) * 100
                    if buy_volume == 0: continue
                    
                    est_cost = price * buy_volume * 1.01
                    projected_value = market_val + est_cost
                    
                    # 各种风控检查
                    check_asset = total_asset if total_asset > 0 else (cash + market_val)
                    if check_asset > 0 and (projected_value / check_asset) > SINGLE_STOCK_LIMIT_PCT:
                        continue # 单标的超限
                    elif quota_left < est_cost:
                        continue # 每日额度不足
                    
                    self.execute_trade(stock, xtconstant.STOCK_BUY, buy_volume, price, f"深跌抄底({day_pct:.2%},反弹{rebound_ratio:.2%})")
                    quota_left -= est_cost

if __name__ == '__main__':
    strategy = RobustStrategy()
    strategy.start()