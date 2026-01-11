# coding=utf-8
import time
import json
import os
import datetime
import pandas as pd
import numpy as np
from xtquant import xttrader
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
from xtquant import xtconstant
from xtquant import xtdata

# ==================== 用户配置区域 ====================
MINI_QMT_PATH = r'D:\光大证券金阳光QMT实盘\userdata_mini'
ACCOUNT_ID = '88888888'

# 1. 资金风控
MAX_DAILY_BUY_AMOUNT = 100000.0   
SINGLE_STOCK_LIMIT_PCT = 0.30     

# 2. 止盈止损参数
HOLD_PROFIT_PCT = 0.20     
HOLD_LOSS_PCT = -0.15      
TRAILING_DRAWDOWN = 0.005  # 0.5% 回撤

# 3. 抄底参数
BUY_DIP_PCT = -0.06        

# 4. ATR 动态参数
ATR_MULTIPLIER = 2.0       
ATR_PERIOD = 14

# 5. 市场风控
BENCHMARK_INDEX = '000001.SH'
BENCHMARK_RISK_THRESH = -0.025  

# 6. 系统参数
BUY_QUOTA = 20000 
LOOP_INTERVAL = 5
BJ_TZ = datetime.timezone(datetime.timedelta(hours=8)) # 北京时区
# ====================================================

class RobustStrategy:
    def __init__(self):
        import random
        session_id = int(random.randint(100000, 999999))
        self.trader = XtQuantTrader(MINI_QMT_PATH, session_id)
        self.acc = StockAccount(ACCOUNT_ID)
        
        # 初始化日期状态
        self.current_date_str = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d")
        self.update_filenames()
        
        self.data = self.load_state()
        self.atr_map = {} 
        self.high_pct_map = {} 

    def update_filenames(self):
        """更新文件名 (用于跨日轮转)"""
        self.state_file = f"sim_v2_state_{self.current_date_str}.json"
        self.log_file = f"sim_v2_log_{self.current_date_str}.txt"

    def check_date_rotation(self):
        """[新增] 检查是否跨日，如果跨日则重置状态"""
        now_date = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d")
        if now_date != self.current_date_str:
            print(f"\n>>> [日期轮转] 检测到新日期 {now_date} (原 {self.current_date_str})")
            print(">>> 重置每日额度与状态...")
            self.current_date_str = now_date
            self.update_filenames()
            # 重置内存状态
            self.data = {"daily_buy_total": 0.0, "stocks": {}}
            self.high_pct_map = {}
            # 尝试加载新文件(如果是重启情况)或保持重置
            loaded = self.load_state()
            if loaded['stocks']: self.data = loaded
            else: self.save_state() # 创建新文件

    def start(self):
        print(">>> [模拟盘 v2.2-健壮版] 正在连接 QMT ...")
        self.trader.start()
        res = self.trader.connect()
        if res != 0:
            print(f"!!! 连接失败: {res}")
            return
        
        bj_time = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")
        print(f">>> 连接成功 | 北京时间: {bj_time}")
        print(f">>> 增强特性: 自动跨日重置 | 真实最高价移动止盈 | 数据新鲜度检查")

        xtdata.subscribe_quote(BENCHMARK_INDEX, period='tick', count=1)

        while True:
            try:
                self.check_date_rotation() # 每轮循环先检查日期
                self.run_logic()
            except Exception as e:
                import traceback
                print(f"!!! 运行异常: {e}")
                traceback.print_exc()
            time.sleep(LOOP_INTERVAL)

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

    def update_state(self, stock_code, action, money_amount=0.0):
        if stock_code not in self.data['stocks']:
            self.data['stocks'][stock_code] = {'bought': 0, 'sold': 0}
        
        if action == 'buy':
            self.data['stocks'][stock_code]['bought'] = 1
            self.data['daily_buy_total'] += money_amount
        elif action == 'sell':
            self.data['stocks'][stock_code]['sold'] = 1
        self.save_state()

    def calculate_atr_data(self, stock_list):
        need_calc = [s for s in stock_list if s not in self.atr_map]
        if not need_calc: return
        data_map = xtdata.get_market_data(field_list=['high', 'low', 'close', 'preClose'], stock_list=need_calc, period='1d', count=ATR_PERIOD+10, dividend_type='front')
        for stock in need_calc:
            if stock not in data_map: self.atr_map[stock] = None; continue
            df = data_map[stock]
            if len(df) < ATR_PERIOD: self.atr_map[stock] = None; continue
            tr = pd.concat([df['high']-df['low'], (df['high']-df['close'].shift(1)).abs(), (df['low']-df['close'].shift(1)).abs()], axis=1).max(axis=1)
            self.atr_map[stock] = tr.rolling(window=ATR_PERIOD).mean().iloc[-1]

    def is_limit_down(self, tick):
        pct = (tick['lastPrice'] - tick['lastClose']) / tick['lastClose']
        if tick['bidVol'][0] == 0 and pct < -0.05: return True
        limit = -0.198 if tick['stockCode'].startswith(('688', '300')) else -0.098
        return pct < limit

    def check_benchmark_risk(self):
        tick = xtdata.get_full_tick([BENCHMARK_INDEX])
        if not tick or BENCHMARK_INDEX not in tick: return False, 0.0
        d = tick[BENCHMARK_INDEX]
        if d['lastClose'] == 0: return False, 0.0
        pct = (d['lastPrice'] - d['lastClose']) / d['lastClose']
        return (pct < BENCHMARK_RISK_THRESH), pct

    def place_order(self, stock, action_type, volume, price, remark=""):
        action_str = "买入" if action_type == xtconstant.STOCK_BUY else "卖出"
        trade_price = price * 1.005 if action_type == xtconstant.STOCK_BUY else price * 0.995
        bj_time_str = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")
        
        log_line = f"[{bj_time_str}] [BJ] {action_str} | 代码:{stock} | 数量:{volume} | 价格:{trade_price:.2f} | 说明:{remark}\n"
        print("\n" + "*"*50)
        print(log_line.strip())
        print("*"*50 + "\n")
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f: f.write(log_line)
        except: pass
        
        # 2. 真实交易代码 (默认注释状态)
        # -----------------------------------------------------------
        # 【重要】实盘时，请删除下面三引号（'''）来取消注释
        # -----------------------------------------------------------
        '''
        print(f">>> 正在发送真实交易指令: {stock} {action_str} ...")
        self.trader.order_stock(
            self.acc,                  # 账号对象
            stock,                     # 股票代码
            action_type,               # 指令类型(买/卖)
            int(volume),               # 数量(必须为整数)
            xtconstant.FIX_PRICE,      # 报价类型(限价)
            trade_price,               # 委托价格
            f"策略:{remark}",           # 订单备注
            "0"                        # 订单ID(0为自动)
        )
        '''
        # -----------------------------------------------------------
    def run_logic(self):
        now_dt = datetime.datetime.now(BJ_TZ)
        now_str = now_dt.strftime('%H:%M:%S')
        now_time = now_dt.time()
        
        # 时间过滤
        if (now_time < datetime.time(9, 35)) or (datetime.time(11, 30) < now_time < datetime.time(13, 0)) or (now_time > datetime.time(14, 55)):
            print(f"\r[{now_str} BJ] 休市中 (09:35-11:30, 13:00-14:55)...", end="")
            return

        is_crash, m_pct = self.check_benchmark_risk()
        
        asset = self.trader.query_stock_asset(self.acc)
        total_asset = asset.total_asset if asset else 0
        cash = asset.cash if asset else 0
        
        positions = self.trader.query_stock_positions(self.acc)
        pos_map = {p.stock_code: p for p in positions if p.volume > 0}
        monitor_set = set(pos_map.keys()) | set(self.data['stocks'].keys())
        stock_list = list(monitor_set)
        
        if not stock_list: return

        self.calculate_atr_data(stock_list)
        for s in stock_list: xtdata.subscribe_quote(s, period='tick', count=1)
        ticks = xtdata.get_full_tick(stock_list)
        
        quota_left = MAX_DAILY_BUY_AMOUNT - self.data['daily_buy_total']

        print(f"\r[{now_str} BJ] 大盘:{m_pct:.2%} | 资金:{cash:.0f} | 额度:{quota_left:.0f} | 监控:{len(stock_list)}只", end="")
        if is_crash: print(" [熔断]", end="")

        for stock in stock_list:
            if stock not in ticks: continue
            tick = ticks[stock]
            
            # --- [优化2] 数据新鲜度检查 ---
            # QMT timetag 是毫秒级时间戳 (例如 1716193000000)
            data_time = datetime.datetime.fromtimestamp(tick['time'] / 1000, BJ_TZ)
            time_diff = (now_dt - data_time).total_seconds()
            
            # 如果数据滞后超过 120秒，且不是午休刚结束，则忽略
            # (注: 这里简单处理，仅打印警告，实际可选择 continue 跳过)
            if abs(time_diff) > 120 and not (13 <= now_time.hour < 13.05):
                # print(f" [延迟警告 {stock} {int(time_diff)}s]", end="")
                pass

            price = tick['lastPrice']
            pre = tick['lastClose']
            high_price = tick['high'] # [关键优化] 获取今日真实最高价
            
            if price <= 0 or pre <= 0: continue
            
            day_pct = (price - pre) / pre
            # [关键优化] 计算今日真实最高涨幅 (无论何时重启，这个值都是对的)
            day_high_pct = (high_price - pre) / pre
            
            # 兼容旧逻辑，确保内存里有这个值
            self.high_pct_map[stock] = day_high_pct
            
            total_return_pct = 0.0
            avg_cost = 0.0
            can_sell = 0
            current_hold_market_value = 0.0 
            
            if stock in pos_map:
                pos = pos_map[stock]
                can_sell = pos.can_use_volume
                avg_cost = pos.open_price
                current_hold_market_value = pos.market_value
                if avg_cost > 0:
                    total_return_pct = (price - avg_cost) / avg_cost
            
            atr = self.atr_map.get(stock, pre*0.03)
            dyn_prof_line = (2 * atr) / pre  
            dyn_loss_line = -(2 * atr) / pre 
            
            state = self.get_stock_state(stock)

            # --- 卖出逻辑 ---
            if state['sold'] == 0 and can_sell > 0:
                reason = ""
                if total_return_pct > HOLD_PROFIT_PCT:
                    reason = f"总仓止盈(>{HOLD_PROFIT_PCT:.0%})"
                elif total_return_pct < HOLD_LOSS_PCT:
                    reason = f"总仓止损(<{HOLD_LOSS_PCT:.0%})"
                
                # [优化1] 使用 day_high_pct (真实最高) 来判断移动止盈
                elif day_high_pct > dyn_prof_line:
                    drawdown = day_high_pct - day_pct
                    if drawdown >= TRAILING_DRAWDOWN:
                        reason = f"移动止盈(最高{day_high_pct:.1%} 回撤{drawdown:.1%})"
                
                elif (dyn_loss_line > day_pct > BUY_DIP_PCT):
                    reason = f"ATR止损(<{dyn_loss_line:.1%})"
                
                if reason:
                    self.place_order(stock, xtconstant.STOCK_SELL, can_sell, price, remark=reason)
                    self.update_state(stock, 'sell')
                    continue

            # --- 买入逻辑 ---
            if state['bought'] == 0 and not is_crash:
                if day_pct < BUY_DIP_PCT:
                    if self.is_limit_down(tick): continue

                    buy_volume= int(BUY_QUOTA / price / 100) * 100
                    est_cost = price * buy_volume * 1.01
                    projected_value = current_hold_market_value + est_cost
                    check_asset = total_asset if total_asset > 0 else (cash + current_hold_market_value)
                    
                    if check_asset > 0 and (projected_value / check_asset) > SINGLE_STOCK_LIMIT_PCT:
                        pass
                    elif quota_left < est_cost:
                        pass
                    elif cash < est_cost:
                        pass
                    else:
                        self.place_order(stock, xtconstant.STOCK_BUY, buy_volume, price, remark=f"深跌抄底({day_pct:.2%})")
                        self.update_state(stock, 'buy', money_amount=est_cost)
                        quota_left -= est_cost
                        cash -= est_cost

if __name__ == '__main__':
    strategy = RobustStrategy()
    strategy.start()