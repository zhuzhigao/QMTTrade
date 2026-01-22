# coding=utf-8
import time
import os
import datetime
import pandas as pd
import numpy as np
from xtquant import xttrader, xtconstant, xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

# ==================== 用户配置区域 ====================
# [核心开关] True=模拟模式(读CSV), False=实盘模式(读账户)
# 注意：实盘模式下请确保 MiniQMT 客户端已登录且路径配置正确
SIMULATION = False 

MINI_QMT_PATH = r'D:\光大证券金阳光QMT实盘\userdata_mini'
ACCOUNT_ID = '47601131'

# 文件路径配置
CSV_INPUT_POS = 'siminput.csv'       # 模拟：初始持仓 / 实盘：重点关注池
CSV_CURRENT_POS = 'simcurrent.csv'   # 模拟：当前持仓（动态更新）
LOG_FILE_REAL = 'tradelog.csv'       # 实盘：交易日志
LOG_FILE_SIM = 'simlog.csv'          # 模拟：交易日志

# 1. 资金风控
MAX_DAILY_BUY_AMOUNT = 30000.0   # 每日最大买入金额
SINGLE_STOCK_LIMIT_PCT = 0.30    # 单只股票最大仓位占比

# 2. 止盈止损参数
HOLD_PROFIT_PCT = 0.20     
HOLD_LOSS_PCT = -0.15      
TRAILING_DRAWDOWN = 0.005  

# 3. 抄底参数
BUY_DIP_PCT = -0.06        # 触发抄底的跌幅阈值 (-6%)
REBOUND_PCT = 0.005        # 右侧交易确认：从最低点反弹幅度 (0.5%)

# 4. ATR 动态参数
ATR_MULTIPLIER = 2.0       
ATR_PERIOD = 14

# 5. 市场风控
BENCHMARK_INDEX = '000001.SH'
BENCHMARK_RISK_THRESH = -0.025  

# 6. 系统参数
BUY_QUOTA = 15000 
LOOP_INTERVAL = 5
BJ_TZ = datetime.timezone(datetime.timedelta(hours=8))

# 7. 滑点参数 (当获取不到盘口价格时的备用滑点)
HUADIAN = 0.002
# ====================================================

class PositionManager:
    """
    持仓管理器：负责抹平【实盘】与【模拟】的数据差异
    保持原逻辑不变，确保正确读取 CSV 或 实盘账户
    """
    def __init__(self, trader, account):
        self.trader = trader
        self.account = account
        self.sim_positions = {} 
        
        if SIMULATION:
            self.init_sim_data()
            print(f">>> [模拟] 已加载最新持仓文件， 共 {len(self.sim_positions)} 只股票")
            
    def load_input_csv_stocks(self):
        """仅读取 siminput.csv 中的股票代码，用于实盘监控"""
        stocks = set()
        if os.path.exists(CSV_INPUT_POS):
            try:
                df = pd.read_csv(CSV_INPUT_POS, encoding='utf-8-sig')
                if 'stock_code' in df.columns:
                    stocks = set(df['stock_code'].astype(str).dropna().tolist())
            except Exception as e:
                print(f"!!! 读取 {CSV_INPUT_POS} 失败: {e}")
        return list(stocks)
        
    def init_sim_data(self):
        """模拟模式：从 input.csv 或 current.csv 加载持仓"""
        load_file = CSV_CURRENT_POS if os.path.exists(CSV_CURRENT_POS) else CSV_INPUT_POS
        
        if os.path.exists(load_file):
            try:
                df = pd.read_csv(load_file, encoding='utf-8-sig')
                if not df.empty and all(col in df.columns for col in ['stock_code', 'cost', 'volume']):
                    self.sim_positions.clear()
                    for _, row in df.iterrows():
                        self.sim_positions[row['stock_code']] = {
                            'volume': int(row['volume']),
                            'cost': float(row['cost'])
                        }
            except Exception as e:
                print(f"!!! [模拟] 读取持仓文件失败: {e}")
        else:
             print(f"!!! [模拟]没有找到持仓文件")

    def download_historical_data(self, monitor_stocks):
        # 获取当前的北京时间
        # 无论服务器在伦敦还是纽约，这个 time_now 永远是北京时间
        now_bj = datetime.datetime.now(BJ_TZ)

        # --- 计算日期 ---

        days_to_look_back = ATR_PERIOD * 2 

        # 使用北京时间计算 start 和 end
        start_date = (now_bj - datetime.timedelta(days=days_to_look_back)).strftime('%Y%m%d')
        end_date = now_bj.strftime('%Y%m%d')

        print(f"准备下载数据范围: {start_date} ~ {end_date}")
        for stock_code in list(monitor_stocks):
            xtdata.download_history_data(stock_code, period='1d', start_time=start_date, end_time=end_date)
        print(f"!!! 数据下载完成，共 {len(monitor_stocks)} 只股票")

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
            self.init_sim_data() 
            return [k for k, v in self.sim_positions.items()]
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
            return 10000000.0, 10000000.0 + total_mkt_value 
        else:
            asset = self.trader.query_stock_asset(self.account)
            if asset:
                return asset.cash, asset.total_asset
            return 0.0, 0.0

    def update_sim_position(self, stock, action_type, trade_vol, trade_price):
        if not SIMULATION: return
        if stock not in self.sim_positions:
            self.sim_positions[stock] = {'volume': 0, 'cost': 0.0}
        curr = self.sim_positions[stock]
        
        if action_type == xtconstant.STOCK_BUY:
            new_cost = (curr['volume'] * curr['cost'] + trade_vol * trade_price) / (curr['volume'] + trade_vol)
            curr['volume'] += trade_vol
            curr['cost'] = new_cost
        elif action_type == xtconstant.STOCK_SELL:
            curr['volume'] = max(0, curr['volume'] - trade_vol)
            if curr['volume'] == 0:
                curr['cost'] = 0.0

        # 不清理持仓为0的，继续监控
        #if curr['volume'] == 0:
        #    del self.sim_positions[stock]
        
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
        
        # [改进1] 移除 JSON，改为内存变量（模拟用）+ 实时查询（实盘用）
        self.sim_daily_buy = 0.0 
        self.sim_today_traded_cache = set() # 格式 {"000001.SH_23", ...} 用于模拟盘去重
        
        self.atr_map = {} 
        self.pos_mgr = None
        self.lastest_init_stocks = set()

    # [改进] 获取当日已买入金额 (替代原 JSON 逻辑)
    def get_daily_buy_amount(self):
        if SIMULATION:
            return self.sim_daily_buy
        
        # 实盘：查询当日委托，计算已占用资金
        try:
            orders = self.trader.query_stock_orders(self.acc, cancelable_only=False)
            total_buy = 0.0
            today_str = datetime.datetime.now().strftime("%Y%m%d")
            
            for o in orders:
                # 过滤出今天的买入单
                order_date_str = ""
                ts = o.order_time 

                # 【关键步骤】将时间戳转为 "20250121" 格式的字符串
                # 注意：如果是实盘，order_time 可能是 0 (如废单)，需要容错
                if ts > 0:
                    order_date_str = datetime.datetime.fromtimestamp(ts).strftime("%Y%m%d")

                if order_date_str.startswith(today_str) and o.order_type == xtconstant.STOCK_BUY:
                    amt = o.price * o.order_volume
                    # 如果市价单price为0，尝试用成交金额
                    if amt == 0 and o.trade_amount > 0:
                        amt = o.trade_amount
                    total_buy += amt
            return total_buy
        except Exception as e:
            print(f"!!! 查询当日委托失败: {e}")
            return 9999999.0 # 查失败则风控拉满，暂停买入

    # [新增] 核心风控：检查今日是否已操作过 (严格限制每天一次)
    def has_traded_today(self, stock_code, action_type):
        """
        返回 True 表示今天已经对该股票做过该方向的操作，应跳过
        """
        # 1. 模拟模式：查内存 Set
        if SIMULATION:
            key = f"{stock_code}_{action_type}"
            return key in self.sim_today_traded_cache

        # 2. 实盘模式：查当日委托记录
        try:
            orders = self.trader.query_stock_orders(self.acc, cancelable_only=False)
            today_str = datetime.datetime.now().strftime("%Y%m%d")
            
            for o in orders:
                if not str(o.order_time).startswith(today_str): continue
                if o.stock_code == stock_code and o.order_type == action_type:
                    # 只要下过单(哪怕废单)，严格执行纪律，今天不再操作
                    return True 
            return False
        except:
            return True # 查不到数据就保守风控

    # [新增] 检查是否存在未成交挂单 (防止5秒循环内重复报单)
    def has_open_order(self, stock_code, action_type):
        if SIMULATION: return False
        try:
            orders = self.trader.query_stock_orders(self.acc, cancelable_only=True)
            for o in orders:
                if o.stock_code == stock_code and o.order_type == action_type:
                    return True
        except:
            pass
        return False

    def check_date_rotation(self):
        now_date = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d")
        if now_date != self.current_date_str:
            print(f"\n>>> [日期轮转] 检测到新日期 {now_date}")
            self.current_date_str = now_date
            # 重置每日状态
            self.sim_daily_buy = 0.0
            self.sim_today_traded_cache.clear()
            self.pos_mgr.download_historical_data(self.pos_mgr.get_all_positions_codes())

    def log_trade_csv(self, stock, action_str, volume, price, cost, pnl):
        filename = LOG_FILE_SIM if SIMULATION else LOG_FILE_REAL
        time_str = datetime.datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")
        
        # 如果文件不存在，写入表头
        if not os.path.exists(filename):
            with open(filename, 'w', encoding='utf-8-sig') as f:
                f.write("股票,时间,操作,股数,成交价,持仓成本,盈亏\n")
        line = f"{stock},{time_str},{action_str},{volume},{price:.2f},{cost:.2f},{pnl:.2f}\n"
        with open(filename, 'a', encoding='utf-8-sig') as f:
            f.write(line)

    def execute_trade(self, stock, action_type, volume, base_price, tick_data=None, remark=""):
        """
        统一执行交易：处理实盘下单、模拟更新、日志记录
        [改进] 增加了 tick_data 参数用于获取买一卖一价
        """
        # 1. 挂单检查 (仅实盘)
        if not SIMULATION and self.has_open_order(stock, action_type):
            print(f"!!! [跳过] {stock} 存在未成交挂单")
            return

        curr_vol, curr_cost, _ = self.pos_mgr.get_position(stock)
        action_str = "买入" if action_type == xtconstant.STOCK_BUY else "卖出"
        
        # 2. 价格优化 [改进]：优先用对手价，HUADIAN作为兜底
        trade_price = base_price
        if tick_data:
            if action_type == xtconstant.STOCK_BUY:
                # 买入用卖一 (Ask1)
                ask1 = tick_data.get('askPrice', [0])[0]
                trade_price = ask1 if ask1 > 0 else base_price * (1 + HUADIAN)
            else:
                # 卖出用买一 (Bid1)
                bid1 = tick_data.get('bidPrice', [0])[0]
                trade_price = bid1 if bid1 > 0 else base_price * (1 - HUADIAN)
        else:
            trade_price = base_price * (1 + HUADIAN) if action_type == xtconstant.STOCK_BUY else base_price * (1 - HUADIAN)

        amount = trade_price * volume
        
        # 3. 盈亏计算
        pnl = 0.0
        if action_type == xtconstant.STOCK_SELL and curr_vol > 0:
            pnl = (trade_price - curr_cost) * volume
        
        print(f"\n>>> [{'模拟' if SIMULATION else '实盘'}执行] {stock} {action_str} {volume}股 @ {trade_price:.2f} | 说明: {remark}")

        # 4. 执行逻辑
        if SIMULATION:
            self.pos_mgr.update_sim_position(stock, action_type, volume, trade_price)
            
            # [新增] 记录今日已操作 (内存)
            key = f"{stock}_{action_type}"
            self.sim_today_traded_cache.add(key)
            
            if action_type == xtconstant.STOCK_BUY:
                self.sim_daily_buy += amount
        else:
            # [实盘下单] (此处保留注释，用户需手动开启)
            # self.trader.order_stock(
            #    self.acc, stock, action_type, int(volume), xtconstant.FIX_PRICE, trade_price, f"策略:{remark}", "0"
            # )
            print(f"[实盘] 委托已发送(演示): {stock} {action_str} {trade_price:.2f}")

        # 5. 写日志
        self.log_trade_csv(stock, action_str, volume, trade_price, curr_cost, pnl)

    def check_benchmark_risk(self):
        """
        检查大盘风控
        返回: (是否暴跌风险, 大盘涨跌幅)
        """
        try:
            tick = xtdata.get_full_tick([BENCHMARK_INDEX])
            if not tick or BENCHMARK_INDEX not in tick:
                return False, 0.0
            
            price = tick[BENCHMARK_INDEX]['lastPrice']
            pre_close = tick[BENCHMARK_INDEX]['lastClose']
            
            if pre_close == 0: return False, 0.0
            
            pct = (price - pre_close) / pre_close
            
            # 如果大盘跌幅超过阈值（例如 -2.5%），触发风控
            if pct < BENCHMARK_RISK_THRESH:
                return True, pct
            return False, pct
        except Exception as e:
            print(f"!!! 大盘风控检查异常: {e}")
            return False, 0.0
    def is_limit_down(self, tick):
        """
        检查是否跌停
        """
        try:
            price = tick['lastPrice']
            # QMT的tick数据中通常包含跌停价 'lowLimit' (部分版本可能叫 'downStopPrice')
            # 这里做一个通用的判定，如果没有跌停价字段，简单粗暴判定跌幅 > 9.8% (针对非创业板)
            # 建议优先使用 'lowLimit'
            limit_down_price = tick.get('lowLimit') or tick.get('downStopPrice')
            
            if limit_down_price:
                # 价格接近跌停价 (误差 0.05)
                if abs(price - limit_down_price) < 0.03:
                    return True
            else:
                # 备用逻辑
                pre = tick['lastClose']
                if (price - pre) / pre < -0.095:
                    return True
        except:
            pass
        return False    
    
    def is_limit_up(self, tick):
        """
        检查是否涨停
        """
        try:
            price = tick['lastPrice']
            limit_up_price = tick.get('highLimit') or tick.get('upStopPrice')
            
            if limit_up_price:
                if abs(price - limit_up_price) < 0.03:
                    return True
            else:
                pre = tick['lastClose']
                if (price - pre) / pre > 0.095:
                    return True
        except:
            pass
        return False

    def calculate_atr_data(self, stock_list):
        need_calc = [s for s in stock_list if s not in self.atr_map]
        if not need_calc: return
        
        data_map = xtdata.get_market_data(
            field_list=['high', 'low', 'close'], 
            stock_list=need_calc, 
            period='1d', 
            count=ATR_PERIOD+10, 
            dividend_type='front'
        )
        
        # 格式修复：转置数据
        sample_col = data_map['close'].columns[0] if len(data_map['close'].columns) > 0 else ''
        if len(str(sample_col)) == 8 and str(sample_col).isdigit():
            for field in ['high', 'low', 'close']:
                data_map[field] = data_map[field].T

        # 3. 开始计算 ATR
        for stock in need_calc:
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
                df.dropna(inplace=True)
                if len(df) < ATR_PERIOD: 
                    self.atr_map[stock] = None
                    continue
                tr = pd.concat([
                    df['high'] - df['low'], 
                    (df['high'] - df['close'].shift(1)).abs(), 
                    (df['low'] - df['close'].shift(1)).abs()
                ], axis=1).max(axis=1)
                self.atr_map[stock] = tr.rolling(window=ATR_PERIOD).mean().iloc[-1]
            except:
                self.atr_map[stock] = None
                print(f"!!! ATR计算异常: {stock}")
        print(f"ATR计算完成，成功更新 {len(need_calc)} 只股票")
    
    def print_dashboard(self, now_time, m_pct, quota_left, stock_list, ticks):
        """
        核心显示模块：负责渲染监控看板
        """
        # 1. 准备数据
        lines = []
        mode = 'SIM' if SIMULATION else 'REAL'
        cash, total_asset = self.pos_mgr.get_cash_and_asset()
        
        # 2. 拼接头部信息 (状态栏)
        lines.append(f"========== 量化监控看板 ({now_time}) ==========")
        lines.append(f"模式: {mode} | 大盘: {m_pct:+.2%} | 资金: {cash:.0f} | 额度: {quota_left:.0f}")
        lines.append("-" * 65)
        lines.append(f"{'代码':<10} | {'名称':<8} | {'现价':<8} | {'涨跌幅':<8} | {'ATR':<6} | {'持仓/信号'}")
        lines.append("-" * 65)

        # 3. 遍历股票拼接行数据
        # 为了版面整洁，可以按涨跌幅排序显示
        # sorted_stocks = sorted(stock_list, key=lambda s: ticks[s]['lastPrice'] if s in ticks else 0, reverse=True)
        
        for stock in stock_list:
            if stock not in ticks: continue
            tick = ticks[stock]
            price = tick['lastPrice']
            pre = tick['lastClose']
            
            # 计算涨跌
            pct = (price - pre) / pre if pre > 0 else 0
            
            # 获取名称 (兼容写法)
            detail = xtdata.get_instrument_detail(stock)
            name = "--"
            if detail:
                name = detail.get('InstrumentName', '--') if isinstance(detail, dict) else getattr(detail, 'InstrumentName', '--')
            
            # 获取 ATR 值 (用于显示波动率)
            atr_val = self.atr_map.get(stock, 0)
            atr_str = f"{atr_val:.2f}" if atr_val else "-"

            # 获取持仓信息
            vol, cost, _ = self.pos_mgr.get_position(stock)
            
            # 构建信号提示
            status_msg = ""
            if vol > 0:
                pnl_pct = (price - cost) / cost if cost > 0 else 0
                status_msg = f"持仓:{vol}({pnl_pct:+.1%})"
            else:
                if pct < BUY_DIP_PCT: status_msg = "🔥超跌关注"
                else: status_msg = "监控中"
            lines.append(f"{stock:<10} | {name[:4]:<8} | {price:<8.2f} | {pct:<+8.2%} | {atr_str:<6} | {status_msg}")
        
        lines.append("=" * 65)
        os.system('cls' if os.name == 'nt' else 'clear') 
        print("\n".join(lines))

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
        monitor_stocks = self.pos_mgr.get_all_positions_codes()
        self.lastest_init_stocks = set(monitor_stocks)
        self.pos_mgr.download_historical_data(monitor_stocks)
        xtdata.subscribe_quote(BENCHMARK_INDEX, period='tick', count=1)

        while True:
            try:
                self.check_date_rotation()
                self.run_logic()
            except Exception as e:
                import traceback
                print(f"!!! 全局运行异常: {e}")
                traceback.print_exc()
            time.sleep(LOOP_INTERVAL)

    def run_logic(self):
        now_dt = datetime.datetime.now(BJ_TZ)
        now_time = now_dt.time()
        
        # 简单时间过滤
        if (now_time < datetime.time(9, 30)) or (now_time > datetime.time(15, 0)):
            print("非交易时间...", end="\r")
            return

        is_crash, m_pct = self.check_benchmark_risk()
        
        # 刷新股票池
        monitor_stocks = self.pos_mgr.get_all_positions_codes()
        stock_list = list(monitor_stocks)
        new_stocks = set(stock_list) - self.lastest_init_stocks
        if new_stocks:  
            print(f"\n>>> 发现新监控股票: {new_stocks}")
            self.pos_mgr.download_historical_data(new_stocks)
        self.lastest_init_stocks = set(stock_list)
        
        if not stock_list: 
            print(f"\r[{now_time}] 空仓且无关注股票...", end="")
            return

        self.calculate_atr_data(stock_list)
        for s in stock_list: xtdata.subscribe_quote(s, period='tick', count=1)
        ticks = xtdata.get_full_tick(stock_list)
        
        # [改进] 实时获取当日额度
        daily_used = self.get_daily_buy_amount()
        quota_left = MAX_DAILY_BUY_AMOUNT - daily_used
        self.print_dashboard(now_time, m_pct, quota_left, stock_list, ticks)

        # [改进] 增加异常捕获，单只股票报错不影响整体
        for stock in stock_list:
            try:
                if stock not in ticks: continue
                tick = ticks[stock]
                
                # [改进] 行情新鲜度检查 ( > 60秒视为过期)
                # timetag 为毫秒
                timetag = tick.get('timetag', '')
                if timetag:
                    date_time = datetime.datetime.strptime(timetag, "%Y%m%d %H:%M:%S").timestamp()
                    ts = time.time() - date_time
                    if ts > 60:
                        continue

                price = tick['lastPrice']
                pre = tick['lastClose']
                if price <= 0: continue
                
                high_price = tick['high']
                low_price = tick['low'] 
                day_pct = (price - pre) / pre
                day_high_pct = (high_price - pre) / pre
                
                vol, avg_cost, market_val = self.pos_mgr.get_position(stock)
                
                total_return_pct = 0.0
                if avg_cost > 0:
                    total_return_pct = (price - avg_cost) / avg_cost
                
                atr = self.atr_map.get(stock, pre*0.03) or pre*0.03
                dyn_prof_line = (2 * atr) / pre  
                dyn_loss_line = -(2 * atr) / pre 
                
                # --- 卖出逻辑 ---
                if vol > 0:
                    # [新增] 严格执行：今日若买过则不卖(针对T+1)，今日若卖过则不再卖
                    if self.has_traded_today(stock, xtconstant.STOCK_BUY): continue
                    if self.has_traded_today(stock, xtconstant.STOCK_SELL): continue
                    
                    # 挂单检查
                    if self.has_open_order(stock, xtconstant.STOCK_SELL): continue

                    reason = ""
                    if total_return_pct > HOLD_PROFIT_PCT:
                        reason = f"总仓止盈(>{HOLD_PROFIT_PCT:.0%})"
                    elif total_return_pct < HOLD_LOSS_PCT:
                        reason = f"总仓止损(<{HOLD_LOSS_PCT:.0%})"
                    elif day_high_pct > dyn_prof_line:
                        drawdown = day_high_pct - day_pct
                        if drawdown >= TRAILING_DRAWDOWN:
                            reason = f"移动止盈(最高{day_high_pct:.1%} 回撤{drawdown:.1%})"
                    elif (dyn_loss_line > day_pct > BUY_DIP_PCT):
                        reason = f"ATR止损(<{dyn_loss_line:.1%})"
                    
                    if reason:
                        self.execute_trade(stock, xtconstant.STOCK_SELL, vol, price, tick, reason)
                        continue

                # --- 买入逻辑 ---
                # 仅当空仓时才考虑买入 (vol == 0)，避免加仓
                if vol == 0 and not is_crash:
                    # [新增] 严格执行：今日已操作过(买或卖)则不再操作
                    if self.has_traded_today(stock, xtconstant.STOCK_BUY): continue
                    if self.has_traded_today(stock, xtconstant.STOCK_SELL): continue

                    rebound_ratio = 0.0
                    if low_price > 0:
                         rebound_ratio = (price - low_price) / low_price
                    
                    if day_pct < BUY_DIP_PCT and rebound_ratio >= REBOUND_PCT:
                        if self.is_limit_down(tick): continue

                        buy_volume = int(BUY_QUOTA / price / 100) * 100
                        if buy_volume == 0: continue
                        
                        est_cost = price * buy_volume * (1 + HUADIAN)
                        
                        if quota_left < est_cost: continue 
                        
                        # 实盘资产占比检查
                        _, total_asset_val = self.pos_mgr.get_cash_and_asset()
                        if total_asset_val > 0 and (est_cost / total_asset_val) > SINGLE_STOCK_LIMIT_PCT:
                             continue

                        self.execute_trade(stock, xtconstant.STOCK_BUY, buy_volume, price, tick, f"深跌抄底({day_pct:.2%})")
                        quota_left -= est_cost

            except Exception as e:
                # 异常隔离
                continue

if __name__ == '__main__':
    strategy = RobustStrategy()
    strategy.start()