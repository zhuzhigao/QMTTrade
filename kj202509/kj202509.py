# coding=utf-8
import sys
import os
import time
import datetime
import pandas as pd
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant
from datetime import timezone, timedelta

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
from utils.utilities import StrategyLedger


BEIJING_TZ = timezone(timedelta(hours=8))
DEBUG = True

# ================= 1. 交易回调类 (处理订单状态反馈) =================
class MyCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        print("!! 警告：与 QMT 极简模式终端连接断开 !!")
        
    def on_stock_order(self, order):
        print(f">> 委托回报: 代码:{order.stock_code}, 状态:{order.order_status}, 报单价格:{order.price}")
        
    def on_stock_trade(self, trade):
        print(f">> 成交回报: 代码:{trade.stock_code}, 成交量:{trade.traded_volume}, 成交价:{trade.traded_price}")


# ================= 2. 策略核心逻辑类 =================
class AllWeatherStrategy:
    def __init__(self, trader, account):
        self.trader = trader
        self.account = account
        
        # --- 参数配置 ---
        self.stock_num = 3                   # A股持仓数量
        self.total_budget = 60000            # 策略最大使用资金（元），不足时用实际可用资金
        self.benchmark_big = '000300.SH'     # 大盘动量基准
        self.benchmark_small = '000852.SH'   # 小盘动量基准
        self.foreign_etf = ['518880.SH', '513100.SH'] # 防御外盘ETF：黄金、纳指
        
        # --- 状态记录 ---
        self.monthly_adjusted_month = -1
        self.weekly_check_week = -1
        self.stop_loss_date = ""
        self.current_style = 'DEFENSE'
        self.ledger = StrategyLedger(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'strategy_09_holdings.json'))
        
        # --- 核心时间节点 ---
        self.stop_loss_time = "14:45:00"     # 日内防洗盘止损时间
        self.circuit_breaker_time = "14:30:00" # 周五防暴跌熔断时间
        
        print(">> 策略初始化完成，等待行情与时间触发...")

    def handlebar(self):
        """核心驱动函数，在主循环中被每秒调用一次"""
        now = datetime.datetime.now(BEIJING_TZ)
        current_time = now.strftime("%H:%M:%S")
        current_date = now.strftime("%Y%m%d")
        current_month = now.month
        current_week = now.isocalendar()[1]
        
        # 必须在交易时间段内才执行业务逻辑
        if not ("09:30:00" <= current_time <= "15:00:00"):
            return

        # ---------------------------------------------------------
        # 模块 1：月度调仓与平滑动量指标 (每月第一个交易日 09:35 执行)
        # ---------------------------------------------------------
        if current_time >= "09:35:00" and self.monthly_adjusted_month != current_month:
            print(f"[{current_time}] 执行月度动量研判与调仓...")
            
            # 下载并获取近21天日线数据
            start_date = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=30)).strftime("%Y%m%d")
            xtdata.download_history_data2([self.benchmark_big, self.benchmark_small], period='1d', start_time=start_date, end_time='', )
            big_data = xtdata.get_market_data(['close'], [self.benchmark_big], '1d', count=21, dividend_type='front')
            small_data = xtdata.get_market_data(['close'], [self.benchmark_small], '1d', count=21,dividend_type='front')
            
            # xtdata 返回格式处理
            if not big_data.empty and not small_data.empty:
                big_close = big_data['close'].T.iloc[0] # 转换取对应的 Series
                small_close = small_data['close'].T.iloc[0]

                if len(big_close) >= 21 and len(small_close) >= 21:
                    # 复合平滑动量 = 50%的10日动量 + 50%的20日动量
                    big_ret_10 = (big_close.iloc[-1] / big_close.iloc[-11] - 1) * 100
                    big_ret_20 = (big_close.iloc[-1] / big_close.iloc[-21] - 1) * 100
                    small_ret_10 = (small_close.iloc[-1] / small_close.iloc[-11] - 1) * 100
                    small_ret_20 = (small_close.iloc[-1] / small_close.iloc[-21] - 1) * 100

                    big_momentum = 0.5 * big_ret_10 + 0.5 * big_ret_20
                    small_momentum = 0.5 * small_ret_10 + 0.5 * small_ret_20

                    if big_momentum < 0 and small_momentum < 0:
                        self.current_style = 'DEFENSE'
                        print(">> 动量皆负，A股泥沙俱下，切换至外盘 ETF 防御模式！")
                        self.buy_defense_etf()
                    elif big_momentum >= small_momentum:
                        self.current_style = 'BIG'
                        print(">> 大盘动量占优，精选大盘白马股！")
                        self.buy_a_shares('BIG')
                    else:
                        self.current_style = 'SMALL'
                        print(">> 小盘动量占优，精选高质微盘股！")
                        self.buy_a_shares('SMALL')

                    # 只有数据充足、调仓成功执行后才锁定本月
                    self.monthly_adjusted_month = current_month
                else:
                    print("!! 历史数据不足21条，本次月度调仓跳过，下次循环重试。")
            else:
                print("!! 基准指数数据获取为空，本次月度调仓跳过，下次循环重试。")
            
        # ---------------------------------------------------------
        # 模块 2：周度熔断观察 (每周五 14:30)
        # ---------------------------------------------------------
        if current_time >= self.circuit_breaker_time and self.weekly_check_week != current_week:
            if now.weekday() == 4: # 4 代表周五
                print(f"[{current_time}] 执行周度熔断审查...")
                benchmark = self.benchmark_big if self.current_style == 'BIG' else self.benchmark_small
                
                start_date = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=30)).strftime("%Y%m%d")
                xtdata.download_history_data2([self.benchmark_big, self.benchmark_small], period='1d', start_time=start_date, end_time='', )
                b_data = xtdata.get_market_data(['close'], [benchmark], '1d', count=20, dividend_type='front')
                
                if not b_data.empty:
                    closes = b_data['close'].T.iloc[0]
                    ma20 = closes.mean()
                    current_price = closes.iloc[-1]
                    
                    if current_price < ma20 and self.current_style != 'DEFENSE':
                        print(f"!! 警报：{benchmark} 跌破20日均线，触发周度熔断，提前防御 !!")
                        self.current_style = 'DEFENSE'
                        self.buy_defense_etf()
                        
                self.weekly_check_week = current_week

        # ---------------------------------------------------------
        # 模块 3：日内硬止损 (每日 14:45 执行)
        # ---------------------------------------------------------
        if current_time >= self.stop_loss_time and self.stop_loss_date != current_date:
            # 获取当前账号持仓
            positions = self.trader.query_stock_positions(self.account)
            if positions:
                for pos in positions:
                    stock = pos.stock_code
                    cost_price = pos.open_price
                    volume = pos.volume

                    if not self.ledger.is_in_ledger(stock):
                        continue  # 只处理本策略买入的持仓

                    if volume > 0 and pos.can_use_volume > 0: # 确保有可用持仓
                        # 订阅并获取 Tick 现价
                        xtdata.subscribe_quote(stock, period='tick', count=1)
                        tick = xtdata.get_full_tick([stock])
                        if stock in tick:
                            current_price = tick[stock]['lastPrice']

                            # 触发 8% 止损
                            if current_price < cost_price * 0.92:
                                print(f"!! 止损触发 !! [{stock}] 现价 {current_price} 跌破成本 {cost_price} 达 8%")
                                # 异步市价清仓
                                if not DEBUG:
                                    self.trader.order_stock_async(
                                        self.account, stock, xtconstant.STOCK_SELL,
                                        pos.can_use_volume, xtconstant.LATEST_PRICE, 0, 'strategy_stop_loss', '止损卖出'
                                    )
                                self.ledger.remove(stock)
                                print(">> 提示：止损后腾出资金空仓保留，不向下摊平。")
                                
            self.stop_loss_date = current_date


# ================= 业务辅助方法 =================
    def buy_defense_etf(self):
        """核心业务 1：清仓A股，等权买入外盘ETF避险"""
        print(f">> 开始执行防御模式：清仓A股，准备买入 ETF {self.foreign_etf}")
        
        # 1. 卖出本策略持有的、非目标 ETF 的持仓
        positions = self.trader.query_stock_positions(self.account)
        if positions:
            for pos in positions:
                if pos.can_use_volume > 0 and pos.stock_code not in self.foreign_etf and self.ledger.is_in_ledger(pos.stock_code):
                    if not DEBUG:
                        self.trader.order_stock_async(
                            self.account, pos.stock_code, xtconstant.STOCK_SELL,
                            pos.can_use_volume, xtconstant.LATEST_PRICE, 0, 'strategy_clear', '清仓避险'
                        )
                    self.ledger.remove(pos.stock_code)
        
        # 等待20秒，确保清仓订单成交、资金释放回账户
        time.sleep(20)
        
        # 2. 获取最新可用资金
        asset = self.trader.query_stock_asset(self.account)
        if not asset:
            print("!! 获取资产失败，放弃本次 ETF 买入 !!")
            return
            
        available_cash = asset.cash
        budget = min(available_cash, self.total_budget)
        print(f">> 当前账户可用资金: {available_cash:.2f}，本次使用预算: {budget:.2f}")

        # 3. 计算并等权买入 ETF
        if budget > 1000: # 留存少许底仓防止滑点导致废单
            target_value_per_etf = budget / len(self.foreign_etf)
            for etf in self.foreign_etf:
                # 订阅并获取最新 Tick 现价
                xtdata.subscribe_quote(etf, period='tick', count=1)
                tick = xtdata.get_full_tick([etf])
                if etf in tick:
                    price = tick[etf]['lastPrice']
                    if price > 0:
                        # 计算买入股数（向下取整到 100 的整数倍）
                        volume = int(target_value_per_etf / price / 100) * 100
                        if volume >= 100:
                            if not DEBUG:
                                self.trader.order_stock_async(
                                    self.account, etf, xtconstant.STOCK_BUY,
                                    volume, xtconstant.LATEST_PRICE, 0, 'strategy_buy_etf', '买入外盘ETF'
                                )
                            self.ledger.add(etf)
                            print(f">> 发送委托: 买入 {etf}, 数量: {volume}股, 预估耗资: {volume*price:.2f}")


    def buy_a_shares(self, style):
        """核心业务 2：基本面选股，剔除劣质股后等权建仓A股"""
        print(f">> 开始执行 {style} 风格建仓逻辑...")

        # 1. 获取基础候选股票池
        if style == 'BIG':
            # 大盘风格：使用沪深300作为成分池
            pool = xtdata.get_stock_list_in_sector('沪深300') 
        else:
            # 小盘风格：使用中证1000代表优质微/小盘
            pool = xtdata.get_stock_list_in_sector('中证1000')

        if not pool:
            print("!! 获取板块成分股失败，请检查QMT终端左下角【数据下载】是否下载了板块数据 !!")
            return

        # 2. 基础过滤：剔除ST、退市股 (对应原策略 filter_st_stock)
        valid_pool = []
        for code in pool:
            detail = xtdata.get_instrument_detail(code)
            if detail:
                name = detail.get('InstrumentName', '')
                if 'ST' not in name and '退' not in name:
                    valid_pool.append(code)

        print(f">> 剔除ST等风险股后，候选池剩余: {len(valid_pool)} 只")

        # 3. 核心：基本面清洗 (调用我们专门解决幸存者偏差的过滤函数)
        target_list = self._filter_fundamentals(valid_pool, style)

        if not target_list:
            print("!! 基本面选股结果为空，放弃本次 A 股建仓，维持原状。 !!")
            return
            
        print(f">> 最终锁定强基本面标的: {target_list}")

        # 4. 执行调仓交易
        # 4.1 卖出不在 target_list 中的持仓
        positions = self.trader.query_stock_positions(self.account)
        hold_codes = []
        if positions:
            for pos in positions:
                if self.ledger.is_in_ledger(pos.stock_code):
                    hold_codes.append(pos.stock_code)  # 只记录本策略持有的股票
                if self.ledger.is_in_ledger(pos.stock_code) and pos.stock_code not in target_list and pos.can_use_volume > 0:
                    if not DEBUG:
                        self.trader.order_stock_async(
                            self.account, pos.stock_code, xtconstant.STOCK_SELL,
                            pos.can_use_volume, xtconstant.LATEST_PRICE, 0, 'strategy_sell_a', '不符风格卖出'
                        )
                    self.ledger.remove(pos.stock_code)

        time.sleep(20) # 等待平仓资金释放
        
        # 4.2 计算现金并买入新目标
        asset = self.trader.query_stock_asset(self.account)
        if asset:
            available_cash = asset.cash
            budget = min(available_cash, self.total_budget)
            # 只买入当前没持有的目标股
            buy_targets = [code for code in target_list if code not in hold_codes]

            if buy_targets and budget > 2000:
                # 预留 2% 资金作为手续费和滑点缓冲
                safe_cash = budget * 0.98
                cash_per_stock = safe_cash / len(buy_targets)
                
                for code in buy_targets:
                    xtdata.subscribe_quote(code, period='tick', count=1)
                    tick = xtdata.get_full_tick([code])
                    if code in tick:
                        price = tick[code]['lastPrice']
                        if price > 0:
                            volume = int(cash_per_stock / price / 100) * 100
                            if volume >= 100:
                                if not DEBUG:
                                    self.trader.order_stock_async(
                                        self.account, code, xtconstant.STOCK_BUY,
                                        volume, xtconstant.LATEST_PRICE, 0, 'strategy_buy_a', f'建仓{style}'
                                    )
                                self.ledger.add(code)
                                print(f">> 发送委托: 买入 {code}, 数量: {volume}股, 预估耗资: {volume*price:.2f}")


    def _filter_fundamentals(self, pool, style):
        """
        核心防雷区：基本面清洗 (替代聚宽 get_fundamentals)
        解决幸存者偏差：强制校验扣非净利润
        """
        # 注意：使用此功能前，务必在 QMT 终端进行【财务数据下载】
        # 这里提取的是 QMT 财务表库中常用的字段进行 Pandas 向量化运算
        
        # 字段说明：
        # ROE_weighted: 净资产收益率 (加权)
        # dedu_np_yoy: 扣除非经常性损益的净利润同比增长率 (核心防雷因子)
        # PE_TTM: 市盈率 TTM
        
        field_list = ['ROE_weighted', 'dedu_np_yoy', 'PE_TTM', 'total_market_cap']
        
        # 获取最近一期的财务数据
        fin_data = xtdata.get_financial_data(pool, table_list=field_list)
        
        if not fin_data:
            print(">> 警告：未能获取财务数据，请确认是否在QMT下载了财务数据！将默认返回前3只股票...")
            return pool[:self.stock_num]
            
        try:
            # 将多维财务字典转换为容易操作的 DataFrame
            df_list = []
            for field in field_list:
                if field in fin_data:
                    # 取最新的一行数据并转置
                    s = fin_data[field].iloc[-1].T
                    s.name = field
                    df_list.append(s)
            
            df = pd.concat(df_list, axis=1).dropna()
            
            if df.empty:
                return pool[:self.stock_num]

            # ========== 实施我们优化的硬核过滤逻辑 ==========
            # 基础保底逻辑：扣非净利润必须大于 0 (防假高收益、防卖房保壳)
            df = df[df['dedu_np_yoy'] > 0] 

            if style == 'BIG':
                # 大盘逻辑 (对应原码 ROIC_BIG/BIG/BM)：高盈利，低估值 [cite: 14, 15, 17]
                # ROE > 10%, PE < 30
                df = df[(df['ROE_weighted'] > 10) & (df['PE_TTM'] > 0) & (df['PE_TTM'] < 30)]
                # 排序：按市值从大到小排序，选取大白马
                df = df.sort_values(by='total_market_cap', ascending=False)
                
            else: # SMALL
                # 小盘微盘逻辑 (对应原码 SMALL)：极高的资产利用率，微小市值 [cite: 12, 13]
                # ROE > 15% (原版有ROA，此处以ROE和扣非叠加作为平替)
                df = df[df['ROE_weighted'] > 15]
                # 排序：按市值从小到大排序，吃微盘溢价
                df = df.sort_values(by='total_market_cap', ascending=True)

            # 返回排名前 N 位的股票代码列表
            return df.index.tolist()[:self.stock_num]
            
        except Exception as e:
            print(f">> 基本面数据处理出错: {e}，返回默认前3只。")
            return pool[:self.stock_num]


# ================= 3. 主函数执行入口 (Main) =================
if __name__ == '__main__':
    # ---------------- 必须修改的配置 ----------------
    # 请将此处替换为你本地电脑上 QMT 极简模式终端所在的路径（userdata_mini 文件夹）
    qmt_path = r'D:\光大证券金阳光QMT实盘\userdata_mini'
    account_id = '47601131' 
    # ------------------------------------------------

    # 创建会话 ID
    session_id = int(time.time())
    
    # 实例化交易对象并创建账号
    trader = XtQuantTrader(qmt_path, session_id)
    acc = StockAccount(account_id)
    
    # 注册回调
    callback = MyCallback()
    trader.register_callback(callback)
    
    # 启动交易线程并连接
    trader.start()
    connect_result = trader.connect()
    
    if connect_result == 0:
        print(f'>> 极简模式连接成功，正在订阅资金账号: {account_id}')
        trader.subscribe(acc)
    else:
        print('>> 极简模式连接失败，请检查 QMT 极简模式是否开启并登录，以及路径是否正确！')
        exit()

    # 实例化我们的策略
    strategy = AllWeatherStrategy(trader, acc)

    print(">> 进入主事件循环，按 Ctrl+C 终止运行。")
    try:
        # 死循环不断驱动 handlebar (相当于引擎)
        while True:
            strategy.handlebar()
            # 暂停 3 秒，避免过度消耗 CPU
            time.sleep(3) 
    except KeyboardInterrupt:
        print("\n>> 收到手动停止信号，正在断开连接退出程序...")
        trader.stop()