# -*- coding: utf-8 -*-
import time
import datetime
from datetime import timezone, timedelta
import sqlite3
import pandas as pd
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
import xtquant.xtconstant as xtconstant
import os, json

# ================= 1. 全局配置与参数 =================
BEIJING_TZ = timezone(timedelta(hours=8))
class Config:
    account_id = '47601131'  # 您的资金账号
    mini_qmt_path =  r'D:\光大证券金阳光QMT实盘\userdata_mini'  # 【必改】极简模式客户端安装路径
    db_path = r'C:\Users\xiusan\OneDrive\Investment\Quant_data\stock_data.db' # 【必改】本地SQLite数据库路径
    blacklist_path = 'blacklist.json'
    
    # 策略核心参数
    pass_months = [1, 4]             # 空仓的月份 (规避年报、一季报披露期爆雷)
    etf = '511880.SH'                # 空仓月份持有的银华日利ETF
    base_stock_num = 4               # 基础持仓股票数量
    stoploss_limit = 0.09            # 个股止损线 9%
    stoploss_market = 0.05           # 市场大跌止损线 5%
    index_code = '000300.SH'         # 参考大盘指数改为沪深300
    
    # 选股过滤参数
    min_market_cap = 10_0000_0000    # 最小市值：10亿 (原代码中的10个亿)
    max_price = 50                   # 股票单价上限设置

def load_blacklist():
    """从本地 JSON 文件读取小黑屋数据"""
    if os.path.exists(Config.blacklist_path):
        try:
            with open(Config.blacklist_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"--> 成功从本地恢复小黑屋记忆，当前黑名单包含 {len(data)} 只股票。")
                return data
        except Exception as e:
            print(f"--> 读取小黑屋文件失败: {e}，将初始化为空。")
    return {}
def save_blacklist(blacklist_dict):
    """将小黑屋数据保存到本地 JSON 文件"""
    try:
        with open(Config.blacklist_path, 'w', encoding='utf-8') as f:
            json.dump(blacklist_dict, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"--> 保存小黑屋文件失败: {e}")

class GlobalVar:
    target_list = []                 # 今日目标持仓
    stock_num = Config.base_stock_num
    market_crash = False             # 大盘是否暴跌
    # 【新增：止损冷却小黑屋】记录止损股票和日期，格式: {'600000.SH': datetime.date(2026, 3, 7)}
    blacklist = load_blacklist()                   

# ================= 2. 交易回调与状态管理 =================
class MyCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        print("警告：交易服务器连接断开！")
    
    def on_stock_order(self, order):
        print(f"订单更新: {order.stock_code}, 状态: {order.order_status_msg}, 成交均价: {order.traded_price}, 成交量: {order.traded_volume}")
        
    def on_stock_trade(self, trade):
        print(f"成交回报: {trade.stock_code}, 数量: {trade.traded_volume}, 价格: {trade.traded_price}")

# ================= 3. 核心选股与信号模块 =================
def get_market_trend_stock_num():
    """动态仓位控制：根据沪深300与10日均线的偏离度(乖离率)决定持仓数量"""
    # 往前推 40 个自然日，绝对保证覆盖 20 个交易日
    start_date = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=40)).strftime("%Y%m%d")
    xtdata.download_history_data2([Config.index_code], period='1d', start_time=start_date, end_time='')
    df = xtdata.get_market_data_ex(['close'], [Config.index_code], period='1d', count=20, dividend_type='front')[Config.index_code]
    
    if df.empty or len(df) < 10:
        return Config.base_stock_num
        
    df['ma10'] = df['close'].rolling(window=10).mean()
    last_close = df['close'].iloc[-1]
    last_ma = df['ma10'].iloc[-1]
    
    # 计算乖离率百分比 (Bias Ratio)
    diff_pct = (last_close - last_ma) / last_ma
    
    if diff_pct >= 0.05:             return 3
    elif 0.02 <= diff_pct < 0.05:    return 3
    elif -0.02 <= diff_pct < 0.02:   return 4
    elif -0.05 <= diff_pct < -0.02:  return 5
    else:                            return 6    

def get_fundamental_pool(limit=10):
    """从SQLite获取高分红/基本面达标股票，结合QMT进行市值、价格、ST排雷"""
    print("开始从本地数据库及QMT进行基本面选股...")
    
    # 1. 连接本地数据库
    conn = sqlite3.connect(Config.db_path)
    query = """
        SELECT d.qmt_code, d.[现金分红-股息率], f.净资产收益率, f.[净利润-净利润], f.[营业总收入-营业总收入], d.总股本
        FROM dividend_data d
        JOIN financial_report f ON d.qmt_code = f.qmt_code
        WHERE f.[净利润-净利润] > 0 
          AND f.净资产收益率 > 0
          AND f.[营业总收入-营业总收入] > 100000000  -- 营收大于1亿，剔除空壳
          AND d.[现金分红-股息率] > 0
          AND d.qmt_code NOT LIKE '30%'   -- 剔除创业板
          AND d.qmt_code NOT LIKE '68%'   -- 剔除科创板
          AND d.qmt_code NOT LIKE '%.BJ'  -- 剔除北交所
        ORDER BY d.[现金分红-股息率] DESC
        LIMIT 60
    """
    try:
        df_pool = pd.read_sql(query, conn)
    except Exception as e:
        print(f"读取数据库失败: {e}")
        conn.close()
        return []
    finally:
        conn.close()

    candidate_stocks = df_pool['qmt_code'].tolist()
    if not candidate_stocks:
        print("本地数据库未筛选出符合条件的股票！")
        return []

    # 2. QMT 原生实时排雷与量价过滤
    final_target_pool = []
    
    # 【修正处 1：个股初筛行情下载】往前推10个自然日，确保覆盖5个交易日
    start_date_stock = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=10)).strftime("%Y%m%d")
    xtdata.download_history_data2(candidate_stocks, period='1d', start_time=start_date_stock, end_time='')
    
    market_data = xtdata.get_market_data_ex(['close', 'amount'], candidate_stocks, period='1d', count=5)
    
    for _, row in df_pool.iterrows():
        stock = row['qmt_code']
        
        # =======================================================
        # 【新增：检查是否在止损冷却小黑屋中】
        # =======================================================
        if stock in GlobalVar.blacklist:
            # 严格使用 BEIJING_TZ 保持时区一致
            ban_date_str = GlobalVar.blacklist[stock]
            ban_date = datetime.datetime.strptime(ban_date_str, "%Y-%m-%d").date()
            days_banned = (datetime.datetime.now(BEIJING_TZ).date() - ban_date).days
            if days_banned < 30:  # 30天内不允许再买
                print(f"    -> [风控拦截] 跳过 {stock}，触发止损后目前在 30 天冷却期内。")
                continue
            else:
                # 满30天了，刑满释放
                print(f"    -> [风控释放] {stock} 止损冷却期已满 30 天，移除黑名单。")
                del GlobalVar.blacklist[stock]
                save_blacklist(GlobalVar.blacklist)

        total_share = row['总股本']
        
        detail = xtdata.get_instrument_detail(stock)
        if detail:
            name = detail.get('InstrumentName', '')
            if 'ST' in name or '退' in name:
                continue
                
        if stock in market_data and not market_data[stock].empty:
            df = market_data[stock]
            latest_price = df['close'].iloc[-1]
            avg_amount = df['amount'].mean()
            
            market_cap = total_share * latest_price if pd.notna(total_share) else 0
            
            if (latest_price <= Config.max_price and 
                market_cap >= Config.min_market_cap and 
                avg_amount > 20000000):  
                
                final_target_pool.append(stock)
                
        if len(final_target_pool) >= limit:
            break
            
    print(f"最终选股结果: {final_target_pool}")
    return final_target_pool

def get_tolerant_target_list(trader, account, target_num, tolerance_pool_size=10):
    """
    获取带有“排名宽容度”的最终目标持仓名单
    :param trader: 交易对象
    :param account: 资金账号
    :param target_num: 目标持仓数量 (即 GlobalVar.stock_num)
    :param tolerance_pool_size: 宽容池大小 (建议设置为目标数量的2-3倍)
    """
    # 1. 获取当前真实持仓的股票代码（剔除银华日利ETF和空仓）
    positions = trader.query_stock_positions(account)
    current_holdings = [
        pos.stock_code for pos in positions 
        if pos.volume > 0 and pos.stock_code != Config.etf
    ]

    # 2. 从数据库获取放宽后的候选池 (排名前 tolerance_pool_size 的股票)
    # 注意：你需要确保你的 get_fundamental_pool 函数能够接收 limit 参数
    candidate_pool = get_fundamental_pool(limit=tolerance_pool_size) 
    
    target_list = []
    
    # 3. 优先保留老将：只要当前持仓在候选池(前15名)中，就继续保留在目标名单中
    for stock in current_holdings:
        if stock in candidate_pool:
            target_list.append(stock)
            
    # 4. 填补空缺：如果当前达标的老将数量不足 target_num，则从候选池中最优秀的开始递补
    for stock in candidate_pool:
        if len(target_list) >= target_num:
            break  # 名额已满，停止递补
        if stock not in target_list:
            target_list.append(stock)
            
    return target_list
# ================= 4. 交易与风控模块 =================

def check_stop_loss(trader, account):
    """【风控】个股止盈止损与大盘趋势止损"""
    positions = trader.query_stock_positions(account)
    if not positions:
        return
        
    # 1. 检查大盘暴跌系统性风险
    # 【修正处 2：止损时的大盘行情下载】往前推 5 天，确保覆盖昨今 2 天
    start_date_idx = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=5)).strftime("%Y%m%d")
    xtdata.download_history_data2([Config.index_code], period='1d', start_time=start_date_idx, end_time='')
    idx_df = xtdata.get_market_data_ex(['close', 'open'], [Config.index_code], period='1d', count=1)[Config.index_code]
    
    if not idx_df.empty:
        down_ratio = (idx_df['close'].iloc[-1] / idx_df['open'].iloc[-1]) - 1
        if down_ratio <= -Config.stoploss_market:
            print(f"大盘跌幅 {down_ratio:.2%} 触发止损!")
            GlobalVar.market_crash = True

    # 2. 检查个股
    for pos in positions:
        if pos.can_use_volume == 0 or pos.stock_code == Config.etf:
            continue
            
        cost = pos.open_price
        current_price = pos.market_value / pos.volume if pos.volume > 0 else 0
        
        if GlobalVar.market_crash:
            order_target_volume(trader, account, pos.stock_code, 0, current_price, 'market_crash_sell')
            continue
            
        if current_price >= cost * 2:
            print(f"[{pos.stock_code}] 盈利100%触发止盈")
            order_target_volume(trader, account, pos.stock_code, 0, current_price, 'take_profit')
            
        elif current_price < cost * (1 - Config.stoploss_limit):
            print(f"[{pos.stock_code}] 跌幅超9%触发止损，关进 30 天小黑屋！")
            # 原有的加入黑名单逻辑（记录当天的日期）
            today_str = datetime.datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
            GlobalVar.blacklist[pos.stock_code] = today_str
            
            # 【新增这一行】：立刻把更新后的小黑屋写进硬盘！
            save_blacklist(GlobalVar.blacklist)

            order_target_volume(trader, account, pos.stock_code, 0, current_price, 'stop_loss')

def adjust_positions(trader, account, target_list):
    """【目标调仓】对比当前持仓，执行卖出和买入，实现等权重调仓"""
    positions = trader.query_stock_positions(account)
    hold_list = [p.stock_code for p in positions if p.volume > 0]
    
    has_sell_order = False  # 新增标签
    # 1. 卖出不在目标列表中的股票
    for p in positions:
        if p.volume > 0 and p.stock_code not in target_list:
            print(f"调仓卖出: {p.stock_code}")
            if order_target_volume(trader, account, p.stock_code, 0, 0, 'rebalance_sell'):
                has_sell_order = True
            
    if has_sell_order:
        print("已发送卖出指令，等待 120 秒确认成交释放资金...")
        time.sleep(120) 

    #重新获取 positions
    positions = trader.query_stock_positions(account)
    hold_list = [p.stock_code for p in positions if p.volume > 0]
    # 2. 计算买入新标的的可用预算
    # 计算本次调仓中，继续保留的老股票的当前总市值
    retained_value = sum(
        (p.open_price * p.volume) for p in positions 
        if p.volume > 0 and p.stock_code in target_list
    )
    
    # 策略总额度 60000 扣除保留老股票的市值，得出可用于买新股的预算
    # 使用 max(0, ...) 防止老股票盈利太多导致预算变成负数
    target_total_capital = 60000
    budget_for_new = max(0, target_total_capital - retained_value)
    
    # 获取账户实际可用资金（防止账户本身没钱了）
    asset = trader.query_stock_asset(account)
    
    # 最终可用的买入资金 = 取“新股剩余预算”和“账户真实闲置资金”两者的较小值
    available_cash = min(budget_for_new, asset.cash)

    buy_list = [s for s in target_list if s not in hold_list]
    
    if not buy_list or available_cash < 1000:
        return
        
    cash_per_stock = available_cash / len(buy_list)
    
    # 【修正处 3：买入前的新标的行情下载】往前推 5 天，确保覆盖最新 1 天
    start_date_buy = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=5)).strftime("%Y%m%d")
    for stock in buy_list:
        xtdata.download_history_data2([stock], period='1d', start_time=start_date_buy, end_time='')
        price_df = xtdata.get_market_data_ex(['close'], [stock], period='1d', count=1)
        if stock in price_df and not price_df[stock].empty:
            price = price_df[stock]['close'].iloc[-1]
            volume = int(cash_per_stock / price / 100) * 100 # 向下取整到整百股
            if volume > 0:
                print(f"--> [发送订单] 动作: 买入 | 代码: {stock} | 数量: {volume}股 | 挂单价: {price} | 业务: rebalance_buy")
                # trader.order_stock(account, stock, xtconstant.STOCK_BUY, volume, xtconstant.LATEST_PRICE, price, 'strategy', 'rebalance_buy')

def order_target_volume(trader, account, stock_code, target_vol, price, remark='adjust'):
    """基础辅助函数：下单直到满足目标股数"""
    positions = trader.query_stock_positions(account)
    current_vol = 0
    can_use_vol = 0
    for p in positions:
        if p.stock_code == stock_code:
            current_vol = p.volume
            can_use_vol = p.can_use_volume  # 获取实际可用(可卖)股数
            break
            
    diff = target_vol - current_vol
    if diff > 0:
        print(f"--> [发送订单] 动作: 买入 | 代码: {stock_code} | 数量: {diff}股 | 挂单价: {price} | 业务: {remark}")
        # trader.order_stock(account, stock_code, xtconstant.STOCK_BUY, diff, xtconstant.LATEST_PRICE, price, 'strategy', remark) 
        return True
    elif diff < 0:
        sell_vol = min(abs(diff), can_use_vol)
        if sell_vol > 0:
            print(f"--> [发送订单] 动作: 卖出 | 代码: {stock_code} | 数量: {sell_vol}股 | 挂单价: {price} | 业务: {remark}")
            #trader.order_stock(account, stock_code, xtconstant.STOCK_SELL, sell_vol, xtconstant.LATEST_PRICE, price, 'strategy', remark)
            return True
        else:
            print(f"--> [忽略请求] {stock_code} 需卖出，但可用额度为 0 (可能是 T+1 锁仓或在途冻结)。")
            return False
    return False

# ================= 5. 定时任务主循环 =================

def run_strategy():
    # 初始化交易接口
    session_id = int(time.time())
    trader = XtQuantTrader(Config.mini_qmt_path, session_id)
    account = StockAccount(Config.account_id)
    
    trader.register_callback(MyCallback())
    trader.start()
    trader.connect()
    trader.subscribe(account)
    print("====== QMT 交易接口连接成功，策略启动 ======")

    # 每日任务执行标记
    task_done = {'09:05': False, '10:00': False, '14:00': False}
    
    while True:
        now = datetime.datetime.now(BEIJING_TZ)
        time_str = now.strftime("%H:%M")
        
        # 午夜重置任务标记
        if time_str == "00:00":
            for k in task_done.keys(): task_done[k] = False
            GlobalVar.market_crash = False
            time.sleep(60)

        # 09:05 盘前准备：测算大盘趋势并更新仓位数量
        if DEBUG or (time_str == "09:05" and not task_done['09:05']):
            GlobalVar.stock_num = get_market_trend_stock_num()
            print(f"[{time_str}] 今日大盘趋势运算完成，计划持仓股数: {GlobalVar.stock_num}")
            task_done['09:05'] = True

        # 10:00 调仓时刻：风控检查 + 根据月份与基本面选股池调仓
        if DEBUG or (time_str == "10:00" and not task_done['10:00']):
            # 每天 10:00 都检查止损
            check_stop_loss(trader, account)
            
            # 判断今天是不是周一 (weekday() == 0 代表周一)
            is_rebalance_day = (now.weekday() == 0)
            
            if now.month in Config.pass_months:
                print(f"[{time_str}] 当前为规避月份({now.month}月)，空仓防雷，买入 ETF。")
                adjust_positions(trader, account, [Config.etf])
            elif is_rebalance_day and not GlobalVar.market_crash:
                # 只有周一且大盘未暴跌时，才进行基本面选股和调仓
                print(f"[{time_str}] 今日是调仓日, 开始评估持仓排名与宽容度...")
                # 调用带有宽容度的选股逻辑
                # GlobalVar.stock_num 是通过大盘均线计算出的动态目标仓位数
                GlobalVar.target_list = get_tolerant_target_list(
                    trader, 
                    account, 
                    target_num=GlobalVar.stock_num, 
                    tolerance_pool_size=10
                )
                adjust_positions(trader, account, GlobalVar.target_list)
            else:
                print(f"[{time_str}] 今日非调仓日，仅执行风控监控。")
                
            task_done['10:00'] = True

        # 14:00 下午风控：再次检查系统暴跌或个股止损
        if DEBUG or (time_str == "14:00" and not task_done['14:00']):
            check_stop_loss(trader, account)
            task_done['14:00'] = True

        # 防止高频死循环占用CPU
        time.sleep(1) 


DEBUG = False
if __name__ == '__main__':
    run_strategy()

#Todo: 自己买入的股票的黑名单，以及T+1限制导致可用资金计算错误的bug