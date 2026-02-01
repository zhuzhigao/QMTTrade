# 克隆自聚宽文章：https://www.joinquant.com/post/65739
# 标题：【讨论帖】顶级融合连续涨2025年收益707%，防未来
# 作者：烟花三月zy

from jqdata import *  # 导入聚宽数据API
from jqfactor import *  # 导入聚宽因子API
import numpy as np  # 导入numpy库，用于数值计算
import pandas as pd  # 导入pandas库，用于数据处理
from datetime import datetime, time, timedelta  # 导入日期时间相关模块
from scipy import stats  # 导入scipy统计模块
import math  # 导入数学模块
from collections import defaultdict  # 导入默认字典模块

# ======================================
# 全局变量统一初始化（整合A、B方案，避免冲突）
# ======================================
def initialize(context):  # 初始化函数，设置策略参数
    # --------------------------
    # A方案（股票策略）初始化
    # --------------------------
    # 开启防未来函数
    set_option('avoid_future_data', True)  # 设置避免使用未来数据
    # 设定基准（A方案：中小板指）
    set_benchmark('399101.XSHE')  # 设置策略基准为中小板指数
    # 用真实价格交易
    set_option('use_real_price', True)  # 设置使用真实价格进行回测
    # 股票交易滑点和手续费
    set_slippage(PriceRelatedSlippage(0.002), type="stock")  # 设置股票交易滑点为0.2%
    set_order_cost(  # 设置股票交易成本
        OrderCost(  # 定义订单成本结构
            open_tax=0,  # 开仓税费为0
            close_tax=0.0005,  # 平仓印花税为0.05%
            open_commission=0.0001,  # 开仓佣金为0.01%
            close_commission=0.0001,  # 平仓佣金为0.01%
            close_today_commission=0,  # 当日平仓佣金为0
            min_commission=1,  # 最低佣金为1元
        ),
        type="stock",  # 类型为股票
    )
    # ETF（B方案）交易成本（单独设置，与股票区分）
    set_order_cost(OrderCost(open_commission=0.0001, close_commission=0.0001, min_commission=0.1), type='fund')  # 设置ETF交易成本
    # ETF滑点设置
    set_slippage(FixedSlippage(0.000), type='fund')  # 设置ETF固定滑点为0

    # 过滤日志级别
    log.set_level('order', 'error')  # 设置订单日志级别为错误
    log.set_level('system', 'error')  # 设置系统日志级别为错误
    log.set_level('strategy', 'debug')  # 设置策略日志级别为调试

    # A方案：布尔型全局变量
    g.no_trading_today_signal = False  # 恒为False，不触发空仓
    g.run_stoploss = True  # 是否进行止损
    g.run_tech_stoploss = True  # 是否启用技术面止损
    g.tech_stoploss_triggered = False  # 技术面止损触发标记
    g.market_type = ""  # 市场类型：bull(牛)/shock_bull(震荡看涨)/bear(熊)
    g.shock_bull_flag = False  # 震荡市是否看涨
    g.buy_allowed_flag = False  # 是否允许买入（牛市/震荡看涨时为True）
    g.HV_control = False # 是否日频判断放量
    g.no_trading_hold_signal = False  # 不交易持有信号
    g.reason_to_sell = ''  # 卖出原因

    # A方案：列表型全局变量
    g.hold_list = [] # A方案当前持仓股票
    g.yesterday_HL_list = [] # 记录昨日涨停的股票
    g.stock_last_close = {}  # 缓存股票昨日收盘价（0轴参考）
    g.industry_trend = {}  # 缓存各板块趋势（看涨/中性/看跌）
    g.target_list = [] # A方案目标股票列表
    g.not_buy_again = [] # A方案禁止重复买入股票
    g.no_trading_buy = []  # A方案保留变量
    g.stockL = [] # A方案保留变量

    # A方案：核心配置（只持有1只最优股票）
    g.stock_num = 1  # 持仓股票数量为1
    g.up_price = 20  # 股票单价上限20元
    g.limit_days_window = 3 * 250 # 历史涨停参考窗口期（3年）
    g.init_stock_count = 1000 # 初始股池数量1000只
    g.stoploss_strategy = 3  # 1=止损线，2=市场趋势，3=联合止损
    g.stoploss_limit = 0.93  # 个股止损线93%
    g.stoploss_market = 0.93  # 市场趋势止损参数93%

    # A方案：板块配置
    g.industry_level = "sw2"  # 申万二级行业（板块划分标准）
    g.industry_ma_short = 5    # 板块短期均线（判断短期趋势）5日
    g.industry_ma_long = 20    # 板块长期均线（判断中长期趋势）20日
    g.industry_trend_valid_stock = 3  # 板块有效成分股数量下限3只

    # A方案：技术分析参数
    g.tech_ma_short = 5    # 短期均线：5日均线
    g.tech_ma_mid = 10     # 中期均线：10日均线
    g.tech_ma_long = 14    # 长期均线：14天
    g.tech_macd_fast = 12  # MACD快速线周期12
    g.tech_macd_slow = 20  # MACD慢速线周期20
    g.tech_macd_signal = 9 # MACD信号线周期9
    g.tech_kdj_period = 9  # KDJ周期9
    g.tech_boll_period = 14 # 布林带周期：14天
    g.tech_boll_dev = 2.0  # 布林带标准差倍数2倍
    g.tech_indicators = ["ma", "macd", "kdj", "boll"] # 纳入判断的技术指标
    g.tech_weights = {  # 技术指标权重配置
        "ma": 0.3,    # 均线权重最高30%
        "macd": 0.3,  # MACD权重次之30%
        "kdj": 0.2,   # KDJ权重中等20%
        "boll": 0.2   # 布林带权重较低20%
    }
    g.tech_bearish_threshold = 0.3 # 加权看跌阈值0.7

    # A方案：上证指数相关参数
    g.tech_stoploss_index = "000001.XSHG"  # 上证指数代码
    g.tech_cum_drop_days = 3          # 技术面累计跌幅天数7天
    g.tech_cum_drop_threshold = -2.0  # 技术面累计跌幅阈值-8%
    g.tech_ma_data = None  # 技术面均线数据
    g.tech_macd_data = None  # 技术面MACD数据
    g.index_ma5 = 5      # 上证指数 5日均线
    g.index_ma10 = 10    # 上证指数 10日均线
    g.index_ma20 = 14    # 上证指数 14日均线

    # A方案：放量相关参数
    g.HV_duration = 120*2 # 放量判断周期240天
    g.HV_ratio = 0.85    # 放量判断阈值85%

    # --------------------------
    # B方案（ETF动量策略）初始化
    # --------------------------
    g.m_days = 21  # B方案：动量计算天数21天
    g.max_score = 6  # B方案：动量分数上限阈值6
    g.min_score = 0  # B方案：动量分数下限阈值0
    g.score_threshold_multiplier = 1.1  # 动量分数增长阈值倍数
    g.max_observed_score = float('-inf') # B方案：最大观察分数（调试用）

    # 用于调试的变量
    g.max_observed_score = float('-inf')
    g.yesterday_scores = {}  # 存储前一天的ETF分数

    # B方案：ETF池设置
    g.etf_pool = [  # ETF池列表
        '518880.XSHG',  # 黄金ETF
        '161226.XSHE',  # (白银JJ)
        '501018.XSHG',  # 南方原油etf
        '159985.XSHE',  # 豆粕etf
        '513520.XSHG',  # 日经ETF
        '513100.XSHG',  # 纳指100
        '513300.XSHG',  # (纳斯达克ETF)
        '513400.XSHG',  # (道琼斯)
        '159529.XSHE',  # (标普消费ETF)
        '513030.XSHG',  # (德国30)
        '159329.XSHE',  # (沙特ETF)
        '513020.XSHG',  # 港股科技etf
        '513130.XSHG',  # 恒生科技ETF
        '513090.XSHG',  # (香港证券etf)
        '513120.XSHG',  # (香港创新药)
        '159206.XSHE',  # (卫星ETF).
        '159218.XSHE',  # (卫星产业ETF)
        '159227.XSHE',  # (航天航空ETF)
        '159565.XSHE',  # (汽车零部件ETF)
        '562500.XSHG',  # (机器人)
        '159819.XSHE',  # (人工智能)
        '159363.XSHE',  # (创业板人工智能TFHB)
        '512480.XSHG',  # (半导体)
        '512760.XSHG',  # (存储芯片)
        '515880.XSHG',  # (通信ETF)
        '515050.XSHG',  # (5GETF)
        '159786.XSHE',  # (VRETF)
        '159890.XSHE',  # (云计算ETF)
        '516160.XSHG',  # (新能源)
        '515790.XSHG',  # (光伏ETF)
        '159755.XSHE',  # (电池ETF)
        '512660.XSHG',  # (军工ETF)
        '159732.XSHE',  # (消费电子)
        '159992.XSHE',  # (创新药XY)
        '159852.XSHE',  # (软件ETF)
        '159851.XSHE',  # (金融科技ETF)
        '159869.XSHE',  # (游戏ETF)
        '516780.XSHG',  # (稀土ETF)
        '159928.XSHE',  # (消费ETF)
        '512690.XSHG',  # (酒ETF)
        '515170.XSHG',  # (食品饮料ETF)
        '512010.XSHG',  # (医药ETF)
        '512980.XSHG',  # (传媒ETF)
        '159378.XSHE',  # (通用航空ETF)
        '159611.XSHE',  # (电力ETF)
        '159766.XSHE',  # (旅游ETF)
        '515220.XSHG',  # (煤炭ETF)
        '159865.XSHE',  # (养殖ETF)
        '562800.XSHG',  # (稀有金属)
        '510050.XSHG',  # 上证50etf
        '510300.XSHG',  # 沪深300etf
        '159922.XSHE',  # 中证500etf
        '159531.XSHE',  # 中证2000ETF
        '159915.XSHE',  # 创业板etf
        '588080.XSHG',  #(科创板50)
        '588380.XSHG',  # (双创50ETF)
        '160211.XSHE',  # 国泰小盘
        '512000.XSHG',  # 券商ETF
        '512800.XSHG',  # 银行ETF
        '510880.XSHG',  # 红利ETF
        '511090.XSHG',  # 30年国债ETF
    ]
    g.safe_haven_etf = '160513.XSHE'  # B方案：避险资产ETF

    # --------------------------
    # 新增：防反复买卖标记
    # --------------------------
    g.last_traded_stock = None  # 上次交易的股票
    g.last_traded_time = None   # 上次交易时间
    g.trading_cooldown = 15     # 交易冷却时间（分钟）
    
    # --------------------------
    # 新增：交易状态记录
    # --------------------------
    g.b_935_rebalance_executed = False  # B方案9:40调仓是否已执行
    g.b_935_target_etf = None           # B方案9:40目标ETF
    g.b_935_target_value = 0            # B方案9:40目标持仓值
    g.b_935_executed_successfully = False  # 9:40交易是否成功执行

    # --------------------------
    # 统一定时任务配置（先A后B，核心：判断A空仓后执行B）
    # --------------------------
    # A方案定时任务
    run_daily(reset_tech_stoploss_flag, '9:00')  # 每日9:00重置技术面止损标记
    run_daily(calc_tech_indicators, '9:02')  # 每日9:02计算技术面指标
    run_daily(calc_csi1000_market_type, '9:03')  # 每日9:03计算市场类型
    run_daily(record_stock_last_close, '9:06')  # 每日9:06记录股票昨日收盘价
    run_daily(calc_industry_trend, '9:07')  # 每日9:07计算板块趋势
    run_daily(prepare_stock_list, '9:05')  # 每日9:05准备股票列表
    run_weekly(weekly_adjustment,2,'09:35')  # 每周二9:40进行周度调仓
    run_daily(sell_stocks, time='09:35')  # 每日9:40执行股票卖出
    run_daily(execute_tech_stoploss, time='09:35')  # 每日9:40执行技术面止损
    run_daily(check_limit_up_and_zero_axis, time='09:35')  # 每日9:40检查涨停及零轴

    run_daily(close_account, '15:00')  # 每日9:40收盘

    # 核心调度任务：先执行A方案，再判断是否执行B方案
    run_daily(check_rebalance, '09:35')  # 每日9:40检查A方案持仓并执行B方案
    # B方案检查任务（可选，用于二次确认）
    run_daily(check_rebalance, '10:45')  # 每日10:45检查9:40是否交易成功，未成功则重新交易
    # 账户日志记录
    run_daily(log_portfolio_info, '15:30')  # 每日9:40记录账户信息

    log.info("A+B方案汇总初始化完成，优先执行A股票策略，A空仓时自动执行B ETF策略")  # 输出初始化完成日志

# ======================================
# 核心调度函数：判断A方案是否空仓，空仓则执行B方案
# ======================================
def check_a_position_and_run_b(context):  # 检查A方案持仓并执行B方案函数
    """
    核心逻辑：
    1.  判断A方案是否持有股票（过滤ETF持仓，仅判断股票）
    2.  若A方案无股票持仓（空仓），执行B方案调仓逻辑
    3.  若A方案有股票持仓，跳过B方案
    """
    log.info("--- 开始判断A方案持仓状态 ---")  # 输出开始判断日志
    # 获取A方案股票持仓（过滤B方案ETF持仓）
    a_stock_positions = []  # 初始化A方案股票持仓列表
    for security in context.portfolio.positions.keys():  # 遍历持仓中的所有证券
        # 区分股票（stock）和ETF（fund）
        sec_type = get_security_info(security).type  # 获取证券类型
        if sec_type == 'stock':  # 如果是股票
            a_stock_positions.append(security)  # 添加到A方案股票持仓列表

    if len(a_stock_positions) > 0:  # 如果A方案持有股票
        log.info(f"A方案持有股票：{a_stock_positions}，不执行B方案")  # 输出A方案持有股票信息
        # 记录B方案9:40未执行状态
        g.b_935_rebalance_executed = True  # 设置已执行标记
        g.b_935_target_etf = None  # 目标ETF为None
        g.b_935_executed_successfully = True  # 设置执行成功标记
        return  # 返回，不执行B方案
    else:  # 如果A方案没有持有股票
        log.info("A方案空仓，触发执行B ETF动量策略")  # 输出A方案空仓信息
        # 执行B方案调仓逻辑
        rebalance_logic(context)  # 调用B方案调仓逻辑

# ======================================
# A方案（股票策略）所有函数
# ======================================
def reset_tech_stoploss_flag(context):  # 重置技术面止损标记函数
    g.tech_stoploss_triggered = False  # 重置技术面止损触发标记
    g.tech_ma_data = None  # 重置技术面均线数据
    g.tech_macd_data = None  # 重置技术面MACD数据
    g.market_type = ""  # 重置市场类型
    g.shock_bull_flag = False  # 重置震荡市看涨标记
    g.buy_allowed_flag = False  # 重置买入允许标记
    g.industry_trend = {}  # 重置板块趋势字典
    log.debug("技术面止损标记、上证指数市场类型标记及板块趋势标记已重置")  # 输出重置日志

def record_stock_last_close(context):  # 记录股票昨日收盘价函数
    g.stock_last_close = {}  # 清空股票昨日收盘价字典
    if not g.hold_list:  # 如果当前无持仓股票
        log.debug("当前无持仓股票，无需记录昨日收盘价")  # 输出无持仓日志
        return  # 返回
    try:  # 尝试获取数据
        stock_prices = get_price(  # 获取股票价格
            g.hold_list,  # 持仓股票列表
            end_date=context.previous_date,  # 截止日期为前一日
            count=1,  # 获取1天数据
            frequency='daily',  # 日频数据
            fields=['close'],  # 收盘价字段
            panel=False,  # 不使用面板数据
            fq='pre'  # 前复权
        )
        for idx, row in stock_prices.iterrows():  # 遍历价格数据
            stock_code = row['code']  # 获取股票代码
            last_close = row['close']  # 获取收盘价
            if pd.notna(last_close):  # 如果收盘价不为空
                g.stock_last_close[stock_code] = last_close  # 记录到字典中
        log.debug(f"成功记录持仓股票昨日收盘价：{g.stock_last_close}")  # 输出记录成功日志
    except Exception as e:  # 捕获异常
        log.error(f"记录股票昨日收盘价失败：{e}")  # 输出错误日志
        g.stock_last_close = {}  # 清空字典

def is_stock_data_sufficient(context, stock_code):  # 检查股票数据是否充足函数
    try:  # 尝试获取数据
        end_date = context.previous_date  # 获取前一日日期
        required_trade_days = g.tech_boll_period + 5  # 计算所需交易日（布林带周期+5）
        trade_dates = get_trade_days(end_date=end_date, count=required_trade_days)  # 获取交易日列表
        if len(trade_dates) < g.tech_boll_period:  # 如果交易日不足
            log.warn(f"{stock_code} 有效交易日不足，返回数据不足")  # 输出警告日志
            return False  # 返回False
        start_date = trade_dates[0]  # 获取开始日期
        stock_prices = get_price(  # 获取股票价格
            stock_code,  # 股票代码
            start_date=start_date,  # 开始日期
            end_date=end_date,  # 结束日期
            frequency='daily',  # 日频数据
            fields=['close'],  # 收盘价字段
            skip_paused=True,  # 跳过停牌
            fq='pre'  # 前复权
        )
        return len(stock_prices) >= g.tech_boll_period  # 返回数据长度是否满足要求
    except Exception as e:  # 捕获异常
        log.error(f"检查{stock_code}数据充足性失败：{e}")  # 输出错误日志
        return False  # 返回False

def calc_industry_trend(context):  # 计算板块趋势函数
    try:  # 尝试计算
        all_industries = get_industries(g.industry_level, date=context.previous_date)  # 获取所有行业
        end_date = context.previous_date  # 获取前一日日期
        max_ma_period = max(g.industry_ma_short, g.industry_ma_long)  # 计算最大均线周期
        start_date = end_date - timedelta(days=max_ma_period + 5)  # 计算开始日期

        for ind_code, ind_info in all_industries.iterrows():  # 遍历所有行业
            stock_list = get_industry_stocks(ind_code, date=end_date)  # 获取行业成分股
            if not stock_list:  # 如果无成分股
                g.industry_trend[ind_code] = "neutral"  # 设置为中性
                log.debug(f"行业{ind_code}（{ind_info['industry_name']}）无成分股，趋势标记为中性")  # 输出日志
                continue  # 继续下一个行业

            stock_trend_list = []  # 初始化股票趋势列表
            for stock in stock_list:  # 遍历行业成分股
                try:  # 尝试获取数据
                    stock_prices = get_price(  # 获取股票价格
                        stock,  # 股票代码
                        start_date=start_date,  # 开始日期
                        end_date=end_date,  # 结束日期
                        frequency='daily',  # 日频数据
                        fields=['close'],  # 收盘价字段
                        skip_paused=True,  # 跳过停牌
                        fq='pre'  # 前复权
                    )
                except Exception as e:  # 捕获异常
                    continue  # 继续下一只股票
                if len(stock_prices) < max_ma_period:  # 如果数据不足
                    continue  # 继续下一只股票

                stock_prices['ma_short'] = stock_prices['close'].rolling(window=g.industry_ma_short).mean()  # 计算短期均线
                stock_prices['ma_long'] = stock_prices['close'].rolling(window=g.industry_ma_long).mean()  # 计算长期均线
                latest_close = stock_prices['close'].iloc[-1]  # 获取最新收盘价
                latest_ma_short = stock_prices['ma_short'].iloc[-1]  # 获取最新短期均线
                latest_ma_long = stock_prices['ma_long'].iloc[-1]  # 获取最新长期均线

                if pd.isna(latest_ma_short) or pd.isna(latest_ma_long):  # 如果均线数据为空
                    continue  # 继续下一只股票

                if latest_ma_short > latest_ma_long and latest_close > latest_ma_long:  # 如果短期均线上穿长期均线且价格高于长期均线
                    stock_trend_list.append("bull")  # 标记为看涨
                elif latest_ma_short < latest_ma_long and latest_close < latest_ma_long:  # 如果短期均线下穿长期均线且价格低于长期均线
                    stock_trend_list.append("bear")  # 标记为看跌
                else:  # 其他情况
                    stock_trend_list.append("neutral")  # 标记为中性

            if len(stock_trend_list) < g.industry_trend_valid_stock:  # 如果有效股票数量不足
                g.industry_trend[ind_code] = "neutral"  # 设置为中性
                log.debug(f"行业{ind_code}（{ind_info['industry_name']}）有效成分股不足，趋势标记为中性")  # 输出日志
            else:  # 如果有效股票数量足够
                bull_count = stock_trend_list.count("bull")  # 统计看涨股票数量
                bear_count = stock_trend_list.count("bear")  # 统计看跌股票数量
                neutral_count = stock_trend_list.count("neutral")  # 统计中性股票数量

                if bull_count >= max(bear_count, neutral_count):  # 如果看涨数量最多
                    g.industry_trend[ind_code] = "bull"  # 设置为看涨
                elif bear_count >= max(bull_count, neutral_count):  # 如果看跌数量最多
                    g.industry_trend[ind_code] = "bear"  # 设置为看跌
                else:  # 如果中性数量最多
                    g.industry_trend[ind_code] = "neutral"  # 设置为中性
                log.debug(f"行业{ind_code}（{ind_info['industry_name']}）趋势：{g.industry_trend[ind_code]} | 看涨：{bull_count} | 看跌：{bear_count} | 中性：{neutral_count}")  # 输出行业趋势日志

        bull_industry_count = len([v for v in g.industry_trend.values() if v == "bull"])  # 统计看涨行业数量
        bear_industry_count = len([v for v in g.industry_trend.values() if v == "bear"])  # 统计看跌行业数量
        neutral_industry_count = len([v for v in g.industry_trend.values() if v == "neutral"])  # 统计中性行业数量
        log.debug(f"板块趋势计算完成 | 看涨：{bull_industry_count} | 看跌：{bear_industry_count} | 中性：{neutral_industry_count}")  # 输出统计日志
    except Exception as e:  # 捕获异常
        log.error(f"计算板块趋势失败：{e}")  # 输出错误日志
        g.industry_trend = {}  # 清空板块趋势字典

def calc_tech_indicators(context):  # 计算技术面指标函数
    if not g.run_tech_stoploss:  # 如果不执行技术面止损
        return  # 返回
    end_date = context.previous_date if hasattr(context, 'previous_date') else (context.current_dt - timedelta(days=1)).date()  # 获取前一日日期
    need_data_count = g.tech_ma_long + g.tech_cum_drop_days  # 计算需要的数据量
    try:  # 尝试获取数据
        index_price = get_price(  # 获取指数价格
            g.tech_stoploss_index,  # 上证指数代码
            end_date=end_date,  # 结束日期
            count=need_data_count,  # 数据数量
            frequency='daily',  # 日频数据
            fields=['close'],  # 收盘价字段
            skip_paused=True,  # 跳过停牌
            fq='pre'  # 前复权
        )
    except Exception as e:  # 捕获异常
        log.error(f"获取上证指数技术面数据失败：{e}")  # 输出错误日志
        g.tech_ma_data = None  # 清空均线数据
        g.tech_macd_data = None  # 清空MACD数据
        return  # 返回
    if index_price is None or len(index_price) < need_data_count:  # 如果数据为空或不足
        log.warn(f"上证指数技术面数据不足")  # 输出警告日志
        g.tech_ma_data = None  # 清空均线数据
        g.tech_macd_data = None  # 清空MACD数据
        return  # 返回
    index_price['ma_short'] = index_price['close'].rolling(window=g.tech_ma_short).mean()  # 计算短期均线
    index_price['ma_mid'] = index_price['close'].rolling(window=g.tech_ma_mid).mean()  # 计算中期均线
    index_price['ma_long'] = index_price['close'].rolling(window=g.tech_ma_long).mean()  # 计算长期均线
    g.tech_ma_data = index_price[['ma_short', 'ma_mid', 'ma_long', 'close']].iloc[-g.tech_cum_drop_days:]  # 保存均线数据
    index_price['ema_fast'] = index_price['close'].ewm(span=g.tech_macd_fast, adjust=False).mean()  # 计算快速EMA
    index_price['ema_slow'] = index_price['close'].ewm(span=g.tech_macd_slow, adjust=False).mean()  # 计算慢速EMA
    index_price['dif'] = index_price['ema_fast'] - index_price['ema_slow']  # 计算DIF线
    index_price['dea'] = index_price['dif'].ewm(span=g.tech_macd_signal, adjust=False).mean()  # 计算DEA线
    g.tech_macd_data = index_price[['dif', 'dea']].iloc[-2:]  # 保存MACD数据
    log.debug("上证指数技术面指标（均线+MACD）计算完成")  # 输出计算完成日志

def calc_csi1000_market_type(context):  # 计算市场类型函数
    sh_index = g.tech_stoploss_index  # 获取上证指数代码
    if sh_index is None:  # 如果指数代码为空
        log.error("未指定上证指数，无法判断市场类型")  # 输出错误日志
        g.buy_allowed_flag = False  # 设置不允许买入
        return  # 返回
    end_date = context.previous_date if hasattr(context, 'previous_date') else (context.current_dt - timedelta(days=1)).date()  # 获取前一日日期
    need_data_count = g.index_ma20 + 5  # 计算需要的数据量
    try:  # 尝试获取数据
        market_price = get_price(  # 获取市场价格
            sh_index,  # 指数代码
            end_date=end_date,  # 结束日期
            count=need_data_count,  # 数据数量
            frequency='daily',  # 日频数据
            fields=['close'],  # 收盘价字段
            skip_paused=True,  # 跳过停牌
            fq='pre'  # 前复权
        )
    except Exception as e:  # 捕获异常
        log.error(f"获取上证指数数据失败：{e}")  # 输出错误日志
        g.market_type = ""  # 清空市场类型
        g.buy_allowed_flag = False  # 设置不允许买入
        return  # 返回
    if market_price is None or len(market_price) < need_data_count:  # 如果数据为空或不足
        log.warn(f"上证指数数据不足，默认允许买入")  # 输出警告日志
        g.market_type = "shock_bull"  # 设置为震荡看涨
        g.buy_allowed_flag = True  # 设置允许买入
        return  # 返回
    market_price['ma5'] = market_price['close'].rolling(window=g.index_ma5).mean()  # 计算5日均线
    market_price['ma10'] = market_price['close'].rolling(window=g.index_ma10).mean()  # 计算10日均线
    market_price['ma20'] = market_price['close'].rolling(window=g.index_ma20).mean()  # 计算20日均线
    latest_ma5 = market_price['ma5'].iloc[-1]  # 获取最新5日均线
    latest_ma10 = market_price['ma10'].iloc[-1]  # 获取最新10日均线
    latest_ma20 = market_price['ma20'].iloc[-1]  # 获取最新20日均线
    latest_close = market_price['close'].iloc[-1]  # 获取最新收盘价
    if latest_ma5 > latest_ma10 and latest_ma10 > latest_ma20:  # 如果均线多头排列
        g.market_type = "bull"  # 设置为牛市
        g.buy_allowed_flag = True  # 设置允许买入
        log.debug(f"上证指数均线多头排列，判断为牛市，允许买入")  # 输出牛市日志
    elif latest_close > latest_ma20:  # 如果收盘价高于20日均线
        g.market_type = "shock_bull"  # 设置为震荡看涨
        g.buy_allowed_flag = True  # 设置允许买入
        log.debug(f"上证指数收盘价在14日均线上方，判断为震荡看涨，允许买入")  # 输出震荡看涨日志
    else:  # 其他情况
        g.market_type = "bear"  # 设置为熊市
        g.buy_allowed_flag = False  # 设置不允许买入
        log.debug(f"上证指数收盘价在14日均线下方，判断为熊市，禁止买入")  # 输出熊市日志

def get_tech_drop_signal():  # 获取技术面下跌信号函数
    if not g.run_tech_stoploss or g.tech_ma_data is None or g.tech_macd_data is None:  # 如果不执行技术面止损或数据为空
        return False  # 返回False
    ma_empty_flag = False  # 初始化均线空头标记
    cum_drop_flag = False  # 初始化累计跌幅标记
    macd_death_flag = False  # 初始化MACD死叉标记
    latest_ma_short = g.tech_ma_data['ma_short'].iloc[-1]  # 获取最新短期均线
    latest_ma_mid = g.tech_ma_data['ma_mid'].iloc[-1]  # 获取最新中期均线
    latest_ma_long = g.tech_ma_data['ma_long'].iloc[-1]  # 获取最新长期均线
    if not np.isnan(latest_ma_short) and not np.isnan(latest_ma_mid) and not np.isnan(latest_ma_long):  # 如果均线数据不为空
        if latest_ma_short < latest_ma_mid and latest_ma_mid < latest_ma_long:  # 如果均线空头排列
            ma_empty_flag = True  # 设置均线空头标记
            log.debug(f"触发上证指数均线空头信号")  # 输出均线空头信号日志
    start_close = g.tech_ma_data['close'].iloc[0]  # 获取起始收盘价
    end_close = g.tech_ma_data['close'].iloc[-1]  # 获取结束收盘价
    cum_drop_pct = (end_close / start_close - 1) * 100  # 计算累计跌幅
    if cum_drop_pct <= g.tech_cum_drop_threshold:  # 如果累计跌幅超过阈值
        cum_drop_flag = True  # 设置累计跌幅标记
        log.debug(f"触发上证指数累计跌幅信号")  # 输出累计跌幅信号日志
    if len(g.tech_macd_data) >= 2:  # 如果MACD数据至少有2条
        prev_dif = g.tech_macd_data['dif'].iloc[-2]  # 获取前一日DIF
        prev_dea = g.tech_macd_data['dea'].iloc[-2]  # 获取前一日DEA
        latest_dif = g.tech_macd_data['dif'].iloc[-1]  # 获取最新DIF
        latest_dea = g.tech_macd_data['dea'].iloc[-1]  # 获取最新DEA
        if not np.isnan(prev_dif) and not np.isnan(prev_dea) and not np.isnan(latest_dif) and not np.isnan(latest_dea):  # 如果MACD数据不为空
            if prev_dif >= prev_dea and latest_dif < latest_dea:  # 如果DIF上穿DEA后又下穿
                macd_death_flag = True  # 设置MACD死叉标记
                log.debug(f"触发上证指数MACD死叉信号")  # 输出MACD死叉信号日志
    return ma_empty_flag or cum_drop_flag or macd_death_flag  # 返回任一信号触发

def execute_tech_stoploss(context):  # 执行技术面止损函数
    if not g.run_tech_stoploss or g.tech_stoploss_triggered:  # 如果不执行技术面止损或已触发
        return  # 返回
    if not get_tech_drop_signal():  # 如果技术面下跌信号未触发
        return  # 返回
    g.tech_stoploss_triggered = True  # 设置技术面止损已触发
    g.reason_to_sell = 'stoploss'  # 设置卖出原因为止损
    log.debug("【上证指数技术面止损触发】执行全局清仓")  # 输出止损触发日志
    for stock in context.portfolio.positions.keys():  # 遍历持仓中的所有股票
        try:  # 尝试卖出
            order_target_value(stock, 0)  # 目标持仓值设为0
            log.debug(f"技术面止损卖出：{stock}")  # 输出卖出日志
        except Exception as e:  # 捕获异常
            log.error(f"技术面止损卖出{stock}失败：{e}")  # 输出错误日志

def prepare_stock_list(context):  # 准备股票列表函数
    g.hold_list= []  # 清空持仓列表
    for position in list(context.portfolio.positions.values()):  # 遍历持仓中的所有仓位
        stock = position.security  # 获取股票代码
        g.hold_list.append(stock)  # 添加到持仓列表
    if g.hold_list != []:  # 如果持仓列表不为空
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit','low_limit'], count=1, panel=False, fill_paused=False)  # 获取价格数据
        df = df[df["close"] == df["high_limit"]]  # 筛选涨停股票
        g.yesterday_HL_list = list(df.code)  # 获取昨日涨停股票列表
    else:  # 如果持仓列表为空
        g.yesterday_HL_list = []  # 清空昨日涨停股票列表
    g.no_trading_today_signal = False  # 设置今日交易信号为False

def get_history_highlimit(context, stock_list, days=3*250, p=0.10):  # 获取历史涨停股票函数
    df = get_price(  # 获取价格数据
        stock_list,  # 股票列表
        end_date=context.previous_date,  # 结束日期
        frequency="daily",  # 日频数据
        fields=["close", "high_limit"],  # 收盘价和涨停价字段
        count=days,  # 数据天数
        panel=False,  # 不使用面板数据
    )
    df = df[df["close"] == df["high_limit"]]  # 筛选涨停股票
    grouped_result = df.groupby('code').size().reset_index(name='count')  # 按股票代码分组统计涨停次数
    grouped_result = grouped_result.sort_values(by=["count"], ascending=False)  # 按涨停次数降序排序
    result_list = grouped_result["code"].tolist()[:int(len(grouped_result)*p)]  # 获取前p比例的股票
    log.info(f"历史涨停筛选：前{len(result_list)}只股票")  # 输出筛选日志
    return result_list  # 返回结果列表

def get_start_point(context, stock_list, days=3*250):  # 获取启动点函数
    df = get_price(  # 获取价格数据
        stock_list,  # 股票列表
        end_date=context.previous_date,  # 结束日期
        frequency="daily",  # 日频数据
        fields=["open", "low", "close", "high_limit"],  # 开盘价、最低价、收盘价、涨停价字段
        count=days,  # 数据天数
        panel=False,  # 不使用面板数据
    )
    stock_start_point = {}  # 初始化启动点字典
    stock_price_bias = {}  # 初始化价格偏差字典
    current_data = get_current_data()  # 获取当前数据
    for code, group in df.groupby('code'):  # 按股票代码分组
        group = group.sort_values('time')  # 按时间排序
        limit_hit_rows = group[group['close'] == group['high_limit']]  # 获取涨停记录
        if not limit_hit_rows.empty:  # 如果有涨停记录
            latest_limit_hit = limit_hit_rows.iloc[-1]  # 获取最近一次涨停
            latest_limit_index = latest_limit_hit.name  # 获取索引
            previous_rows = group[group.index <= latest_limit_index].iloc[::-1]  # 获取涨停前的数据
            for idx, row in previous_rows.iterrows():  # 遍历涨停前数据
                if row['close'] < row['open']:  # 如果收盘价小于开盘价（阴线）
                    stock_start_point[code] = row['low']  # 记录最低价为启动点
                    break  # 跳出循环
    for code, start_point in stock_start_point.items():  # 遍历启动点
        last_price = current_data[code].last_price  # 获取当前价格
        bias = last_price / start_point  # 计算价格偏差
        stock_price_bias[code] = bias  # 记录偏差
    sorted_list = sorted(stock_price_bias.items(), key=lambda x: x[1], reverse=False)  # 按偏差升序排序
    return [i[0] for i in sorted_list]  # 返回股票代码列表

def get_stock_industry_code(context, stock):  # 获取股票行业代码函数
    try:  # 尝试获取行业信息
        industry_info = get_industry(stock, date=context.previous_date)  # 获取行业信息
        if g.industry_level in industry_info[stock]:  # 如果行业级别在信息中
            return industry_info[stock][g.industry_level]  # 返回行业代码
        else:  # 如果不在
            return None  # 返回None
    except Exception as e:  # 捕获异常
        log.error(f"获取{stock}板块信息失败：{e}")  # 输出错误日志
        return None  # 返回None

def is_stock_bearish_by_weight(context, stock):  # 判断股票是否加权看跌函数
    if not is_stock_data_sufficient(context, stock):  # 如果数据不充足
        log.debug(f"{stock} 数据不足，直接标记为加权看跌")  # 输出数据不足日志
        return True  # 返回True
    try:  # 尝试计算
        end_date = context.previous_date  # 获取前一日日期
        start_date = end_date - timedelta(days=g.tech_boll_period + 10)  # 计算开始日期
        stock_prices = get_price(  # 获取股票价格
            stock,  # 股票代码
            start_date=start_date,  # 开始日期
            end_date=end_date,  # 结束日期
            frequency='daily',  # 日频数据
            fields=['open', 'high', 'low', 'close'],  # 开盘、最高、最低、收盘价字段
            skip_paused=True,  # 跳过停牌
            fq='pre'  # 前复权
        )
        if stock_prices.empty:  # 如果价格数据为空
            log.warn(f"{stock} 无有效历史交易数据，标记为加权看跌")  # 输出警告日志
            return True  # 返回True
        indicator_bearish = {ind: False for ind in g.tech_indicators}  # 初始化指标看跌标记
        bearish_score = 0.0  # 初始化看跌得分
        # 均线指标
        if "ma" in g.tech_indicators:  # 如果包含均线指标
            stock_prices['ma_short'] = stock_prices['close'].rolling(window=g.tech_ma_short).mean()  # 计算短期均线
            stock_prices['ma_mid'] = stock_prices['close'].rolling(window=g.tech_ma_mid).mean()  # 计算中期均线
            stock_prices['ma_long'] = stock_prices['close'].rolling(window=g.tech_ma_long).mean()  # 计算长期均线
            latest_ma_short = stock_prices['ma_short'].iloc[-1]  # 获取最新短期均线
            latest_ma_mid = stock_prices['ma_mid'].iloc[-1]  # 获取最新中期均线
            latest_ma_long = stock_prices['ma_long'].iloc[-1]  # 获取最新长期均线
            if pd.notna(latest_ma_short) and pd.notna(latest_ma_mid) and pd.notna(latest_ma_long):  # 如果均线数据不为空
                indicator_bearish['ma'] = (latest_ma_short < latest_ma_mid) and (latest_ma_mid < latest_ma_long)  # 判断均线空头排列
            bearish_score += indicator_bearish['ma'] * g.tech_weights['ma']  # 累加加权得分
        # MACD指标
        if "macd" in g.tech_indicators:  # 如果包含MACD指标
            stock_prices['ema_fast'] = stock_prices['close'].ewm(span=g.tech_macd_fast, adjust=False).mean()  # 计算快速EMA
            stock_prices['ema_slow'] = stock_prices['close'].ewm(span=g.tech_macd_slow, adjust=False).mean()  # 计算慢速EMA
            stock_prices['dif'] = stock_prices['ema_fast'] - stock_prices['ema_slow']  # 计算DIF
            stock_prices['dea'] = stock_prices['dif'].ewm(span=g.tech_macd_signal, adjust=False).mean()  # 计算DEA
            if len(stock_prices) >= 2:  # 如果数据至少有2条
                latest_dif = stock_prices['dif'].iloc[-1]  # 获取最新DIF
                latest_dea = stock_prices['dea'].iloc[-1]  # 获取最新DEA
                prev_dif = stock_prices['dif'].iloc[-2]  # 获取前一日DIF
                prev_dea = stock_prices['dea'].iloc[-2]  # 获取前一日DEA
                if pd.notna(latest_dif) and pd.notna(latest_dea) and pd.notna(prev_dif) and pd.notna(prev_dea):  # 如果数据不为空
                    macd_death_cross = (prev_dif >= prev_dea) and (latest_dif < latest_dea)  # 判断MACD死叉
                    macd_below_zero = (latest_dif < 0) and (latest_dea < 0)  # 判断MACD在零轴下方
                    indicator_bearish['macd'] = macd_death_cross and macd_below_zero  # 设置MACD看跌标记
            bearish_score += indicator_bearish['macd'] * g.tech_weights['macd']  # 累加加权得分
        # KDJ指标
        if "kdj" in g.tech_indicators:  # 如果包含KDJ指标
            stock_prices['lowest_low'] = stock_prices['low'].rolling(window=g.tech_kdj_period).min()  # 计算最低价最低值
            stock_prices['highest_high'] = stock_prices['high'].rolling(window=g.tech_kdj_period).max()  # 计算最高价最高值
            stock_prices['rsv'] = (stock_prices['close'] - stock_prices['lowest_low']) / (stock_prices['highest_high'] - stock_prices['lowest_low']) * 100  # 计算RSV
            stock_prices['k'] = stock_prices['rsv'].rolling(window=3).mean()  # 计算K值
            stock_prices['d'] = stock_prices['k'].rolling(window=3).mean()  # 计算D值
            stock_prices['j'] = 3 * stock_prices['k'] - 2 * stock_prices['d']  # 计算J值
            if len(stock_prices) >= 2:  # 如果数据至少有2条
                latest_k = stock_prices['k'].iloc[-1]  # 获取最新K值
                latest_d = stock_prices['d'].iloc[-1]  # 获取最新D值
                latest_j = stock_prices['j'].iloc[-1]  # 获取最新J值
                prev_k = stock_prices['k'].iloc[-2]  # 获取前一日K值
                prev_d = stock_prices['d'].iloc[-2]  # 获取前一日D值
                if pd.notna(latest_k) and pd.notna(latest_d) and pd.notna(latest_j) and pd.notna(prev_k) and pd.notna(prev_d):  # 如果数据不为空
                    kdj_death_cross = (prev_k >= prev_d) and (latest_k < latest_d)  # 判断KDJ死叉
                    kdj_j_below_zero = (latest_j < 0)  # 判断J值低于0
                    indicator_bearish['kdj'] = kdj_death_cross and kdj_j_below_zero  # 设置KDJ看跌标记
            bearish_score += indicator_bearish['kdj'] * g.tech_weights['kdj']  # 累加加权得分
        # 布林带指标
        if "boll" in g.tech_indicators:  # 如果包含布林带指标
            stock_prices['boll_mid'] = stock_prices['close'].rolling(window=g.tech_boll_period).mean()  # 计算布林带中轨
            stock_prices['boll_std'] = stock_prices['close'].rolling(window=g.tech_boll_period).std()  # 计算布林带标准差
            stock_prices['boll_upper'] = stock_prices['boll_mid'] + g.tech_boll_dev * stock_prices['boll_std']  # 计算布林带上轨
            stock_prices['boll_lower'] = stock_prices['boll_mid'] - g.tech_boll_dev * stock_prices['boll_std']  # 计算布林带下轨
            if len(stock_prices) >= 2:  # 如果数据至少有2条
                latest_close = stock_prices['close'].iloc[-1]  # 获取最新收盘价
                latest_boll_lower = stock_prices['boll_lower'].iloc[-1]  # 获取最新布林带下轨
                prev_boll_upper = stock_prices['boll_upper'].iloc[-2]  # 获取前一日布林带上轨
                latest_boll_upper = stock_prices['boll_upper'].iloc[-1]  # 获取最新布林带上轨
                prev_boll_lower = stock_prices['boll_lower'].iloc[-2]  # 获取前一日布林带下轨
                latest_boll_lower = stock_prices['boll_lower'].iloc[-1]  # 获取最新布林带下轨
                if pd.notna(latest_close) and pd.notna(latest_boll_lower) and pd.notna(prev_boll_upper) and pd.notna(latest_boll_upper) and pd.notna(prev_boll_lower) and pd.notna(latest_boll_lower):  # 如果数据不为空
                    price_below_lower = (latest_close < latest_boll_lower)  # 判断价格跌破下轨
                    boll_downward = (latest_boll_upper < prev_boll_upper) and (latest_boll_lower < prev_boll_lower)  # 判断布林带向下收缩
                    indicator_bearish['boll'] = price_below_lower and boll_downward  # 设置布林带看跌标记
            bearish_score += indicator_bearish['boll'] * g.tech_weights['boll']  # 累加加权得分
        is_bearish = bearish_score >= g.tech_bearish_threshold  # 判断总得分是否超过阈值
        log.debug(f"{stock} 技术分析结果：{indicator_bearish} | 加权得分：{bearish_score:.2f} | 阈值：{g.tech_bearish_threshold} | 判定看跌：{is_bearish}")  # 输出分析结果日志
        return is_bearish  # 返回是否看跌
    except Exception as e:  # 捕获异常
        log.error(f"{stock} 技术分析失败：{e}，标记为加权看跌")  # 输出错误日志
        return True  # 返回True

def get_stock_list(context):  # 获取股票列表函数
    final_list = []  # 初始化最终列表
    yesterday = context.previous_date  # 获取前一日日期
    initial_list = get_all_securities("stock", yesterday).index.tolist()  # 获取所有股票
    initial_list = filter_new_stock(context, initial_list)  # 过滤新股
    initial_list = filter_kcbj_stock(initial_list)  # 过滤科创板和北交所股票
    initial_list = filter_st_stock(context, initial_list)  # 过滤ST股票
    initial_list = filter_paused_stock(initial_list)  # 过滤停牌股票
    q = query(  # 查询条件
        valuation.code,indicator.eps  # 查询股票代码和每股收益
        ).filter(  # 过滤条件
            valuation.code.in_(initial_list)  # 代码在初始列表中
            ).order_by(  # 排序方式
                valuation.market_cap.asc()  # 按市值升序排列
                )
    df = get_fundamentals(q)  # 获取基本面数据
    if df.empty:  # 如果数据为空
        log.warn("小市值筛选无有效股票，返回空列表")  # 输出警告日志
        return final_list  # 返回空列表
    initial_list = df['code'].tolist()[:g.init_stock_count]  # 获取前1000只股票
    initial_list = filter_limitup_stock(context, initial_list)  # 过滤涨停股票
    initial_list = filter_limitdown_stock(context, initial_list)  # 过滤跌停股票
    if initial_list:  # 如果列表不为空
        initial_list = get_history_highlimit(context, initial_list, g.limit_days_window)  # 获取历史涨停股票
    else:  # 如果列表为空
        log.warn("历史涨停筛选前无有效股票，返回空列表")  # 输出警告日志
        return final_list  # 返回空列表
    if initial_list:  # 如果列表不为空
        initial_list = get_start_point(context, initial_list, g.limit_days_window)  # 获取启动点
    else:  # 如果列表为空
        log.warn("启动点筛选前无有效股票，返回空列表")  # 输出警告日志
        return final_list  # 返回空列表
    stock_list = get_stock_industry(initial_list)  # 按行业分散化选择股票
    # 板块趋势筛选
    qualified_stocks = []  # 初始化合格股票列表
    for stock in stock_list:  # 遍历股票列表
        ind_code = get_stock_industry_code(context, stock)  # 获取行业代码
        if not ind_code or ind_code not in g.industry_trend:  # 如果行业代码为空或不在趋势字典中
            continue  # 继续下一只股票
        ind_trend = g.industry_trend[ind_code]  # 获取行业趋势
        if ind_trend in ["bull", "neutral"]:  # 如果行业趋势为看涨或中性
            qualified_stocks.append(stock)  # 添加到合格股票列表
            log.debug(f"{stock}（板块：{ind_code}）趋势{ind_trend}，符合要求")  # 输出符合要求日志
        else:  # 如果行业趋势为看跌
            log.debug(f"{stock}（板块：{ind_code}）趋势{ind_trend}，过滤剔除")  # 输出过滤日志
    if not qualified_stocks:  # 如果合格股票为空
        qualified_stocks = stock_list  # 使用原行业分散筛选结果
        log.warn("板块趋势筛选无合格股票，使用原行业分散筛选结果")  # 输出警告日志
    final_list = qualified_stocks[:10]  # 取前10只股票
    log.info(f"候选股票列表（按排名排序）：{final_list}")  # 输出候选股票日志
    return final_list  # 返回最终列表

def get_valid_target_stock(context, candidate_stocks):  # 获取有效目标股票函数
    if not candidate_stocks:  # 如果候选股票列表为空
        log.warn("候选股票列表为空，无有效目标股票")  # 输出警告日志
        return None  # 返回None
    sufficient_data_stocks = []  # 初始化数据充足股票列表
    for stock in candidate_stocks:  # 遍历候选股票
        if is_stock_data_sufficient(context, stock):  # 如果数据充足
            sufficient_data_stocks.append(stock)  # 添加到数据充足股票列表
            log.debug(f"{stock} 数据充足，纳入有效筛选范围")  # 输出数据充足日志
        else:  # 如果数据不足
            log.warn(f"{stock} 数据不足，跳过筛选")  # 输出数据不足日志
    if not sufficient_data_stocks:  # 如果数据充足股票为空
        log.warn("无数据充足的候选股票，将保留原有持仓")  # 输出警告日志
        return None  # 返回None
    for stock in sufficient_data_stocks:  # 遍历数据充足股票
        is_bearish = is_stock_bearish_by_weight(context, stock)  # 判断是否加权看跌
        if not is_bearish:  # 如果不是看跌
            log.info(f"{stock}（排名{sufficient_data_stocks.index(stock)+1}）加权得分未达标，选为目标股票")  # 输出选为目标股票日志
            return stock  # 返回目标股票
        else:  # 如果是看跌
            log.info(f"{stock}（排名{sufficient_data_stocks.index(stock)+1}）加权看跌，放弃该股票")  # 输出放弃股票日志
    log.warn("所有数据充足的候选股票均加权看跌，无有效目标股票")  # 输出警告日志
    return None  # 返回None

def weekly_adjustment(context):
    if g.no_trading_today_signal == False:
        close_no_trading_hold(context)
        candidate_stocks = get_stock_list(context)
        valid_target = get_valid_target_stock(context, candidate_stocks)
        
        # ========== 新增：目标一致性判断（核心修改） ==========
        # 1. 提取当前持仓的股票（过滤ETF，仅保留A方案股票持仓）
        current_hold_stock = None
        for sec in context.portfolio.positions.keys():
            sec_type = get_security_info(sec).type
            if sec_type == 'stock':  # 仅判断股票持仓
                current_hold_stock = sec
                break  # A方案仅持有1只股票，找到后直接跳出
        
        # 2. 对比当前持仓股票与本次目标股票是否一致
        if valid_target is not None and current_hold_stock == valid_target:
            log.info(f"当前持仓股票{current_hold_stock}与本次目标股票{valid_target}一致，无需执行调仓操作")
            g.target_list = [valid_target]  # 保持目标列表更新
            return  # 跳过后续调仓流程
        
        # 3. 若目标不一致/无当前持仓，再执行原有调仓逻辑
        if not valid_target:
            log.warn("无有效目标股票，保留原有持仓，不执行调仓操作")
            g.target_list = []
            return
        
        g.target_list = [valid_target]
        log.info(f"本次有效目标股票：{g.target_list}")
        
        # 原有卖出逻辑（仅卖出非目标/非昨日涨停股票）
        for stock in g.hold_list:
            if (stock not in g.target_list) and (stock not in g.yesterday_HL_list):
                log.info(f"卖出非目标股票：{stock}")
                position = context.portfolio.positions[stock]
                close_position(position)
            else:
                log.info(f"继续持有：{stock}")
        
        # 原有买入逻辑
        if g.buy_allowed_flag:
            buy_security(context, g.target_list)
            for position in list(context.portfolio.positions.values()):
                stock = position.security
                g.not_buy_again.append(stock)
        else:
            log.info(f"当前市场类型不允许买入，放弃买入目标股票{valid_target}，保留原有持仓")
def check_limit_up_and_zero_axis(context):  # 检查涨停及零轴函数
    if not g.yesterday_HL_list:  # 如果昨日涨停列表为空
        return  # 返回
    current_data = get_current_data()  # 获取当前数据
    for stock in g.yesterday_HL_list:  # 遍历昨日涨停股票
        if stock not in context.portfolio.positions:  # 如果不在持仓中
            continue  # 继续下一只股票
        if stock not in g.stock_last_close:  # 如果不在昨日收盘价字典中
            log.warn(f"未获取到{stock}昨日收盘价，跳过0轴判断")  # 输出警告日志
            continue  # 继续下一只股票
        zero_axis_price = g.stock_last_close[stock]  # 获取昨日收盘价（零轴）
        current_price = current_data[stock].last_price  # 获取当前价格
        if current_price < zero_axis_price:  # 如果当前价格低于零轴
            log.info(f"{stock}涨停次日价格{current_price:.2f}低于0轴（{zero_axis_price:.2f}），执行卖出")  # 输出卖出日志
            position = context.portfolio.positions[stock]  # 获取仓位
            close_position(position)  # 关闭仓位
            g.reason_to_sell = 'limit_up_zero_axis'  # 设置卖出原因为涨停零轴
        else:  # 如果当前价格高于零轴
            log.debug(f"{stock}涨停次日价格{current_price:.2f}高于0轴（{zero_axis_price:.2f}），继续持有")  # 输出继续持有一天志

def check_limit_up(context):  # 检查涨停函数
    now_time = context.current_dt  # 获取当前时间
    if g.yesterday_HL_list != []:  # 如果昨日涨停列表不为空
        for stock in g.yesterday_HL_list:  # 遍历昨日涨停股票
            if context.portfolio.positions[stock].closeable_amount > -100:  # 如果可卖出数量大于-100
                current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)  # 获取价格数据
                if current_data.iloc[0,0] < current_data.iloc[0,1]:  # 如果收盘价小于涨停价
                    log.info(f"[{stock}]涨停打开，卖出")  # 输出涨停打开卖出日志
                    position = context.portfolio.positions[stock]  # 获取仓位
                    close_position(position)  # 关闭仓位
                    g.reason_to_sell = 'limitup'  # 设置卖出原因为涨停
                else:  # 如果收盘价等于涨停价
                    log.info(f"[{stock}]涨停，继续持有")  # 输出继续持有一天志

def check_remain_amount(context):  # 检查剩余金额函数
    if g.reason_to_sell == 'limitup' or g.reason_to_sell == 'limit_up_zero_axis':  # 如果卖出原因是涨停或涨停零轴
        g.hold_list= []  # 清空持仓列表
        for position in list(context.portfolio.positions.values()):  # 遍历持仓仓位
            stock = position.security  # 获取股票代码
            g.hold_list.append(stock)  # 添加到持仓列表
        if len(g.hold_list) < g.stock_num and g.buy_allowed_flag:  # 如果持仓数量小于目标数量且允许买入
            candidate_stocks = get_stock_list(context)  # 获取股票列表
            valid_target = get_valid_target_stock(context, candidate_stocks)  # 获取有效目标股票
            if valid_target:  # 如果有有效目标股票
                target_list = [valid_target]  # 设置目标列表
                target_list = filter_not_buy_again(target_list)  # 过滤不重复买入股票
                log.info(f"有余额可用{round((context.portfolio.cash),2)}元，补买目标股票：{target_list}")  # 输出补买日志
                buy_security(context, target_list)  # 买入证券
        g.reason_to_sell = ''  # 清空卖出原因
    else:  # 如果卖出原因不是涨停或涨停零轴
        g.reason_to_sell = ''  # 清空卖出原因

def trade_afternoon(context):  # 下午交易函数
    if g.no_trading_today_signal == False:  # 如果今日交易信号为False
        check_limit_up(context)  # 检查涨停
        if g.HV_control == True:  # 如果启用放量控制
            check_high_volume(context)  # 检查放量
        huanshou(context)  # 换手率控制
        check_remain_amount(context)  # 检查剩余金额

def sell_stocks(context):  # 卖出股票函数
    if g.run_stoploss == True:  # 如果执行止损
        if g.run_tech_stoploss and not g.tech_stoploss_triggered:  # 如果执行技术面止损且未触发
            if get_tech_drop_signal():  # 如果技术面下跌信号触发
                g.tech_stoploss_triggered = True  # 设置技术面止损已触发
                g.reason_to_sell = 'stoploss'  # 设置卖出原因为止损
                log.debug("【上证指数技术面止损触发】执行全局清仓")  # 输出止损触发日志
                for stock in context.portfolio.positions.keys():  # 遍历持仓股票
                    order_target_value(stock, 0)  # 目标持仓值设为0
                    log.debug(f"技术面止损卖出：{stock}")  # 输出卖出日志
                return  # 返回
        if g.stoploss_strategy == 1:  # 如果止损策略为1（固定止损）
            for stock in context.portfolio.positions.keys():  # 遍历持仓股票
                if context.portfolio.positions[stock].price >= context.portfolio.positions[stock].avg_cost * 2:  # 如果价格达到成本价的2倍
                    order_target_value(stock, 0)  # 目标持仓值设为0
                    log.debug(f"收益100%止盈,卖出{stock}")  # 输出止盈日志
                elif context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit:  # 如果价格低于成本价的93%
                    order_target_value(stock, 0)  # 目标持仓值设为0
                    log.debug(f"收益止损,卖出{stock}")  # 输出止损日志
                    g.reason_to_sell = 'stoploss'  # 设置卖出原因为止损
        elif g.stoploss_strategy == 2:  # 如果止损策略为2（市场趋势止损）
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False)  # 获取中小板指数数据
            down_ratio = (stock_df['close'] / stock_df['open']).mean()  # 计算跌幅比例
            if down_ratio <= g.stoploss_market:  # 如果跌幅超过阈值
                g.reason_to_sell = 'stoploss'  # 设置卖出原因为止损
                log.debug(f"中小板指惨跌,平均降幅{down_ratio:.2%}")  # 输出跌幅日志
                for stock in context.portfolio.positions.keys():  # 遍历持仓股票
                    order_target_value(stock, 0)  # 目标持仓值设为0
        elif g.stoploss_strategy == 3:  # 如果止损策略为3（联合止损）
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False)  # 获取中小板指数数据
            down_ratio = (stock_df['close'] / stock_df['open']).mean()  # 计算跌幅比例
            if down_ratio <= g.stoploss_market:  # 如果跌幅超过阈值
                g.reason_to_sell = 'stoploss'  # 设置卖出原因为止损
                log.debug(f"中小板指惨跌,平均降幅{down_ratio:.2%}")  # 输出跌幅日志
                for stock in context.portfolio.positions.keys():  # 遍历持仓股票
                    order_target_value(stock, 0)  # 目标持仓值设为0
            else:  # 如果跌幅未超过阈值
                for stock in context.portfolio.positions.keys():  # 遍历持仓股票
                    if context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit:  # 如果价格低于成本价的93%
                        order_target_value(stock, 0)  # 目标持仓值设为0
                        log.debug(f"收益止损,卖出{stock}")  # 输出止损日志
                        g.reason_to_sell = 'stoploss'  # 设置卖出原因为止损

def check_high_volume(context):  # 检查放量函数
    current_data = get_current_data()  # 获取当前数据
    for stock in context.portfolio.positions:  # 遍历持仓股票
        if current_data[stock].paused == True:  # 如果股票停牌
            continue  # 继续下一只股票
        if current_data[stock].last_price == current_data[stock].high_limit:  # 如果股票涨停
            continue  # 继续下一只股票
        if context.portfolio.positions[stock].closeable_amount ==0:  # 如果可卖出数量为0
            continue  # 继续下一只股票
        df_volume = get_bars(stock,count=g.HV_duration,unit='1d',fields=['volume'],include_now=True, df=True)  # 获取成交量数据
        if df_volume['volume'].values[-1] > g.HV_ratio*df_volume['volume'].values.max():  # 如果最新成交量超过最大成交量的85%
            position = context.portfolio.positions[stock]  # 获取仓位
            r = close_position(position)  # 关闭仓位
            log.info(f"[{stock}]天量，卖出, close_position: {r}")  # 输出卖出日志
            g.reason_to_sell = 'limitup'  # 设置卖出原因为涨停

def filter_paused_stock(stock_list):  # 过滤停牌股票函数
    current_data = get_current_data()  # 获取当前数据
    return [stock for stock in stock_list if not current_data[stock].paused]  # 返回未停牌股票列表

def filter_st_stock(context, stock_list):  # 过滤ST股票函数
    current_data = get_current_data()  # 获取当前数据
    filtered_list = []  # 初始化过滤后列表
    for stock in stock_list:  # 遍历股票列表
        if (not current_data[stock].is_st) and ("ST" not in current_data[stock].name.upper()):  # 如果不是ST股票
            filtered_list.append(stock)  # 添加到过滤后列表
        else:  # 如果是ST股票
            log.debug(f"过滤掉ST股票：{stock}（名称：{current_data[stock].name}，ST状态：{current_data[stock].is_st}）")  # 输出过滤日志
    return filtered_list  # 返回过滤后列表

def filter_kcbj_stock(stock_list):  # 过滤科创板和北交所股票函数
    for stock in stock_list[:]:  # 遍历股票列表副本
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':  # 如果是科创板或北交所股票
            stock_list.remove(stock)  # 从列表中移除
    return stock_list  # 返回过滤后列表

def filter_limitup_stock(context, stock_list):  # 过滤涨停股票函数
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近1分钟收盘价
    current_data = get_current_data()  # 获取当前数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()  # 返回在持仓中或未涨停的股票
            or last_prices[stock][-1] < current_data[stock].high_limit]  # 价格小于涨停价

def filter_limitdown_stock(context, stock_list):  # 过滤跌停股票函数
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近1分钟收盘价
    current_data = get_current_data()  # 获取当前数据
    return [stock for stock in stock_list if (stock in context.portfolio.positions.keys()  # 返回在持仓中或未跌停的股票
            or last_prices[stock][-1] > current_data[stock].low_limit)  # 价格大于跌停价
            ]

def filter_new_stock(context,stock_list):  # 过滤新股函数
    yesterday = context.previous_date  # 获取前一日日期
    return [stock for stock in stock_list if not (yesterday - get_security_info(stock).start_date) < timedelta(days=375)]  # 返回上市时间超过375天的股票

def filter_highprice_stock(context,stock_list):  # 过滤高价股票函数
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近1分钟收盘价
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()  # 返回在持仓中或价格不超过20元的股票
            or last_prices[stock][-1] <= g.up_price]  # 价格小于等于20元

def filter_not_buy_again(stock_list):  # 过滤不重复买入股票函数
    return [stock for stock in stock_list if stock not in g.not_buy_again]  # 返回不在不重复买入列表中的股票

def get_stock_industry(stock):  # 按行业分散化选择股票函数
    result = get_industry(security=stock)  # 获取股票行业信息
    selected_stocks = []  # 初始化选择股票列表
    industry_list = []  # 初始化行业列表
    for stock_code, info in result.items():  # 遍历行业信息
        industry_name = info['sw_l2']['industry_name']  # 获取二级行业名称
        if industry_name not in industry_list:  # 如果行业不在列表中
            industry_list.append(industry_name)  # 添加到行业列表
            selected_stocks.append(stock_code)  # 添加到选择股票列表
            if len(industry_list) == 10 :  # 如果行业数量达到10个
                break  # 跳出循环
    return selected_stocks  # 返回选择股票列表

def huanshoulv(context, stock, is_avg=False):  # 计算换手率函数
    if is_avg:  # 如果计算平均换手率
        start_date = context.current_dt - timedelta(days=14)  # 计算14天前日期
        end_date = context.previous_date  # 获取前一日日期
        df_volume = get_price(stock,end_date=end_date, frequency='daily', fields=['volume'],count=14)  # 获取14天成交量数据
        df_cap = get_valuation(stock, end_date=end_date, fields=['circulating_cap'], count=1)  # 获取流通股本数据
        circulating_cap = df_cap['circulating_cap'].iloc[0] if not df_cap.empty else 0  # 获取流通股本
        if circulating_cap == 0:  # 如果流通股本为0
            return 0.0  # 返回0
        df_volume['turnover_ratio'] = df_volume['volume'] / (circulating_cap * 10000)  # 计算换手率
        return df_volume['turnover_ratio'].mean()  # 返回平均换手率
    else:  # 如果计算当前换手率
        date_now = context.current_dt  # 获取当前时间
        df_vol = get_price(stock, start_date=date_now.date(), end_date=date_now, frequency='1m', fields=['volume'],  # 获取当天分钟级成交量数据
                           skip_paused=False, fq='pre', panel=True, fill_paused=False)
        volume = df_vol['volume'].sum()  # 计算当天总成交量
        date_pre = context.previous_date  # 获取前一日日期
        df_circulating_cap = get_valuation(stock, end_date=date_pre, fields=['circulating_cap'], count=1)  # 获取前一日流通股本数据
        circulating_cap = df_circulating_cap['circulating_cap'].iloc[0]  if not df_circulating_cap.empty else 0  # 获取流通股本
        if circulating_cap == 0:  # 如果流通股本为0
            return 0.0  # 返回0
        turnover_ratio = volume / (circulating_cap * 10000)  # 计算换手率
        return turnover_ratio  # 返回换手率

def huanshou(context):  # 换手率控制函数
    ss = []  # 初始化列表
    current_data = get_current_data()  # 获取当前数据
    shrink, expand = 0.003, 0.1  # 设置缩量和放量阈值
    for stock in context.portfolio.positions:  # 遍历持仓股票
        if current_data[stock].paused == True:  # 如果股票停牌
            continue  # 继续下一只股票
        if current_data[stock].last_price >= current_data[stock].high_limit*0.97:  # 如果价格接近涨停价
            continue  # 继续下一只股票
        if context.portfolio.positions[stock].closeable_amount ==0:  # 如果可卖出数量为0
            continue  # 继续下一只股票
        rt = huanshoulv(context, stock, False)  # 获取当前换手率
        avg = huanshoulv(context, stock, True)  # 获取平均换手率
        if avg == 0: continue  # 如果平均换手率为0，继续下一只股票
        r = rt / avg  # 计算换手率倍数
        action, icon = '', ''  # 初始化操作和图标
        if avg < 0.003:  # 如果平均换手率小于0.3%
            action, icon = '缩量', '❄️'  # 设置操作为缩量，图标为雪花
        elif rt > expand and r > 2:  # 如果当前换手率大于0.1且倍数大于2
            action, icon = '放量', '🔥'  # 设置操作为放量，图标为火焰
        if action:  # 如果有操作
            position = context.portfolio.positions[stock]  # 获取仓位
            r = close_position(position)  # 关闭仓位
            log.info(f"{action} {stock} {get_security_info(stock).display_name} 换手率:{rt:.2%}→均:{avg:.2%} 倍率:{r:.1f}x {icon} close_position: {r}")  # 输出操作日志
            g.reason_to_sell = 'limitup'  # 设置卖出原因为涨停

def order_target_value_(security, value):  # 目标持仓值函数
    if value == 0:  # 如果目标值为0
        pass  # 不执行任何操作
    else:  # 如果目标值不为0
        pass  # 不执行任何操作
    return order_target_value(security, value)  # 返回目标持仓值订单

def open_position(security, value):  # 开仓函数
    order = order_target_value_(security, value)  # 执行目标持仓值订单
    if order != None and order.filled > 0:  # 如果订单存在且成交数量大于0
        return True  # 返回True
    return False  # 返回False

def close_position(position):  # 平仓函数
    security = position.security  # 获取证券代码
    order = order_target_value_(security, 0)  # 执行平仓订单
    if order != None:  # 如果订单存在
        if order.status == OrderStatus.held and order.filled == order.amount:  # 如果订单状态为持有且成交数量等于订单数量
            return True  # 返回True
    return False  # 返回False

def buy_security(context,target_list,cash=0,buy_number=0):  # 买入证券函数
    position_count = len(context.portfolio.positions)  # 获取当前持仓数量
    target_num = g.stock_num  # 获取目标持仓数量
    if cash == 0:  # 如果现金为0
        cash = context.portfolio.total_value  # 使用总资产
    if buy_number == 0:  # 如果买入数量为0
        buy_number = target_num  # 使用目标持仓数量
    bought_num = 0  # 初始化已买入数量
    log.debug(f"计划买入{buy_number}只股票")  # 输出买入计划日志
    if target_num > position_count:  # 如果目标数量大于当前持仓数量
        value = cash / target_num  # 计算每只股票的买入金额
        for stock in target_list:  # 遍历目标股票列表
            if context.portfolio.positions[stock].total_amount == 0:  # 如果持仓数量为0
                if bought_num < buy_number:  # 如果已买入数量小于计划买入数量
                    if open_position(stock, value):  # 开仓成功
                        g.not_buy_again.append(stock)  # 添加到不重复买入列表
                        bought_num += 1  # 已买入数量加1
                        log.info(f"成功买入最优股票：{stock}")  # 输出买入成功日志
                        if len(context.portfolio.positions) == target_num:  # 如果持仓数量达到目标数量
                            break  # 跳出循环

def close_account(context):  # 收盘函数
    if g.no_trading_today_signal == True:  # 如果今日交易信号为True
        if len(g.hold_list) != 0 and g.no_trading_hold_signal == False:  # 如果持仓列表不为空且不交易持有信号为False
            for stock in g.hold_list:  # 遍历持仓股票
                position = context.portfolio.positions[stock]  # 获取仓位
                if close_position(position):  # 平仓成功
                    log.info(f"卖出{stock}")  # 输出卖出日志
                else:  # 平仓失败
                    log.info(f"卖出{stock}错误")  # 输出卖出错误日志
            buy_security(context, g.no_trading_buy)  # 买入不交易股票
            g.no_trading_hold_signal = True  # 设置不交易持有信号为True

def close_no_trading_hold(context):  # 关闭不交易持有函数
    if g.no_trading_hold_signal == True:  # 如果不交易持有信号为True
        for stock in g.hold_list:  # 遍历持仓股票
            position = context.portfolio.positions[stock]  # 获取仓位
            close_position(position)  # 平仓
            log.info(f"卖出{stock}")  # 输出卖出日志
        g.no_trading_hold_signal = False  # 设置不交易持有信号为False

# ======================================
# B方案（ETF动量策略）所有函数
# ======================================
def rebalance_logic(context):
    """
    每日调仓逻辑函数，在每天9:40被调用
    """
    log.info("--- {} 触发每日调仓逻辑 ---".format(context.current_dt))
    
    # 1. 计算ETF排名，确定目标ETF
    ranked_etfs = get_etf_rank(context, g.etf_pool)
    
    # 如果有符合条件的ETF，选择排名第一的；否则，选择避险资产
    if ranked_etfs:
        target_etf = ranked_etfs[0]
        log.info(f"动量计算完成，目标ETF为: {get_security_info(target_etf).display_name} ({target_etf})")
    else:
        # target_etf = g.safe_haven_etf
        # log.info(f"无符合动量条件的ETF，切换至避险资产: {get_security_info(target_etf).display_name} ({target_etf})")
        target_etf = None
        log.info(f"无符合动量条件的ETF")

    # 执行调仓
    execute_rebalance(context, target_etf)

def check_rebalance(context):
    """
    检查调仓是否成功，若未成功则重新执行调仓
    """
    log.info("--- {} 触发调仓检查逻辑 ---".format(context.current_dt))
    
    # 重新计算ETF排名，使用最新的价格数据
    ranked_etfs = get_etf_rank(context, g.etf_pool)
    
    if ranked_etfs:
        target_etf = ranked_etfs[0]
        log.info(f"检查时的目标ETF为: {get_security_info(target_etf).display_name} ({target_etf})")
    else:
        target_etf = None
        log.info(f"检查时无符合动量条件的ETF")

    # 检查当前持仓是否与目标一致
    current_positions = list(context.portfolio.positions.keys())
    
    # 如果目标ETF已经是唯一持仓，则无需调仓
    if target_etf is not None:
        if len(current_positions) == 1 and current_positions[0] == target_etf:
            log.info(f"目标 {target_etf} 已满仓持有，调仓成功。")
            return
        else:
            log.info(f"持仓与目标不符，执行二次调仓。")
    else:
        if len(current_positions) == 0:
            log.info("当前为空仓，调仓成功。")
            return
        else:
            log.info("应为空仓但仍有持仓，执行二次调仓。")

    # 执行调仓
    execute_rebalance(context, target_etf)

def execute_rebalance(context, target_etf):
    """
    执行实际的调仓操作
    """
    current_positions = list(context.portfolio.positions.keys())
    
    # 如果目标ETF已经是唯一持仓，则无需调仓
    if len(current_positions) == 1 and current_positions[0] == target_etf:
        log.info(f"目标 {target_etf} 已满仓持有，无需调仓。")
        return

    # 卖出非目标资产
    for security in current_positions:
        if security != target_etf:
            log.info(f"卖出非目标资产: {get_security_info(security).display_name} ({security})")
            order_target(security, 0) # 卖出全部仓位
            
    # 买入目标资产
    # 使用 order_target_value 将全部资产配置到目标ETF上
    # 聚宽会自动处理卖出后资金到账和计算买入数量的过程
    if target_etf:
        log.info(f"将全部资产调仓至目标ETF: {get_security_info(target_etf).display_name} ({target_etf})")
        order_target_value(target_etf, context.portfolio.total_value)
    else:
        log.info("保持空仓状态")

def get_etf_rank(context, etf_pool):
    """
    计算ETF池中各ETF的动量得分并排名
    """
    data = pd.DataFrame(index=etf_pool, columns=["annualized_returns", "r2", "score"])
    print_data = {}
    filtered_out = []  # 记录被过滤掉的ETF
    high_score_etfs = []  # 记录超过阈值的ETF

    for etf in etf_pool:
        # 获取过去 g.m_days 的收盘价历史数据
        prices = attribute_history(etf, g.m_days, '1d', ['close'])['close']
        
        if prices.empty or len(prices) < g.m_days:
            continue

        # 核心计算逻辑：加权线性回归
        y = np.log(prices.values)
        x = np.arange(len(y))
        weights = np.linspace(1, 2, len(y))

        try:
            # 计算年化收益率
            slope, intercept = np.polyfit(x, y, 1, w=weights)
            annualized_returns = math.exp(slope * 250) - 1
            data.loc[etf, "annualized_returns"] = annualized_returns

            # 计算R2
            ss_res = np.sum(weights * (y - (slope * x + intercept)) ** 2)
            ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot else 0
            data.loc[etf, "r2"] = r2

            # 计算得分
            score = annualized_returns * r2
            data.loc[etf, "score"] = score
            
            # 更新最大观察到的分数
            if score > g.max_observed_score:
                g.max_observed_score = score
                
            etf_name = get_security_info(etf).display_name
            print_data[etf_name] = score

        except Exception as e:
            log.warning(f"计算ETF {etf} 得分时出错: {e}")
            continue

    # 过滤不符合条件的ETF，并按得分降序排列
    data.dropna(inplace=True)
    
    # 分别应用上下限过滤
    above_min = data.query(f"score >= {g.min_score}")
    below_max = data.query(f"score <= {g.max_score}")
    
    # 检查是否有ETF超过阈值
    above_threshold = data.query(f"score > {g.max_score}")
    
    if len(above_threshold) > 0:
        # 有ETF超过阈值，执行新逻辑
        high_score_etfs = above_threshold.index.tolist()
        log.info(f"发现 {len(high_score_etfs)} 只ETF超过阈值: {[get_security_info(etf).display_name for etf in high_score_etfs]}")
        
        # 计算前一天的分数
        yesterday_scores = {}
        for etf in high_score_etfs:
            # 获取过去 g.m_days+1 天的收盘价历史数据，以便计算t-1日分数
            prices = attribute_history(etf, g.m_days + 1, '1d', ['close'])['close']
            
            if prices.empty or len(prices) < g.m_days + 1:
                continue

            # 计算t-1日的分数（使用前g.m_days天的数据）
            yesterday_prices = prices[:-1]  # 去掉最后一天，保留前g.m_days天
            
            # 核心计算逻辑：加权线性回归
            y = np.log(yesterday_prices.values)
            x = np.arange(len(y))
            weights = np.linspace(1, 2, len(y))

            try:
                # 计算年化收益率
                slope, intercept = np.polyfit(x, y, 1, w=weights)
                annualized_returns = math.exp(slope * 250) - 1

                # 计算R2
                ss_res = np.sum(weights * (y - (slope * x + intercept)) ** 2)
                ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
                r2 = 1 - ss_res / ss_tot if ss_tot else 0

                # 计算t-1日得分
                yesterday_score = annualized_returns * r2
                yesterday_scores[etf] = yesterday_score
                
            except Exception as e:
                log.warning(f"计算ETF {etf} t-1日得分时出错: {e}")
                continue
        
        # 更新g.yesterday_scores
        g.yesterday_scores = yesterday_scores.copy()
        
        # 根据新逻辑筛选ETF
        qualified_etfs = []
        for etf in high_score_etfs:
            if etf in g.yesterday_scores:
                t_day_score = data.loc[etf, "score"]
                t_minus_1_score = g.yesterday_scores[etf]
                
                # 检查t日分数是否大于等于t-1日分数的1.5倍
                if t_day_score >= t_minus_1_score * g.score_threshold_multiplier:
                    qualified_etfs.append(etf)
                    log.info(f"{get_security_info(etf).display_name}({etf}): t日分数 {t_day_score:.4f} >= t-1日分数 {t_minus_1_score:.4f} * {g.score_threshold_multiplier} = {t_minus_1_score * g.score_threshold_multiplier:.4f}")
                else:
                    log.info(f"{get_security_info(etf).display_name}({etf}): t日分数 {t_day_score:.4f} < t-1日分数 {t_minus_1_score:.4f} * {g.score_threshold_multiplier} = {t_minus_1_score * g.score_threshold_multiplier:.4f}，不进入排名")
            else:
                # 如果没有前一天的分数，则使用原逻辑
                qualified_etfs.append(etf)
        
        # 对符合条件的ETF按分数降序排列
        if qualified_etfs:
            qualified_data = data.loc[qualified_etfs]
            valid_data = qualified_data.sort_values(by="score", ascending=False)
        else:
            # 如果没有符合条件的ETF，按照原逻辑处理
            valid_data = data.query(
                f"{g.min_score} <= score <= {g.max_score}"  # 仅保留符合范围的ETF
            ).sort_values(by="score", ascending=False)
    else:
        # 没有ETF超过阈值，沿用原逻辑
        valid_data = data.query(
            f"{g.min_score} <= score <= {g.max_score}"  # 仅保留符合范围的ETF
        ).sort_values(by="score", ascending=False)

    # 输出过滤信息
    if high_score_etfs:
        log.info(f"超过阈值({g.max_score})的ETF: {', '.join([f'{get_security_info(etf).display_name}({etf})' for etf in high_score_etfs])}")

    # 打印排名靠前的ETF
    top_etfs_info = []
    for etf_code in valid_data.index.tolist():
        etf_name = get_security_info(etf_code).display_name
        score_val = print_data.get(etf_name, 'N/A')
        top_etfs_info.append(f"{etf_name} ({score_val:.4f})")
        
    log.info("ETF动量评分排名: {}".format(' > '.join(top_etfs_info)))
    
    # 每周打印一次最大观察分数
    if context.current_dt.weekday() == 0:  # 周一
        log.info(f"截至今日的最大观察动量分数: {g.max_observed_score:.4f}")
        log.info(f"前一天的分数记录: {[(get_security_info(etf).display_name, g.yesterday_scores.get(etf, 'N/A')) for etf in g.yesterday_scores]}")
    
    return valid_data.index.tolist()

def log_portfolio_info(context):  # 账户信息记录函数
    """账户信息记录（整合A、B方案持仓）"""
    log.info("="*30 + " 每日账户收益统计 " + "="*30)  # 输出统计开始日志
    log.info(f"收盘总资产: {context.portfolio.total_value:.2f}")  # 输出总资产
    log.info(f"可用现金: {context.portfolio.available_cash:.2f}")  # 输出可用现金
    log.info(f"持仓市值: {context.portfolio.positions_value:.2f}")  # 输出持仓市值
    log.info(f"累计收益率: {context.portfolio.returns:.2%}")  # 输出累计收益率
    
    # 打印当前持仓（区分股票和ETF）
    if not context.portfolio.positions:  # 如果无持仓
        log.info("当前无任何持仓（股票+ETF）")  # 输出无持仓日志
    else:  # 如果有持仓
        log.info("当前持仓明细：")  # 输出持仓明细标题
        for security, position in context.portfolio.positions.items():  # 遍历持仓
            sec_type = get_security_info(security).type  # 获取证券类型
            sec_name = get_security_info(security).display_name  # 获取证券名称
            log.info(f"  类型：{sec_type} | 名称：{sec_name}({security}) | 数量：{position.total_amount} | 价值：{position.value:.2f} | 成本：{position.avg_cost:.3f}")  # 输出持仓明细
    log.info("="*75)  # 输出统计结束日志