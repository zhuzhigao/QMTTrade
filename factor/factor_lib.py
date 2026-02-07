import datetime
from xtquant import xtdata

__all__ = ['get_market_sentiment', 'shift_date']

def get_market_sentiment(benchmark: str, at_date: str, sentiment_duration: int = 20):
    """识别市场环境：1-牛市, 2-熊市, 3-震荡"""    
    market_data = xtdata.get_market_data_ex(
        field_list=['close'], 
        stock_list=[benchmark], 
        period='1d', 
        count= sentiment_duration * 2,
        end_time=at_date, 
        dividend_type='front' # 前复权
    )
    # 提取 close 数据表
    index_series = market_data[benchmark]['close'] # 提取上证指数这一行，变成 Series

    # 此时 index_series 的索引是日期，值是收盘价
    # 计算 20 日均线
    ma20 = index_series.rolling(sentiment_duration).mean().iloc[-1]
    current_price = index_series.iloc[-1]
    
    # 简单判定逻辑
    if current_price > ma20 * 1.02: 
        print('牛市')
        return 1 # 牛市：价在均线上方
    if current_price < ma20 * 0.98: 
        print('熊市')
        return 2 # 熊市：价在均线下方
    print('震荡市')
    return 3 # 震荡

def shift_date(date_str, n):
    """
    根据给定的日期字符串加减 n 天
    :param date_str: 初始日期字符串，格式为 '20250101'
    :param n: 加减的天数，正数为加，负数为减
    :return: 处理后的日期字符串，格式为 '20250101'
    """
    try:
        # 1. 将字符串解析为 datetime 对象
        dt_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
        
        # 2. 使用 timedelta 进行天数加减
        new_date_obj = dt_obj + datetime.timedelta(days=n)
        
        # 3. 将结果转回字符串格式
        return new_date_obj.strftime('%Y%m%d')
    except Exception as e:
        print(f"日期转换错误: {e}")
        return date_str
    