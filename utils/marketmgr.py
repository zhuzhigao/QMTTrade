__all__ = ['MarketMgr']

import datetime
import time
import numpy as np
from scipy import stats
from datetime import timezone, timedelta
from xtquant import xtdata

BEIJING_TZ = timezone(timedelta(hours=8))


class MarketMgr:
    """市场环境研判工具"""

    @staticmethod
    def is_monkey_market(stock_code='000300.SH', window=20, er_threshold=0.25, vol_threshold=0.015) -> bool:
        """
        判断指定标的（如大盘指数）当前是否处于"猴市"环境。

        参数:
        - stock_code: 宽基指数代码，默认沪深300 ('000300.SH')
        - window: 观察周期，默认 20 个交易日（约一个月）
        - er_threshold: 效率系数阈值，低于此值说明趋势性弱（多空来回拉锯）
        - vol_threshold: 波动率变异系数阈值，高于此值说明上下振幅大

        返回:
        - bool: True 表示处于猴市，False 表示非猴市（趋势市或极低波动的死市）
        """
        # 1. 补充下载最近的日线数据 (防止本地数据缺失)
        time.sleep(1)
        xtdata.download_history_data2([stock_code], '1d', '20260101', '')

        # 2. 从本地缓存获取最近 window + 1 天的收盘价
        data = xtdata.get_market_data(
            field_list=['close'],
            stock_list=[stock_code],
            period='1d',
            count=window + 1
        )

        # 异常处理：如果没有取到足够的数据
        if stock_code not in data['close'].index or len(data['close'].columns) < window + 1:
            print(f"!! 警告: {stock_code} 日线数据不足，无法计算猴市指标 !!")
            return False

        # 获取收盘价时间序列数组
        closes = data['close'].loc[stock_code].values

        # 3. 计算考夫曼效率系数 (ER)
        net_change = abs(closes[-1] - closes[0])
        sum_of_changes = np.sum(np.abs(np.diff(closes)))
        er = net_change / sum_of_changes if sum_of_changes != 0 else 0.0

        # 4. 计算变异系数 (CV)
        cv_volatility = np.std(closes) / np.mean(closes)

        # 5. 综合判断：没趋势 (ER < 阈值) 且 波动大 (CV > 阈值) = 猴市
        is_monkey = (er < er_threshold) and (cv_volatility > vol_threshold)

        status = "⚠️猴市(宽幅震荡)" if is_monkey else "✅非猴市(趋势或地量)"
        print(f"[{stock_code}] 考夫曼ER: {er:.4f}, 变异系数CV: {cv_volatility:.4f} -> 研判: {status}")

        return bool(is_monkey)

    @staticmethod
    def get_rsrs_signal(index_code='000300.SH', rsrs_n=18, rsrs_m=600) -> float:
        """
        计算大盘RSRS择时信号，返回标准化Z-Score。

        参数:
        - index_code: 择时基准指数，默认沪深300
        - rsrs_n: RSRS回归窗口（交易日数）
        - rsrs_m: 标准化基准天数
        """
        print(f"正在计算 {index_code} 的 RSRS 信号...")
        start_date = (datetime.datetime.now(BEIJING_TZ) - datetime.timedelta(days=rsrs_m + rsrs_n)).strftime("%Y%m%d")
        xtdata.download_history_data(index_code, period='1d', start_time=start_date, end_time='')
        xtdata.download_history_data(index_code, period='1m', start_time=datetime.datetime.now(BEIJING_TZ).strftime("%Y%m%d"), end_time='')

        data = xtdata.get_market_data_ex(['high', 'low'], [index_code], period='1d', count=rsrs_m + rsrs_n, dividend_type='front')[index_code]
        highs = data['high'].values
        lows = data['low'].values

        if len(highs) < rsrs_n + 2:
            raise ValueError(f"RSRS 数据不足：需要至少 {rsrs_n + 2} 条，实际获取 {len(highs)} 条，请检查数据下载。")

        slopes = []
        for i in range(len(highs) - rsrs_n + 1):
            slope, _, _, _, _ = stats.linregress(lows[i:i + rsrs_n], highs[i:i + rsrs_n])
            slopes.append(slope)

        if len(slopes) < 2:
            raise ValueError(f"RSRS slopes 数量不足以标准化：{len(slopes)} 个，请增大 rsrs_m 或检查数据。")

        current_slope = slopes[-1]
        history_slopes = slopes[:-1]
        z_score = (current_slope - np.mean(history_slopes)) / np.std(history_slopes)
        return z_score

    @staticmethod
    def get_market_sentiment(benchmark: str, at_date: str, sentiment_duration: int = 20) -> int:
        """
        识别市场环境。

        返回:
        - 1: 牛市（价格在均线上方 2% 以上）
        - 2: 熊市（价格在均线下方 2% 以上）
        - 3: 震荡市
        """
        market_data = xtdata.get_market_data_ex(
            field_list=['close'],
            stock_list=[benchmark],
            period='1d',
            count=sentiment_duration * 2,
            end_time=at_date,
            dividend_type='front'
        )
        index_series = market_data[benchmark]['close']
        ma20 = index_series.rolling(sentiment_duration).mean().iloc[-1]
        current_price = index_series.iloc[-1]

        if current_price > ma20 * 1.02:
            print('牛市')
            return 1
        if current_price < ma20 * 0.98:
            print('熊市')
            return 2
        print('震荡市')
        return 3
