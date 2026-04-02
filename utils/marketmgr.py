__all__ = ['MarketMgr']

import numpy as np
from xtquant import xtdata


class MarketMgr:
    """市场环境研判工具"""

    def is_monkey_market(self, stock_code='000300.SH', window=20, er_threshold=0.25, vol_threshold=0.015) -> bool:
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
