import os
import json
import datetime
from datetime import timezone, timedelta

__all__ = ['StrategyLedger', 'BlacklistManager']

BEIJING_TZ = timezone(timedelta(hours=8))

class BlacklistManager:
    """小黑屋（黑名单）管理类，用于记录被止损的股票，防止近期被重复买入"""
    
    def __init__(self, filepath = 'blacklist.json'):
        """
        初始化管理器
        :param filepath: 小黑屋 JSON 文件的存储路径
        """
        self.filepath = filepath
        # 实例化时，自动从本地加载记忆字典
        self.data = self.load()

    def load(self):
        """（内部方法）从本地 JSON 文件读取小黑屋数据"""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"--> 成功从本地恢复小黑屋记忆，当前黑名单包含 {len(data)} 只股票。")
                    return data
            except Exception as e:
                print(f"--> 读取小黑屋文件失败: {e}，将初始化为空。")
        return {}

    def save(self):
        """（内部方法）将当前小黑屋数据保存到本地 JSON 文件"""
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"--> 保存小黑屋文件失败: {e}")

    def add(self, stock_code):
        """
        将股票关进小黑屋并自动保存
        :param stock_code: 股票代码
        :param reason: 关进小黑屋的原因或时间（可用来做后续的自动释放逻辑）
        """
        # 如果不在黑名单中，才添加并触发保存
        if stock_code not in self.data:
            today_str = datetime.datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
            self.data[stock_code] = today_str
            self.save()
            print(f"--> [黑名单更新] 已将 {stock_code} 关进小黑屋")

    def remove(self, stock_code):
        """将股票从小黑屋中释放并自动保存"""
        if stock_code in self.data:
            del self.data[stock_code]
            self.save()
            print(f"--> [黑名单更新] 已将 {stock_code} 从小黑屋释放。")

    def is_blacklisted(self, stock_code):
        """检查某只股票是否在小黑屋中"""
        return stock_code in self.data

    def get_all(self):
        """获取完整的小黑屋字典"""
        return self.data
    
class StrategyLedger:
    """策略独立账本类，用于记录本策略买入的股票，实现策略隔离"""
    def __init__(self, filepath='strategy_holdings.json'):
        self.filepath = filepath
        self.holdings = self.load_ledger()

    def load_ledger(self):
        """读取账本：如果本地有记录，则加载；没有则新建空列表"""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"读取账本失败: {e}，将启用空账本")
        return []

    def save_ledger(self):
        """保存账本到本地文件"""
        with open(self.filepath, 'w', encoding='utf-8') as f:
            json.dump(self.holdings, f, ensure_ascii=False, indent=4)

    def add(self, stock_code):
        """记录买入的股票"""
        if stock_code not in self.holdings:
            self.holdings.append(stock_code)
            self.save_ledger()

    def remove(self, stock_code):
        """移除卖出的股票"""
        if stock_code in self.holdings:
            self.holdings.remove(stock_code)
            self.save_ledger()
            
    def get_all(self):
        """获取当前策略名下的所有股票"""
        return self.holdings