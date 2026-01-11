# coding:utf-8
from xtquant import xtdata
from xtquant import xttrader, xttype
import time

# ==========================================
# 1. 路径设置 (请务必修改为您自己的路径！！！)
# ==========================================
# 找到 MiniQMT 的 userdata_mini 文件夹路径
# 注意：文件夹路径中尽量不要有中文，如果有，前面加 r，如 r'D:\光大...'
mini_qmt_path = r'D:\光大证券金阳光QMT实盘\userdata_mini'

# 随便写个数字作为 session_id
session_id = 123456 

# ==========================================
# 2. 测试行情连接 (无需启动 QMT 也能跑)
# ==========================================
print(">>> 正在测试行情下载...")
xtdata.download_history_data('600519.SH', period='1d', start_time='20240101', end_time='20240105')
print("✅ 行情下载指令发送成功！")

data = xtdata.get_market_data(['close'], ['600519.SH'], period='1d', start_time='20240101', end_time='20240105')
if not data['close'].empty:
    print(f"✅ 成功获取到茅台数据，最新价：{data['close'].iloc[-1].values[0]}")
else:
    print("❌ 数据获取为空，请检查路径是否正确。")

# ==========================================
# 3. 测试交易连接 (必须启动 MiniQMT 极简模式)
# ==========================================
print("\n>>> 正在测试交易连接...")

# 创建交易对象
xt_trader = xttrader.XtQuantTrader(mini_qmt_path, session_id)

# 启动并连接
xt_trader.start()
connect_result = xt_trader.connect()

if connect_result == 0:
    print("🎉🎉🎉 恭喜！VSCode 已成功连接到 MiniQMT！")
    
    # 查个资产助助兴
    # 注意：这里需要填真实的资金账号，否则查不到
    # acc = xttype.StockAccount('您的资金账号')
    # assets = xt_trader.query_stock_asset(acc)
    # if assets:
    #     print(f"当前可用资金: {assets.cash}")
else:
    print("❌ 连接失败！")
    print("请检查：\n1. MiniQMT 软件是否已经打开并登录？\n2. path 路径是否指向了 userdata_mini？")