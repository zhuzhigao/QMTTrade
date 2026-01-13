# coding=utf-8
import pandas as pd
import os
from xtquant import xtdata

# ================= 配置区域 =================
CSV_FILE = 'siminput.csv'
# ===========================================

def update_csv_prices():
    # 1. 检查文件是否存在
    if not os.path.exists(CSV_FILE):
        print(f"❌ 错误：找不到文件 {CSV_FILE}")
        return

    print(f"📂 正在读取 {CSV_FILE} ...")
    
    # 2. 读取 CSV (兼容 utf-8 和 utf-8-sig)
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
    except Exception as e:
        print(f"读取失败，尝试使用 GBK 编码... ({e})")
        df = pd.read_csv(CSV_FILE, encoding='gbk')

    # 检查必要的列
    if 'stock_code' not in df.columns:
        print("❌ 错误：CSV 文件缺少 'stock_code' 列")
        return

    stock_list = df['stock_code'].tolist()
    print(f"📋 识别到 {len(stock_list)} 只股票，正在连接 QMT 获取行情...")

    # 3. 获取最新行情 (Tick 快照)
    # 注意：get_full_tick 不需要订阅，直接获取当前时刻的最新快照
    # 如果是收盘后运行，获取的就是收盘价
    ticks = xtdata.get_full_tick(stock_list)

    if not ticks:
        print("⚠️ 警告：未获取到任何行情数据。")
        print("👉 请检查：MiniQMT 客户端是否已启动并登录？")
        return

    # 4. 更新价格
    update_count = 0
    for index, row in df.iterrows():
        code = row['stock_code']
        
        if code in ticks:
            # lastPrice 是最新价（盘中）或收盘价（盘后）
            latest_price = ticks[code]['lastPrice']
            
            # 过滤掉价格为 0 的异常数据（如停牌或无效代码）
            if latest_price > 0:
                old_price = row['cost'] if 'cost' in row else 0
                df.at[index, 'cost'] = latest_price
                print(f"✅ {code}: {old_price} -> {latest_price:.2f}")
                update_count += 1
            else:
                print(f"⚠️ {code}: 获取到的价格为 0，跳过更新")
        else:
            print(f"❌ {code}: 未获取到行情数据")

    # 5. 保存回 CSV
    if update_count > 0:
        # 使用 utf-8-sig 保存，防止 Excel 打开乱码
        df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        print(f"\n🎉 更新完成！成功更新 {update_count} 只股票。")
        print(f"💾 文件已保存至：{os.path.abspath(CSV_FILE)}")
    else:
        print("\n⚠️ 没有数据被更新。")

def start():
        print(f">>> [启动检查] 正在连接行情服务...")
        
        # 1. 显式连接行情服务
        try:
            xtdata.connect(port=58609)
            # 测试一下是否连通
            xtdata.get_market_data(field_list=['close'], stock_list=['000001.SH'], period='1d', count=1)
            print(">>> [行情服务] 连接成功！")
        except Exception as e:
            print(f"\n!!! [严重错误] 无法连接 QMT 行情服务。")
            print("请确认：\n1. 金阳光 QMT 极简模式已登录\n2. 端口号是否为 58609\n")
            return # 连接失败直接退出，不要往下跑了

        mode_str = "模拟盘(Input/Current CSV)" if SIMULATION else "实盘(QMT账户 + Input CSV)"
        print(f">>> [启动策略] 模式: {mode_str}")
        
 
if __name__ == '__main__':
    #update_csv_prices()
    start()