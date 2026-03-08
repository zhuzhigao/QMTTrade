import akshare as ak
import pandas as pd
import sqlite3
import datetime

def save_fund_rank_to_sqlite(db_name="fund_data.db"):
    """
    抓取全市场基金排行数据并存入 SQLite
    """
    print("正在从东方财富获取全市场基金排行数据，请稍候...")
    
    try:
        # 1. 获取全量数据
        # 该接口包含：代码、简称、单位净值、累计净值、日增长率以及近1周~成立来所有涨跌幅
        df = ak.fund_open_fund_rank_em()

        if df is None:
            print("未能获取到数据，请检查网络。")
            return
        print(df.columns.tolist()) # 查看所有列名
        print(df.head())           # 查看前5行数据
        
        # 2. 数据清洗与预处理
        # 添加抓取日期列，方便后续做时序分析（比如查看某基金排名的变化）
        today = datetime.date.today().strftime("%Y-%m-%d")
        df['update_date'] = today
        
        # 将一些百分号或非数值字符处理为浮点数（AKShare通常已处理，但为了稳健建议检查）
        # 强制转换数值列，无法转换的转为 None (NaN)
        #numeric_cols = ['基金代码', '基金简称', '日期', '单位净值', '累计净值', '日增长率', '近1周', '近1月', '近3月', '近6月', '近1年', '近2年', '近3年', '今年来', '成立来', '手续费']
        
        # (1) 基金代码 & 基金简称：强制为字符串
        # 补齐6位，防止 000001 变成 1
        df['基金代码'] = df['基金代码'].astype(str).str.zfill(6)
        df['基金简称'] = df['基金简称'].astype(str)
        
        # (2) 日期：转换为真正的 DateTime 类型
        # 接口通常返回的是截面数据，我们添加一个“数据获取日期”列
        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
 
        # (3) 手续费：转换为百分比数值 (Float)
        if '手续费' in df.columns:
            # 替换 '---' 为空，移除 '%'，转为浮点数
            # 注意：0.15% 在数据库中会存为 0.15
            df['手续费'] = df['手续费'].replace('---', None)
            df['手续费'] = df['手续费'].str.replace('%', '', regex=False)
            df['手续费'] = pd.to_numeric(df['手续费'], errors='coerce')

        # (4) 业绩指标：全部转为数值类型 (Float)
        performance_cols = [
            '单位净值', '累计净值', '日增长率', '近1周', '近1月', 
            '近3月', '近6月', '近1年', '近2年', '近3年', '今年来', '成立来'
        ]
        for col in performance_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # (5) 整理列顺序（可选，将日期放前面方便查看）
        cols = ['序号', '日期', '基金代码', '基金简称'] + [c for c in df.columns if c not in ['序号', '日期', '基金代码', '基金简称']]
        df = df[cols]

        print(df.columns.tolist()) # 查看所有列名
        print(df.head())           # 查看前5行数据
        # 3. 写入 SQLite 数据库
        conn = sqlite3.connect(db_name)
        
        # table_name 定义为 fund_daily_rank
        # if_exists='append' 表示如果表存在就追加数据（保留历史）
        # 如果你只想保留最新的一份数据，可以改为 if_exists='replace'
        df.to_sql('fund_daily_rank', conn, if_exists='append', index=False)
        
        print(f"成功！已将 {len(df)} 条基金数据存入数据库 [{db_name}] 的表 [fund_daily_rank] 中。")
        print(f"当前更新日期：{today}")
        
        conn.close()

    except Exception as e:
        print(f"运行出错: {e}")

if __name__ == "__main__":
    save_fund_rank_to_sqlite(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')