import akshare as ak
import pandas as pd
import sqlite3
import datetime
from pandas.api.types import is_numeric_dtype, is_string_dtype

def save_dataframe_to_sqlite(df:pd.DataFrame , table_name: str, db_name="fund_data.db", append = False):
    print(df.columns.tolist()) # 查看所有列名
    print(df.head())           # 查看前5行数据
    
    try:
        today = datetime.date.today().strftime("%Y-%m-%d")
        df['update_date'] = today
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists= 'append' if append else 'replace', index=False)
        print(f"成功！已将 {len(df)} 条基金数据存入数据库 [{db_name}] 的表 [{table_name}] 中。")
        print(f"当前更新日期：{today}")
        conn.close()
    except Exception as e:
        print(f"运行出错: {e}")

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
        col = '手续费'
        if col in df.columns:
            df[col].replace('---', None)
            df[col] = df[col].str.replace('%', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
        save_dataframe_to_sqlite(df, 'fund', db_name)
    except Exception as e:
        print(f"运行出错: {e}")

def save_manager_info_to_sqlite(db_name="fund_data.db"):
    print(">>> 正在拉取基金经理大名单...")
    try:
        # 1. 获取原始数据
        df = ak.fund_manager_em()
        if df is None:
            print("未能获取到数据，请检查网络。")
            return
        
        df['累计从业时间'] = df['累计从业时间'].astype(str).str.extract(r'(\d+)').astype(float)
        df['现任基金资产总规模'] = df['现任基金资产总规模'].astype(str).str.replace('亿元', '', regex=False)
        df['现任基金资产总规模'] = pd.to_numeric(df['现任基金资产总规模'], errors='coerce')
        df['现任基金最佳回报'] = df['现任基金最佳回报'].astype(str).str.replace('%', '', regex=False)
        df['现任基金最佳回报'] = pd.to_numeric(df['现任基金最佳回报'], errors='coerce')

        save_dataframe_to_sqlite(df, 'manager', db_name)
    except Exception as e:
        print(f"入库失败: {e}")

def save_rating_info_to_sqlite(db_name="fund_data.db"):
    print(">>> 正在拉取全市场基金评级数据 (ak.fund_rating_all)...")
    try:
        # 1. 获取原始数据
        # 注意：如果 ak.fund_rating_all() 报错，请尝试 ak.fund_rating_all_em()
        df = ak.fund_rating_all()
        
        if df is None or df.empty:
            print("未能获取到评级数据。")
            return

        numeric_cols = ['5星评级家数', '上海证券', '招商证券', '济安金信', '晨星评级']
        for col in numeric_cols:
            if col in df.columns:
                # 将 "---" 或其他非标字符替换为 NaN，并转为浮点数
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if '手续费' in df.columns:
            df['手续费'] = df['手续费'].astype(str).str.replace('%', '', regex=False)
            df['手续费'] = pd.to_numeric(df['手续费'], errors='coerce')

        save_dataframe_to_sqlite(df, 'rating', db_name )
    except Exception as e:
        print(f"入库失败: {e}")

def batch_save_fund_analysis(fund_codes, db_name="fund_research.db"):
    """
    批量获取基金风险指标（夏普、波动、回撤等）并存入 SQLite
    """
    all_dfs = []  # 用于存放每只基金的结果
    
    conn = sqlite3.connect(db_name)
    
    print(f">>> 开始抓取 {len(fund_codes)} 只基金的风险指标...")
    
    for i, code in enumerate(fund_codes):
        # 补齐6位字符串代码
        clean_code = str(code).zfill(6)
        print(f"[{i+1}/{len(fund_codes)}] 正在处理: {clean_code}")
        
        try:
            # 1. 获取单只基金的风险分析数据
            df = ak.fund_individual_analysis_xq(symbol=clean_code)
            
            if df is not None and not df.empty:
                # 2. 在第一列插入基金代码
                df.insert(0, '基金代码', clean_code)
                
                # 3. 强制转换数据类型
                # 字符串列：基金代码、周期
                # 数字列：较同类风险收益比, 较同类抗风险波动, 年化波动率, 年化夏普比率, 最大回撤
                numeric_cols = ['较同类风险收益比', '较同类抗风险波动', '年化波动率', '年化夏普比率', '最大回撤']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                all_dfs.append(df)
            
            # 4. 控制频率，防止被封 IP
            time.sleep(1.2) 
            
        except Exception as e:
            print(f"基金 {clean_code} 抓取失败: {e}")
            continue

    # 5. 合并所有 DataFrame
    if all_dfs:
        big_df = pd.concat(all_dfs, ignore_index=True)
        
        # 6. 存入 SQLite 表 fund_analysis
        # 如果表已存在则替换 (replace)；如果要保留历史快照，建议手动加日期列并用 append
        big_df.to_sql('fund_analysis', conn, if_exists='replace', index=False)
        
        print("\n>>> 任务完成！")
        print(f">>> 最终数据表行数: {len(big_df)}")
        print(f">>> 数据预览：\n{big_df.head(6)}") # 展示前两只基金的数据
    else:
        print(">>> 未收集到任何有效数据。")
        
    conn.close()

def get_unique_fund_codes(db_name="fund_data.db"):
    """
    从指定的 SQLite 数据库中获取去重后的基金代码
    """
    try:
        # 1. 建立数据库连接
        conn = sqlite3.connect(db_name)
        
        # 2. 编写 SQL 语句：使用 DISTINCT 关键字对 "基金代码" 字段去重
        # 注意：由于字段名是中文，SQL 语句中建议加上双引号
        sql_query = 'SELECT DISTINCT "基金代码" FROM fund'
        
        # 3. 使用 pandas 读取数据，效率高且易于处理
        df = pd.read_sql(sql_query, conn)
        
        # 4. 关闭连接
        conn.close()
        
        # 5. 将结果转换为列表格式
        print(df.head)
        fund_list = df["基金代码"].tolist()
        
        print(f"成功提取基金代码，共计: {len(fund_list)} 只")
        return fund_list

    except Exception as e:
        print(f"数据库读取出错: {e}")
        return []
    
if __name__ == "__main__":
    # save_fund_rank_to_sqlite(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')
    # save_manager_info_to_sqlite(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')
    # save_rating_info_to_sqlite(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')
   
    #get_unique_fund_codes(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')
    print(ak.fund_individual_analysis_xq(symbol="010415"))
    print(ak.fund_individual_achievement_xq(symbol="010415"))
    print(ak.fund_individual_detail_hold_xq(symbol="010415"))