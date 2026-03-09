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
        df.to_sql('fund', conn, if_exists='append', index=False)
        
        print(f"成功！已将 {len(df)} 条基金数据存入数据库 [{db_name}] 的表 [fund_daily_rank] 中。")
        print(f"当前更新日期：{today}")
        
        conn.close()

    except Exception as e:
        print(f"运行出错: {e}")

def save_manager_info_to_sqlite(db_path="fund_data.db"):
    print(">>> 正在拉取基金经理大名单...")
    try:
        # 1. 获取原始数据
        df = ak.fund_manager_em()
        
        # 2. 数据清洗与类型转换
        # (1) 字符串类处理
        df['姓名'] = df['姓名'].astype(str)
        df['所属公司'] = df['所属公司'].astype(str)
        df['现任基金'] = df['现任基金'].astype(str)
        # 基金代码必须补足6位字符串
        df['现任基金代码'] = df['现任基金代码'].astype(str).str.zfill(6)
        
        # (2) 数字类处理 - 序号
        df['序号'] = pd.to_numeric(df['序号'], errors='coerce')
        
        # (3) 数字类处理 - 累计从业时间 (剥离“天”字或处理空格)
        # 某些版本直接返回数字，某些带单位，这里统一处理
        df['累计从业时间'] = df['累计从业时间'].astype(str).str.extract(r'(\d+)').astype(float)
        
        # (4) 数字类处理 - 现任基金资产总规模 (剥离“亿元”等，转为 Float)
        # 匹配数字和小数点
        df['现任基金资产总规模'] = df['现任基金资产总规模'].astype(str).str.replace('亿元', '', regex=False)
        df['现任基金资产总规模'] = pd.to_numeric(df['现任基金资产总规模'], errors='coerce')
        
        # (5) 数字类处理 - 现任基金最佳回报 (剥离“%”)
        if '现任基金最佳回报' in df.columns:
            df['现任基金最佳回报'] = df['现任基金最佳回报'].astype(str).str.replace('%', '', regex=False)
            df['现任基金最佳回报'] = pd.to_numeric(df['现任基金最佳回报'], errors='coerce')

        # 3. 写入 SQLite
        conn = sqlite3.connect(db_path)
        
        # 使用 replace 确保每次都是最新的经理画像
        df.to_sql('manager', conn, if_exists='replace', index=False)
        
        print(f">>> 成功！已将 {len(df)} 位经理信息存入表 [manager]")
        print("\n--- 数据库字段类型预览 ---")
        # 验证前5行
        print(df.head())
        
        conn.close()
        
    except Exception as e:
        print(f"入库失败: {e}")

def save_rating_info_to_sqlite(db_path="fund_data.db"):
    print(">>> 正在拉取全市场基金评级数据 (ak.fund_rating_all)...")
    try:
        # 1. 获取原始数据
        # 注意：如果 ak.fund_rating_all() 报错，请尝试 ak.fund_rating_all_em()
        df = ak.fund_rating_all()
        
        if df is None or df.empty:
            print("未能获取到评级数据。")
            return

        # 2. 数据清洗与类型转换
        
        # --- 字符串类处理 ---
        df['代码'] = df['代码'].astype(str).str.zfill(6)
        df['简称'] = df['简称'].astype(str)
        df['基金经理'] = df['基金经理'].astype(str)
        df['基金公司'] = df['基金公司'].astype(str)
        df['类型'] = df['类型'].astype(str)

        # --- 数字类处理 ---
        # 需要处理的数值列
        numeric_cols = ['5星评级家数', '上海证券', '招商证券', '济安金信', '晨星评级']
        
        for col in numeric_cols:
            if col in df.columns:
                # 将 "---" 或其他非标字符替换为 NaN，并转为浮点数
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # --- 手续费处理 ---
        if '手续费' in df.columns:
            # 剥离百分号，例如 "0.15%" -> 0.15
            df['手续费'] = df['手续费'].astype(str).str.replace('%', '', regex=False)
            df['手续费'] = pd.to_numeric(df['手续费'], errors='coerce')

        # 3. 写入 SQLite
        conn = sqlite3.connect(db_path)
        
        # 存入 rating 表
        # 使用 replace 保持评级为最新状态
        df.to_sql('rating', conn, if_exists='replace', index=False)
        
        print(f">>> 成功！已将 {len(df)} 条评级信息存入表 [rating]")
        
        # 打印类型验证
        print("\n--- 数据库字段预览 (清洗后) ---")
        print(df.head())
        
        conn.close()
    except Exception as e:
        print(f"入库失败: {e}")

def batch_save_fund_analysis(fund_codes, db_path="fund_research.db"):
    """
    批量获取基金风险指标（夏普、波动、回撤等）并存入 SQLite
    """
    all_dfs = []  # 用于存放每只基金的结果
    
    conn = sqlite3.connect(db_path)
    
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

if __name__ == "__main__":
    #save_fund_rank_to_sqlite(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')
    #save_manager_info_to_sqlite(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')
    save_rating_info_to_sqlite(r'C:\Users\xiusan\OneDrive\Investment\Quant_data\fund_data.db')
   
   
    # print(ak.fund_individual_analysis_xq(symbol="675091"))
    # print(ak.fund_individual_achievement_xq(symbol="675091"))
    # print(ak.fund_individual_detail_hold_xq(symbol="675091"))