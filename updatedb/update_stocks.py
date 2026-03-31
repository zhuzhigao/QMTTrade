# -*- coding: utf-8 -*-
"""
宽客本地因子数据库更新程序 (强固态修复版)
加入了防断连重试机制，并适配了最新的财务字段名称
"""

import akshare as ak
import pandas as pd
import sqlite3
import datetime
from datetime import timezone, timedelta
import os
import time

# ================= 1. 基础配置与网络防断装甲 =================
DB_DIR = r'C:\Users\xiusan\OneDrive\Investment\Quant_data'
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)
DB_PATH = os.path.join(DB_DIR, 'stock_data.db')
BEIJING_TZ = timezone(timedelta(hours=8))

def get_safe_report_dates():
    """
    【量化实盘核心】根据当前自然日，自动推算最安全的 100% 披露完毕的财报期。
    返回: (年度财报日期, 最新季度财报日期)
    """
    now = datetime.datetime.now(BEIJING_TZ)
    year = now.year
    month = now.month
    
    # ==========================================
    # 1. 计算年度报告期 (专门用于获取年度分红数据)
    # 规则：每年的 4月30日 之后，上一年的年报才算全部披露完毕。
    # ==========================================
    if month <= 4:
        # 如果是 1~4 月（例如 2026年3月），2025年报还没发完，最全的是 2024年报
        safe_annual_date = f"{year - 2}1231"
    else:
        # 如果是 5~12 月，上一年的年报已经 100% 披露完毕
        safe_annual_date = f"{year - 1}1231"
        
    # ==========================================
    # 2. 计算季度报告期 (专门用于获取 ROE、净利润等高时效性基本面)
    # ==========================================
    if month <= 4:
        # 1~4月：去年年报和今年一季报都没发完，全市场最完整的最新数据是【去年的三季报】
        safe_quarter_date = f"{year - 1}0930"
    elif month <= 8:
        # 5~8月：今年一季报（4.30截止）已全部出炉，中报没发完。最安全是【今年一季报】
        safe_quarter_date = f"{year}0331"
    elif month <= 10:
        # 9~10月：今年中报（8.31截止）已全部出炉，三季报没发完。最安全是【今年中报】
        safe_quarter_date = f"{year}0630"
    else:
        # 11~12月：今年三季报（10.31截止）已全部出炉。最安全是【今年三季报】
        safe_quarter_date = f"{year}0930"
        
    return safe_annual_date, safe_quarter_date


def get_db_connection():
    return sqlite3.connect(DB_PATH)

def format_qmt_code(code):
    code_str = str(code).zfill(6)
    if code_str.startswith(('6')):
        return f"{code_str}.SH"
    elif code_str.startswith(('0', '3')):
        return f"{code_str}.SZ"
    elif code_str.startswith(('4', '8')):
        return f"{code_str}.BJ"
    return code_str

def fetch_with_retry(func, retries=5, delay=5, **kwargs):
    """
    【核心防断连机制】
    捕获 Connection aborted 等网络崩溃，自动休眠后重连。
    实盘标配，防止无人值守时程序中断。
    """
    for i in range(retries):
        try:
            res = func(**kwargs)
            if res is not None and not res.empty:
                return res
            else:
                raise ValueError("获取到的数据为空")
        except Exception as e:
            print(f"    [网络或数据异常] 正在进行第 {i+1}/{retries} 次重连尝试... (错误: {e})")
            time.sleep(delay)
    raise RuntimeError(f"❌ 经过 {retries} 次重试后仍然失败，请稍后检查网络后再运行。")


# ================= 2. 数据获取与入库模块 =================

def update_dividend_data_to_db():
    safe_annual_date, _ = get_safe_report_dates()
    print(f"[{datetime.datetime.now(BEIJING_TZ)}] 正在获取 {safe_annual_date} 期的分红派息数据...")
    # 把固定的日期改成动态变量


    try:
        # 使用带重试的函数获取数据
        df_bonus = fetch_with_retry(ak.stock_fhps_em, date=safe_annual_date)
        
        df_bonus['qmt_code'] = df_bonus['代码'].apply(format_qmt_code)
        
        numeric_cols = ['现金分红-现金分红比例', '股息率', '每股收益', '每股净资产', '每股公积金', '每股未分配利润', '净利润同比增长']
        for col in numeric_cols:
            if col in df_bonus.columns:
                df_bonus[col] = pd.to_numeric(df_bonus[col], errors='coerce')
                
        cols = ['qmt_code'] + [c for c in df_bonus.columns if c != 'qmt_code']
        df_bonus = df_bonus[cols]
        
        conn = get_db_connection()
        df_bonus.to_sql('dividend_data', conn, if_exists='replace', index=False)
        conn.close()
        print(f"✅ 分红数据入库成功！共存入 {len(df_bonus)} 条记录。")
    except Exception as e:
        print(f"❌ 获取分红数据最终失败: {e}")

def update_financial_report_to_db():
    _, safe_quarter_date = get_safe_report_dates()
    print(f"[{datetime.datetime.now(BEIJING_TZ)}] 2. 正在获取 {safe_quarter_date} 期的深度财务报表数据...")
    # 财务数据对时效性要求更高，用季度 date
   
    try:
        df_finance = fetch_with_retry(ak.stock_yjbb_em, date=safe_quarter_date)
        df_finance['qmt_code'] = df_finance['股票代码'].apply(format_qmt_code)
        
        # 【重要修复】适应最新 AkShare 接口字段变化，改为“营业总收入”
        target_cols = [
            'qmt_code', '股票简称', 
            '每股收益', '营业总收入-营业总收入', '营业总收入-同比增长', 
            '净利润-净利润', '净利润-同比增长', 
            '每股净资产', '净资产收益率', 
            '每股经营现金流量', '销售毛利率'
        ]
        
        # 宽容模式：为了防止未来字段再变更，只提取 DataFrame 中确实存在的列
        exist_cols = [col for col in target_cols if col in df_finance.columns]
        df_clean = df_finance[exist_cols].copy()
        
        # 强制转换为数值型
        num_cols = [
            '每股收益', '营业总收入-营业总收入', '营业总收入-同比增长', 
            '净利润-净利润', '净利润-同比增长', 
            '每股净资产', '净资产收益率', 
            '每股经营现金流量', '销售毛利率'
        ]
        for col in num_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            
        conn = get_db_connection()
        df_clean.to_sql('financial_report', conn, if_exists='replace', index=False)
        conn.close()
        print(f"✅ 深度财务报表入库成功！共存入 {len(df_clean)} 条记录。")
    except Exception as e:
        print(f"❌ 获取深度财务报表最终失败: {e}")


def update_industry_data_to_db():
    """
    【新增模块】通过 AKShare 拉取全市场“申万一级行业”分类，并存入本地 SQLite
    """
    print("=" * 40)
    print("正在拉取申万全市场行业分类，由于需要遍历约30个行业，请耐心等待1-2分钟...")
    
    try:
        # 1. 获取申万一级行业列表
        industry_list_df = ak.sw_index_first_info()
        all_stocks_industry = []
        
        # 2. 遍历每个行业，获取该行业下的成分股
        for index, row in industry_list_df.iterrows():
            industry_code = row['行业代码']
            industry_name = row['行业名称']
            industry_code = str(industry_code).split('.')[0]
            try:
                # 获取该行业下的所有成分股
                cons_df = ak.index_component_sw(symbol=industry_code) 
                print(cons_df.head())
                
                if not cons_df.empty:
                    cons_df['industry'] = industry_name
                    all_stocks_industry.append(cons_df)
                    
                time.sleep(1) # 申万反爬极严，必须慢点
                
            except Exception as e:
                print(f"  - 拉取行业 [{industry_name}] 成分股时出错跳过: {e}")
                continue
                
        # 3. 合并成全市场大表
        if not all_stocks_industry:
            print("❌ 未能获取到任何行业数据！")
            return False
            
        final_df = pd.concat(all_stocks_industry, ignore_index=True)
        
        # 4. 转换 QMT 代码格式 (调用你脚本上方写好的 format_qmt_code 函数)
        final_df['qmt_code'] = final_df['证券代码'].apply(format_qmt_code)
        
        # 去重并只保留我们需要的两列
        result_df = final_df[['qmt_code', 'industry']].drop_duplicates(subset=['qmt_code'])
        
        # 5. 写入本地 SQLite 数据库
        conn = get_db_connection()
        if conn is None:
            return False
            
        result_df.to_sql('stock_industry', conn, if_exists='replace', index=False)
        
        # 建立索引以加快实盘查询速度
        cursor = conn.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_industry_qmt_code ON stock_industry (qmt_code);')
        conn.commit()
        conn.close()
        
        print(f"✅ 行业分类更新成功！共为 {len(result_df)} 只股票打上了行业标签。")
        return True
        
    except Exception as e:
        print(f"❌ 更新行业分类失败: {e}")
        return False
    
def update_audit_report_to_db(csv_path='audit_report.csv'):
    """
    【新增模块】读取从聚宽导出的 CSV 文件并更新到本地 SQLite 的 audit_report 表中
    强制清洗 pub_date 为标准日期字符串，opinion_type_id 为整数
    """
    print("=" * 40)
    print(f"正在更新审计意见表 (从本地 {csv_path} 导入)...")
    
    # 检查 CSV 文件是否存在
    if not os.path.exists(csv_path):
        print(f"❌ 找不到文件: {csv_path}")
        print("请确保已将从聚宽导出的 audit_report.csv 放在与本脚本同目录下。")
        return False
        
    try:
        # 1. 读取 CSV 数据
        df_audit = pd.read_csv(csv_path)
        
        # 2. 【核心】数据类型强制清洗
        # 将 pub_date 转换为标准的 'YYYY-MM-DD' 字符串格式，方便 SQLite 进行日期比对
        df_audit['pub_date'] = pd.to_datetime(df_audit['pub_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # 将 opinion_type_id 强制转换为数值型（如有空值填充为0，最后转为整数）
        df_audit['opinion_type_id'] = pd.to_numeric(df_audit['opinion_type_id'], errors='coerce').fillna(-1).astype(int)
        
        # 剔除那些日期转换失败（NaT/NaN）的异常行
        df_audit = df_audit.dropna(subset=['pub_date', 'qmt_code'])
        
        # 3. 写入数据库
        conn = get_db_connection()
        if conn is None:
            return False
            
        # 写入数据库，每次采用覆盖（replace）的方式
        df_audit.to_sql('audit_report', conn, if_exists='replace', index=False)
        
        # 建议：为 qmt_code 建一个索引，加快实盘策略时的查询速度
        cursor = conn.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_qmt_code ON audit_report (qmt_code);')
        conn.commit()
        conn.close()
        
        print(f"✅ 审计意见表更新成功！共清洗并导入 {len(df_audit)} 条记录。")
        return True
        
    except Exception as e:
        print(f"❌ 更新审计意见表失败: {e}")
        return False

#Todo: Download XtQuant Historical data, finance data and index weight data.

# ================= 3. 执行主入口 =================
if __name__ == '__main__':
    print("="*60)
    print("====== 宽客本地全量因子数据库更新程序启动 (实盘重试版) ======")
    print("="*60)
    
    update_dividend_data_to_db()
    time.sleep(3) # 模块间休眠
    
    update_financial_report_to_db()
    time.sleep(3) # 模块间休眠

    update_industry_data_to_db()
    
    update_audit_report_to_db()
    
    print("\n🎉 所有数据更新程序执行完毕！")