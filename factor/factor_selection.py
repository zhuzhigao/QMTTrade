# 40% 基本面 + 40% 动量/趋势 + 20% 情绪/风控

# 策略核心逻辑 (仅依赖 xtdata)
# 基本面 (40%)：
# PE (市盈率) = 当前股价 / 每股收益(EPS)。逻辑：数值越小越好（价值）。
# PB (市净率) 或 ROE = 当前股价 / 每股净资产 或 EPS / 每股净资产。逻辑：ROE 越大越好（质量）。
# 数据源：xtdata.get_financial_data 中的 PershareIndex（每股指标表）。

# 动量 (40%)：
# RPS (20日涨幅)。逻辑：数值越大越好（趋势）。
# ADTM (趋势强度)。逻辑：数值越大越好。

# 风控 (20%)：
# Volatility (波动率)。逻辑：数值越小越好（稳健）。
# BIAS (乖离率)。逻辑：数值越小（越接近均线）越好（防追高）。

# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import numpy as np
from xtquant import xtdata
import stock_candidates as sc

# 权重配置
W_FUND = 0.4  # 基本面
W_MOM  = 0.4  # 动量
W_RISK = 0.2  # 风控

__all__ = ['select']

# ================= 2. 工具函数 =================

def get_stock_list_from_sector(sector):
      
    """获取板块成分股"""
    try:
        stocks = xtdata.get_stock_list_in_sector(sector)
        return stocks[:50]
    except:
        # 备用测试列表
        return []
    
def progress_callback(data):
    """
    data: dict, e.g., {'finished': 10, 'total': 100}
    """
    finished = data['finished']
    total = data['total']
    percent = (finished / total) * 100 if total > 0 else 0
    print(f"Download Progress: {finished}/{total} ({percent:.2f}%)")
    
    if finished == total:
        print("Download completed!")

def check_and_download_data(stock_list, mdays = 60):
    """强制下载所需数据"""
    # 计算开始时间 (例如过去60天)
    import datetime
    start_date = (datetime.datetime.now() - datetime.timedelta(days=mdays*2)).strftime("%Y%m%d")
    
    print(f">> 正在下载行情数据 (从 {start_date} 开始)...")
    
    # 修复点：使用循环逐个下载
    xtdata.download_history_data2(stock_list, period='1d', start_time=start_date, end_time='', callback=progress_callback)
    
    print(">> 正在下载财务数据...")
    # 财务数据通常支持列表下载，但如果也报错，同样改为循环
    try:
        xtdata.download_financial_data2(stock_list, table_list=[
            # 'Balance'          #资产负债表
            # 'Income'           #利润表
            # 'CashFlow'         #现金流量表
            # 'Capital'          #股本表
            # 'Holdernum'        #股东数
            # 'Top10holder'      #十大股东
            # 'Top10flowholder'  #十大流通股东
            'Pershareindex'    #每股指标
        ], callback=progress_callback)
    except TypeError:
        # 如果财务下载也报同样的错，启用下面的备用方案
        for stock in stock_list:
             xtdata.download_financial_data([stock])

# ================= 3. 核心计算逻辑 (已修复转置问题) =================
def calculate_factors(stock_list, sdays = 20, mdays = 60):
    """
    计算因子核心函数 (Updated)
    逻辑: 40%基本面 + 40%动量 + 20%风控
    """
    print(f">> 开始计算 {len(stock_list)} 只股票的因子...")
    
    # ================= 1. 获取行情数据 (Technical) =================
    # 获取收盘价，用于计算动量、波动率、乖离率以及估值(PE)
    market_data = xtdata.get_market_data(
        field_list=['close'], 
        stock_list=stock_list, 
        period='1d', 
        count=mdays + sdays, 
        dividend_type='front' # 前复权
    )
    
    # 提取 close 数据表
    df_close = market_data.get('close')
    
    # [修复1] 空数据检查
    if df_close is None or df_close.empty:
        print("❌ 错误：未获取到行情数据，请检查 MiniQMT 是否登录。")
        return pd.DataFrame()

    # [修复2] 维度转置自适应
    # 我们需要的格式是：行(Index)=日期, 列(Columns)=股票代码
    # 如果发现股票代码跑到了 Index 上，就转置一下
    if len(stock_list) > 0 and stock_list[0] in df_close.index:
        print("   (检测到数据需要转置: Rows=Stocks -> Rows=Dates)")
        df_close = df_close.T

    # ================= 2. 获取财务数据 (Fundamental) =================
    # 使用 PershareIndex 表
    financial_data = xtdata.get_financial_data(
        stock_list, 
        table_list=['PershareIndex'], 
        report_type='announce_time' # 避免未来函数
    )

    # ================= 3. 逐个股票计算因子 =================
    data_dict = {
        'code': [],
        'R_PE': [],   # 估值 (基本面)
        'R_ROE': [],  # 质量 (基本面)
        'R_Mom_Short': [],  # 动量20 (技术面)
        'R_Mom_Mid': [],  # 动量60 (技术面)
        'R_Vol': [],  # 波动 (风控)
        'R_Bias': []  # 乖离 (风控)
    }

    for stock in stock_list:
        try:
            # --- A. 技术面计算 (基于 df_close) ---
            
            # 安全获取该股序列
            if stock not in df_close.columns: continue
            closes_series = df_close[stock]
            
            # 转 numpy 数组并清洗 NaN
            closes = closes_series.dropna().values
            
            # 长度不够无法计算 MA20
            if len(closes) < mdays: continue
            
            current_price = closes[-1]
            if current_price <= 0: continue # 停牌或数据错误

            # 1. 动量 (Momentum): 20日涨幅
            # 逻辑: 过去20天涨了多少
            mom_short = current_price / closes[-sdays] - 1
            mom_mid = current_price / closes[-mdays] - 1

            # 2. 波动率 (Volatility): 20日收益率标准差
            # 逻辑: 涨跌越平稳越好
            returns = np.diff(closes) / closes[:-1]
            vol = np.std(returns[-sdays:])

            # 3. 乖离率 (Bias): 偏离20日均线的程度
            # 逻辑: 防止追高，取绝对值看偏离度
            ma = np.mean(closes[-sdays:])
            bias = abs((current_price - ma) / ma)

            # --- B. 基本面计算 (基于 financial_data) ---
            
            fin_df = financial_data.get(stock)['PershareIndex']
            
            # 初始化默认“烂”分
            pe = 999.0   # PE越小越好，给个大数
            roe = -99.0  # ROE越大越好，给个小数
            
            # [修复3] 严谨判断：非None、非空字典、非空DataFrame
            if fin_df is not None and isinstance(fin_df, pd.DataFrame) and not fin_df.empty:
                last_report = fin_df.iloc[-1]
                
                # [修复4] 使用您提供的准确字段名
                # s_fa_eps_basic: 基本每股收益
                # equity_roe:     净资产收益率
                # s_fa_bps:       每股净资产 (备用)
                
                eps = last_report.get('s_fa_eps_basic', 0)
                real_roe = last_report.get('equity_roe', -99) # 优先用现成字段
                
                # --- 计算 PE (市盈率) ---
                # 处理空值和亏损
                if pd.isna(eps): eps = 0
                
                if eps > 0:
                    pe = current_price / eps
                else:
                    pe = 999.0 # 亏损股，估值打分极差
                
                # --- 计算 ROE (净资产收益率) ---
                if pd.notna(real_roe) and real_roe != -99:
                    # 如果数据库里有 equity_roe，直接用
                    roe = real_roe
                else:
                    # 备用方案：手动算 EPS / BPS
                    bps = last_report.get('s_fa_bps', 0)
                    if bps > 0:
                        roe = (eps / bps) * 100 # 统一为百分比量级
                    else:
                        roe = -99
            
            # --- C. 存入结果 ---
            data_dict['code'].append(stock)
            data_dict['R_PE'].append(pe)
            data_dict['R_ROE'].append(roe)
            data_dict['R_Mom_Short'].append(mom_short)
            data_dict['R_Mom_Mid'].append(mom_mid)
            data_dict['R_Vol'].append(vol)
            data_dict['R_Bias'].append(bias)

        except Exception as e:
            # 仅在调试时打印错误，防止刷屏
            # print(f"Skipped {stock}: {e}")
            pass

    # 转换为 DataFrame 并设置 index
    df_result = pd.DataFrame(data_dict)
    if not df_result.empty:
        df_result.set_index('code', inplace=True)
        
    return df_result

# ================= 4. 打分与排序 =================

def get_dynamic_weights(sentiment):
    """根据环境返回 4-4-2 微调权重"""
    W_FUND = 0.4
    W_MOM  = 0.4
    W_RISK = 0.2
    if sentiment == 1: # 牛市：进攻，调高动量
        return {'momentum': 0.5, 'fundamental': 0.3, 'risk': 0.2}
    if sentiment == 2: # 熊市：防御，调高风控
        return {'momentum': 0.3, 'fundamental': 0.3, 'risk': 0.4}
    return {'momentum': W_MOM, 'fundamental': W_FUND, 'risk': W_RISK} # 震荡/默认


def filter_outliers_mad(df, columns, n=3):
    """
    MAD (Median Absolute Deviation) 去极值法
    逻辑：把超过 中位数 +/- n * (1.4826 * MAD) 的数据强制拉回边界
    """
    df_fix = df.copy()
    for col in columns:
        x = df_fix[col]
        median = x.median()  # 中位数
        # 计算 MAD: |x - median| 的中位数
        mad = (x - median).abs().median()
        
        # 1.4826 是为了使 MAD 在正态分布下等价于标准差
        threshold = n * (1.4826 * mad)
        
        low = median - threshold
        high = median + threshold
        
        # 缩尾处理
        df_fix[col] = x.clip(low, high)
    return df_fix

def standardize_mad(df):
    """
    基于中位数的稳健标准化 (Robust Standardization)
    代替原来的 (x - mean) / std
    """
    # 同样使用中位数和 MAD 来进行标准化
    df_z = df.copy()
    for col in df.columns:
        median = df[col].median()
        mad = (df[col] - median).abs().median()
        # 防止 mad 为 0 导致除法错误
        if mad == 0: mad = 1e-6
        
        df_z[col] = (df[col] - median) / (1.4826 * mad)
    return df_z


def scoring(df, usesector, sentiment: int):
    if df.empty: return df
    
    # 1. 去极值 & 标准化 (Z-Score)
    # 这一步是为了让 PE(倍数) 和 ROE(百分比) 能加在一起

    # 2. 用 MAD 去极值 (推荐 n=3)
    cols = ['R_PE', 'R_ROE', 'R_Mom_Short', 'R_Mom_Mid', 'R_Vol', 'R_Bias']
    df_clean = filter_outliers_mad(df, columns=cols, n=3)
    
    # 3. 用 MAD 逻辑进行标准化
    df_z = standardize_mad(df_clean)
       
    # 2. 计算综合得分 (注意符号方向)
    # PE:   越低越好 -> 取负号 (-)
    # ROE:  越高越好 -> 取正号 (+)
    # Mom:  越高越好 -> 取正号 (+)
    # Vol:  越低越好 -> 取负号 (-)
    # Bias: 越低越好 -> 取负号 (-)   
    weights = get_dynamic_weights(sentiment)
    
    
    if not usesector:
        # 1. 基本面：质量优先 (ROE 权重加大)
        df['score_fund'] = 0.3 * (-df_z['R_PE']) + 0.7 * df_z['R_ROE']

        # 2. 动量：不仅要看涨幅，还要看是否站稳 (可选：加入成交量得分)
        df['score_mom'] = 0.6 * df_z['R_Mom_Short'] + 0.4 * df_z['R_Mom_Mid']

        # 3. 风控：降低乖离率的权重，避免错过强势启动股
        # 波动率 (-0.7) 依然重要，但乖离率 (-0.3) 只要不是太离谱即可
        df['score_risk'] = 0.7 * (-df_z['R_Vol']) + 0.3 * (-df_z['R_Bias'])
    else:
        df['score_fund'] = 0.5 * (-df_z['R_PE']) + 0.5 * df_z['R_ROE']
        df['score_mom'] = 0.6 * df_z['R_Mom_Short'] + 0.4 * df_z['R_Mom_Mid']
        df['score_risk'] = 0.5 * (-df_z['R_Vol']) + 0.5 * (-df_z['R_Bias'])


    df['Total_Score'] = (weights['fundamental'] * df['score_fund']) + \
                        (weights['momentum'] *  df['score_mom']) + \
                        (weights['risk'] * df['score_risk'])
                        
    return df.sort_values(by='Total_Score', ascending=False)


# == Main entry for selection ========
# 参数：
# stock_pool: 股票池
# top_n: 选多少只
# download: 是否下载数据
# usesector：是否使用类似沪深300之类的股票池子
# sdays： 短线看多少天
# mdays: 中线看多少天
# sentiment:市场状态：牛市1，熊市2还是震荡3
def select (stock_pool, sector, top_n = 10, download = True, sdays = 20, mdays= 60, sentiment = 3):
    usesector = stock_pool is None or len(stock_pool) == 0
    if usesector:
        stock_pool = get_stock_list_from_sector(sector)

    # 2. 下载数据 (首次运行可能较慢)
    if download:
        check_and_download_data(stock_pool, mdays)
    
    # 3. 计算因子
    df_factors = calculate_factors(stock_pool, sdays, mdays)
    print(f"成功计算 {len(df_factors)} 只股票的因子")
    
    # 4. 打分排序
    df_result = scoring(df_factors, usesector, sentiment)
    
    # 5. 输出结果
    if not df_result.empty:
        print("\n[选股结果 Top 10]")
        # 打印展示列：总分、PE(估值)、ROE(质量)、Mom(动量)
        names = [xtdata.get_instrument_detail(code).get('InstrumentName', '未知') for code in df_result.index]
        df_result.insert(0, 'name', names)
        print(df_result[['name', 'R_PE', 'R_ROE', 'R_Mom_Short','R_Mom_Mid', 'R_Vol', 'R_Bias', 'score_fund', 'score_mom', 'score_risk', 'Total_Score']])
        return  df_result.iloc[:top_n, [0]]
    return []


#================= 主程序入口 =================
if __name__ == '__main__':    
    print("=== 启动 xtquant 原生选股策略 ===")
    
    usesector = False
    download = True
    stock_pool = STOCKS = ['301308.SZ', '603986.SH', '002920.SZ', '002555.SZ', '601919.SH', '601857.SH', '601788.SH', '600887.SH', '601898.SH', '600886.SH', '600900.SH', '688981.SH', '688126.SH', '002371.SZ', '002202.SZ', '601633.SH', '300750.SZ', '002594.SZ', '601360.SH', '601601.SH', '601600.SH', '600941.SH', '601988.SH', '600050.SH', '300274.SZ']

    # 1. 获取名单
    if usesector:
        stock_pool = get_stock_list_from_sector( '上证A股' )

    # ================= 1. 参数配置 =================
    # 股票池：默认沪深300 (也可改为 '上证50', '中证500')
    sentiment = 2
    selected = select(stock_pool, "", 10, download, 10, 30, sentiment)
    
    if len(selected) > 0:
        print(f"\n最终选股列表: {selected}")
    else:
        print("未生成有效结果，请检查数据下载是否成功。")