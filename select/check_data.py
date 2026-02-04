# -*- coding: utf-8 -*-
from xtquant import xtdata
import pandas as pd

def check_data_status(stock_code):
    print(f"正在检查 {stock_code} 的本地数据...")
    
    # 尝试获取最近 10 天的数据
    # 注意：period='1d' (日线), count=10
    data = xtdata.get_market_data(
        field_list=['close'], 
        stock_list=[stock_code], 
        period='1d', 
        count=10,
        dividend_type='front'
    )
    
    # 提取 Close 数据
    close_data = data.get('close')
    
    # 验证逻辑
    if close_data is None or close_data.empty:
        print(f"❌ {stock_code}: 本地无数据 (未下载或代码错误)")
        return False
    else:
        # 获取最新的一条日期和价格
        last_date = close_data.index[-1]
        last_price = close_data.iloc[-1, 0]
        print(f"✅ {stock_code}: 数据存在!")
        print(f"   最新数据日期: {last_date}")
        print(f"   最新收盘价: {last_price}")
        print(f"   数据总条数: {len(close_data)}")
        return True

def check_financial_status(stock_code):
    print(f"正在检查 {stock_code} 的财务数据...")

    # 1. 尝试获取“每股指标”数据
    # table_list=['PershareIndex'] 是最基础的财务表
    # report_type='announce_time' 按公告日获取
    data = xtdata.get_financial_data(
        stock_list=[stock_code],
        table_list=['PershareIndex'],
        report_type='announce_time',
        start_time='', 
        end_time=''
    )

    # 2. 检查返回结果
    # get_financial_data 返回的是一个字典: { '600519.SH': DataFrame }
    stock_data = data.get(stock_code)


    if stock_data is None:
        print(f"❌ {stock_code}: 财务数据未下载 (或无数据)")
        print("   -> 请尝试运行 xtdata.download_financial_data(...)")
        return False
    else:
        # 获取最近的一条记录
        last_report = stock_data.iloc[-1]
        report_date = stock_data.index[-1] # 公告日期
        
        print(f"✅ {stock_code}: 财务数据存在!")
        print(f"   数据行数: {len(stock_data)} 行")
        print(f"   最新公告日: {report_date}")
        
        # 尝试打印几个字段看看是不是真的有值
        # 注意：不同表字段名不同，这里打印 EPS (每股收益)
        eps = last_report.get('Eps', 'N/A')
        print(f"   最新EPS: {eps}")
        return True

if __name__ == '__main__':
    # 测试一下 贵州茅台
    check_data_status('301308.SZ')
    check_financial_status('301308.SZ')
    
    # 测试一个不存在的代码，看看效果
    check_data_status('999999.SH')