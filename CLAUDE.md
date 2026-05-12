# QMTTrade — 量化策略开发指南

## 项目概览

本项目是基于光大证券金阳光 QMT 极简模式的 A 股自动化交易系统，同时包含港股和美股的人工辅助调仓工具。

- **A 股（大陆）**：xtquant 实盘下单，账号 `47601131`，路径 `D:\光大证券金阳光QMT实盘\userdata_mini`
- **港股**：akshare 数据，仅输出调仓建议，人工操作
- **美股**：Yahoo Finance (yfinance) 数据，仅输出调仓建议，人工操作

---

## 核心约定

### 资金配置
- 每个策略的默认资产规模为 **6 万元人民币**（或对应币种等值）
- 使用 `total_budget` 变量控制策略使用资金上限，防止单策略吃掉全部现金
- 实际买入时取 `min(available_cash, total_budget)` 作为可用资金

### 持仓隔离
- 每个策略必须使用独立的 `StrategyLedger` 记录本策略买入的标的
- **只操作在自己 ledger 中的持仓**，绝不碰其他策略的持仓
- ledger 文件命名规范：`{策略目录}/{策略编号}_holdings.json`

### 状态持久化
- 使用 `StateManager` 记录需要跨重启保留的状态（已调仓月份、止损日期等）
- 状态文件命名规范：`{策略目录}/{策略编号}_state.json`

---

## 目录结构

```
QMTTrade/
├── utils/
│   ├── utilities.py    # 核心工具：StrategyLedger、StateManager、BlacklistManager、MessagePusher、DateMgr
│   ├── stockmgr.py     # StockInfo、StockMgr（财务数据、历史下载、板块成分）
│   ├── marketmgr.py    # MarketMgr（猴市检测、RSRS信号、市场情绪）
│   └── trademgr.py     # TradeMgr（等待卖出成交确认）
│
├── kj202536/           # 36号策略（参考实现）
│   ├── kj202536.py     # A股实盘版（xtquant）  ← A股策略模板
│   ├── kj202536_hk.py  # 港股建议版（akshare） ← 港股策略模板
│   └── kj202536_us.py  # 美股建议版（yfinance）← 美股策略模板
│
└── kj2025XX/           # 新策略目录（按此规范建立）
    ├── kj2025XX.py
    ├── kj2025XX_state.json    # 运行时自动生成
    └── kj2025XX_holdings.json # 运行时自动生成
```

---

## 工具类使用规范

### StrategyLedger（持仓账本）
```python
from utils.utilities import StrategyLedger
ledger = StrategyLedger(os.path.join(_base, 'kj2025XX_holdings.json'))

ledger.add('600519.SH')          # 买入时登记
ledger.remove('600519.SH')       # 卖出时注销
ledger.is_in_ledger('600519.SH') # 操作前先验证归属
```

### StateManager（状态持久化）
```python
from utils.utilities import StateManager
state = StateManager(os.path.join(_base, 'kj2025XX_state.json'), defaults={
    'monthly_adjusted_month': -1,
    'weekly_adjusted_week': -1,
    'stop_loss_date': '',
})
state.set('monthly_adjusted_month', current_month)  # 写入自动落盘
val = state.get('monthly_adjusted_month')
```

### MessagePusher（微信推送）
```python
from utils.utilities import MessagePusher
pusher = MessagePusher()
pusher.send_strategy_report(
    strategy_name='XX策略',
    buys=['600519.SH 贵州茅台 ¥1800'],
    sells=['000858.SZ 五粮液 500股'],
    extra_msg='月度调仓'
)
```

### StockMgr（行情/财务数据）
```python
from utils.stockmgr import StockMgr
StockMgr.download_history(['000300.SH'], start_time='20250101', period='1d')
info = StockMgr.query_stock('600519.SH')  # 返回 StockInfo（含ROE/PE/市值）
pool = StockMgr.query_stocks_in_sector('000300.SH')  # 指数成分股
```

### MarketMgr（市场环境研判）
```python
from utils.marketmgr import MarketMgr
z = MarketMgr.get_rsrs_signal('000300.SH')  # RSRS Z-Score，>0.5进攻，<-0.5防御
is_monkey = MarketMgr.is_monkey_market()     # 猴市检测
sentiment = MarketMgr.get_market_sentiment('000300.SH', '20250101')  # 1牛/2熊/3震荡
```

### TradeMgr（等待成交）
```python
from utils.trademgr import TradeMgr
sold_targets = {'600519.SH': 90000}  # {代码: 卖前市值}
TradeMgr.wait_for_sells(trader, account, sold_targets, timeout=120, interval=5)
```

---

## A 股策略模板（xtquant 实盘）

参考实现：[kj202536/kj202536.py](kj202536/kj202536.py)

### 文件骨架
```python
# -*- coding: utf-8 -*-
import os, sys, time, datetime, argparse
from datetime import timezone, timedelta
from xtquant import xtdata, xtconstant
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.utilities import StrategyLedger, StateManager, MessagePusher
from utils.stockmgr import StockMgr
from utils.marketmgr import MarketMgr
from utils.trademgr import TradeMgr

BEIJING_TZ = timezone(timedelta(hours=8))
DEBUG = True  # 默认调试模式，实盘用 -m REAL 启动

class MyStrategy:
    TOTAL_BUDGET = 60_000  # 策略资金上限（元）

    def __init__(self, trader, account):
        self.trader  = trader
        self.account = account
        _base = os.path.dirname(os.path.abspath(__file__))
        self.state  = StateManager(os.path.join(_base, 'XX_state.json'), defaults={...})
        self.ledger = StrategyLedger(os.path.join(_base, 'XX_holdings.json'))
        self.pusher = MessagePusher()

    def handlebar(self):
        """每 N 秒轮询一次，按时间节点分发模块"""
        now = datetime.datetime.now(BEIJING_TZ)
        t, today, month = now.strftime('%H:%M:%S'), now.strftime('%Y%m%d'), now.month
        # 按时间窗口触发各模块...

    def _buy_stocks(self, target_list, max_hold):
        """等权买入：取可用现金与 TOTAL_BUDGET 的较小值"""
        ...  # 参考 kj202509/kj202509.py 的 buy_a_shares() 实现

    def _sell_stocks(self, codes):
        """卖出持仓，等待成交后再买入"""
        ...

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', default='DEBUG')
    args = parser.parse_args()
    DEBUG = (args.mode.upper() != 'REAL')

    qmt_path, account_id = r'D:\光大证券金阳光QMT实盘\userdata_mini', '47601131'
    session_id = int(time.time())
    trader = XtQuantTrader(qmt_path, session_id)
    acc = StockAccount(account_id)
    trader.register_callback(MyCallback())
    trader.start()
    if trader.connect() != 0:
        print('连接失败'); sys.exit(1)
    trader.subscribe(acc)

    strategy = MyStrategy(trader, acc)
    try:
        while True:
            strategy.handlebar()
            time.sleep(3)
    except KeyboardInterrupt:
        trader.stop()
```

### A 股策略开发规则

1. **先卖后买**：调仓时先卖出不在目标列表中的持仓，调用 `TradeMgr.wait_for_sells()` 等待成交，再买入新标的
2. **资金限额**：`budget = min(asset.cash, self.TOTAL_BUDGET)`，不允许超额使用
3. **持仓验证**：所有持仓操作前必须调用 `self.ledger.is_in_ledger(code)` 验证归属
4. **下单格式**：
   ```python
   seq = trader.order_stock(
       account, code, xtconstant.STOCK_BUY,  # 或 STOCK_SELL
       volume, xtconstant.LATEST_PRICE, 0,
       'strategy_tag', '备注'
   )
   if seq != -1:
       ledger.add(code)  # 买入成功才登记
   ```
5. **DEBUG 保护**：所有下单代码必须在 `if not DEBUG:` 块内，DEBUG 模式只打印日志
6. **最小手数**：A 股按 100 股为单位，`volume = int(cash / price / 100) * 100`，不足 100 股跳过
7. **股票代码格式**：A 股用 `600519.SH` / `000858.SZ`，不要用聚宽格式 `600519.XSHG`

### 常用数据 API

```python
# 获取历史日线（本地缓存，需先 download）
data = xtdata.get_market_data_ex(['close', 'high', 'low', 'volume'], codes, period='1d', count=N)
df = data['600519.SH']  # DataFrame，列为字段名

# 获取实时 tick
xtdata.subscribe_quote(code, period='tick', count=1)
tick = xtdata.get_full_tick([code])
price = tick[code]['lastPrice']
high_limit = tick[code]['highLimit']

# 获取财务数据
fin = xtdata.get_financial_data([code], table_list=['PershareIndex', 'Income', 'Balance'],
                                 start_time='20230101', report_type='announce_time')
roe = fin[code]['PershareIndex'].iloc[-1]['equity_roe']
eps = fin[code]['PershareIndex'].iloc[-1]['s_fa_eps_basic']
bps = fin[code]['PershareIndex'].iloc[-1]['s_fa_bps']    # 每股净资产（用于计算PB）
dps = fin[code]['PershareIndex'].iloc[-1]['s_fa_dps']    # 每股现金股息

# 获取全 A 股列表
all_stocks = xtdata.get_stock_list_in_sector('沪深A股')

# 获取股票信息（名称、涨停价、上市日期等）
detail = xtdata.get_instrument_detail(code)
name       = detail['InstrumentName']
up_stop    = detail['UpStopPrice']
down_stop  = detail['DownStopPrice']
open_date  = detail['OpenDate']      # 上市日期，格式 20100101
total_vol  = detail['TotalVolume']   # 总股本（用于计算市值）
```

### 常用过滤函数
```python
# 排除科创板 + 北交所
def filter_kcbj(codes):
    return [c for c in codes
            if not c.endswith('.BJ')
            and not c.split('.')[0].startswith('68')
            and c.split('.')[0][0] not in ('4', '8')]

# 排除 ST / 退市
def filter_st(codes):
    return [c for c in codes
            if (d := xtdata.get_instrument_detail(c))
            and 'ST' not in d.get('InstrumentName','')
            and '退' not in d.get('InstrumentName','')]

# 排除停牌（前一日成交量为0）
def filter_suspended(codes):
    data = xtdata.get_market_data_ex(['volume'], codes, period='1d', count=1)
    return [c for c in codes if c in data and not data[c]['volume'].empty
            and data[c]['volume'].iloc[-1] > 0]

# 排除次新股
def filter_new_stock(codes, min_days=365):
    today = datetime.date.today()
    return [c for c in codes
            if (d := xtdata.get_instrument_detail(c))
            and (today - datetime.datetime.strptime(str(d.get('OpenDate','20000101')), '%Y%m%d').date()).days >= min_days]
```

---

## 港股策略模板（akshare 建议输出）

参考实现：[kj202536/kj202536_hk.py](kj202536/kj202536_hk.py)

### 文件骨架
```python
# -*- coding: utf-8 -*-
import sys, datetime, argparse
import numpy as np, pandas as pd
from scipy import stats
import akshare as ak  # pip install akshare

class Config:
    policy_asset = 60_000  # HKD，可通过 --asset 覆盖
    lot_sizes    = {'02800.HK': 500, ...}  # 每手股数，下单前需确认

def _fetch_ohlc(symbol: str, count: int) -> pd.DataFrame:
    """akshare 港股日线，symbol 传裸代码（不含 .HK）"""
    bare = symbol.replace('.HK', '')
    df = ak.stock_hk_hist(symbol=bare, period='daily', adjust='qfq', ...)
    return df[['date', 'high', 'low', 'close']].tail(count)

def run(policy_asset: float):
    # 1. RSRS 择时 → z-score
    # 2. 计算各 ETF 动量分数
    # 3. 按分组选取最优标的
    # 4. 输出调仓建议（含手数、参考金额）

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--asset', type=float, default=Config.policy_asset)
    run(policy_asset=parser.parse_args().asset)
```

### 港股开发规则
- **不接入实盘**：仅做数据计算和打印，由用户人工在券商 App 操作
- **手数配置**：`lot_sizes` 需针对每只标的手动配置，HKEX 手数因股不同
- **数据格式**：akshare 返回中文列名，需 `rename`：`{'日期':'date','最高':'high','最低':'low','收盘':'close'}`
- **代码格式**：配置层用 `02800.HK`，传 akshare 时去掉 `.HK`

---

## 美股策略模板（yfinance 建议输出）

参考实现：[kj202536/kj202536_us.py](kj202536/kj202536_us.py)

### 文件骨架
```python
# -*- coding: utf-8 -*-
import sys, datetime, argparse
import numpy as np, pandas as pd
from scipy import stats
import yfinance as yf  # pip install yfinance

class Config:
    policy_asset = 60_000  # USD
    lot_sizes    = {c: 1 for c in all_symbols}  # 美股无手数限制

def _fetch_ohlc(symbol: str, count: int) -> pd.DataFrame:
    """yfinance Ticker.history()，避免 download() 的 MultiIndex 问题"""
    raw = yf.Ticker(symbol).history(start=..., end=..., auto_adjust=True)
    return raw.rename(columns={'High':'high','Low':'low','Close':'close'})[['high','low','close']].tail(count)

def run(policy_asset: float):
    # 1. RSRS 择时（基准 SPY）
    # 2. 计算各 ETF 动量分数
    # 3. 按分组选取最优标的（每组至多1只，防集中度风险）
    # 4. 输出调仓建议（含股数、参考金额）
```

### 美股开发规则
- **不接入实盘**：仅做数据计算和打印
- **分组去重**：按 `etf_groups` 分组，每组最多选 1 只，防止过度集中
- **缓存复用**：OHLC 数据用 `ohlc_cache` dict 缓存，避免同一 symbol 重复请求
- **代码格式**：美股使用 Yahoo 格式 `SPY`、`QQQ`，不加后缀

---

## RSRS 择时算法（三市通用）

```python
from scipy import stats
import numpy as np

def calc_rsrs(highs, lows, n=18, m=600) -> float:
    """
    返回标准化 Z-Score。
    > +0.5 → 进攻模式；< -0.5 → 防御模式；中间 → 震荡，维持现仓
    """
    slopes = []
    for i in range(len(highs) - n + 1):
        slope, *_ = stats.linregress(lows[i:i+n], highs[i:i+n])
        slopes.append(slope)
    current = slopes[-1]
    hist    = np.array(slopes[:-1])
    return float((current - hist.mean()) / (hist.std() + 1e-9))
```

A 股可直接调用 `MarketMgr.get_rsrs_signal(index_code)` 复用已有实现。

---

## 日志规范

所有策略必须有清晰的日志，方便运行时监控和事后复盘：

```python
import logging

def make_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s  %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger

LOG = make_logger('kj2025XX')
```

关键节点必须 log：
- 策略初始化完成（账号、预算、状态恢复）
- 每次选股结果（代码 + 关键指标）
- 每次调仓计划（卖出列表 + 买入列表）
- 每笔委托（代码、方向、数量、价格、预估金额）
- 止损/空仓触发（触发条件 + 相关持仓）
- 异常捕获（完整异常信息，用 `LOG.exception()`）

---

## DEBUG 模式规范

所有策略必须支持 `DEBUG` 模式，通过命令行参数控制：

```bash
python kj2025XX.py           # 默认 DEBUG，不发真实报单
python kj2025XX.py -m REAL   # 实盘模式，谨慎！
```

**DEBUG 模式要求**：
- 所有选股、计算、过滤逻辑**照常执行**
- 所有日志**照常输出**（让用户看到策略的决策过程）
- **唯一区别**：下单 API 调用放在 `if not DEBUG:` 块内
- `ledger.add()` / `ledger.remove()` 也应在 `if not DEBUG or seq != -1:` 条件下执行

```python
# 标准 DEBUG 保护写法
seq = -1
if not DEBUG:
    seq = trader.order_stock(account, code, xtconstant.STOCK_BUY, ...)
if DEBUG or seq != -1:
    ledger.add(code)
    LOG.info(f"[{'调试' if DEBUG else '委托'}] 买入 {code} {volume}股")
```

---

## 策略命名规范

| 编号 | 文件 | 说明 |
|------|------|------|
| kj202509 | kj202509/kj202509.py | 全天候A股（参考） |
| kj202536 | kj202536/kj202536.py | 多资产轮动（参考） |
| kj2025XX | kj2025XX/kj2025XX.py | 新策略按此命名 |

- 多子策略的策略目录下可有多个文件：`kj2025XX_etf.py`、`kj2025XX_pb.py` 等
- 各子策略文件必须**可独立运行**（有自己的 `__main__` 块和 QMT 连接逻辑）
- 共享基类/工具放在 `kj2025XX_base.py`，不含 `__main__`

---

## 策略 README 规范

### 每个策略目录必须包含 README.md

新建策略或修改策略时，必须在策略目录下维护一个 `README.md`。README 是策略的"使用说明书"，供人（而非 Claude）阅读——写清楚"这个策略是什么、适合什么时候用、怎么跑"。

### README 固定结构

````markdown
# {策略名称}（{编号}）

> 一句话定位：用最简洁的语言说清楚这个策略在做什么

## 策略总述

详细描述策略的完整逻辑：
- 选股宇宙：从哪个范围里选（全A股 / 沪深300成分 / 指定ETF池 / 等）
- 核心信号：什么因子或信号驱动买卖（动量/价值/股息/ML因子/技术指标/等）
- 完整选股流程：Step 1 … Step 2 … 如何打分/排序/过滤
- 调仓执行：如何买入（等权/市值加权）、如何决定卖出
- 风控机制：止损、空仓月、熔断、涨停打开等保护逻辑

## 策略优势、劣势与适用市场环境

### 优势
- 列出 2-4 条真实的优点

### 劣势 / 风险
- 列出 2-4 条真实的缺陷或潜在风险
- 说明在什么情况下策略会显著跑输

### 适合的市场环境
- 最佳环境（如：趋势明显的单边牛市 / 低利率高分红环境 / 等）
- 不适合的环境（如：高波动猴市 / 估值极度扭曲时 / 等）

## 主要参数

| 参数名 | 默认值 | 说明 | 调整建议 |
|--------|--------|------|----------|
| `TOTAL_BUDGET` | 60,000 元 | 策略资金上限 | 根据账户规模调整 |
| `MAX_HOLD` | N | 最大同时持仓数 | 越大分散越高，单股收益越低 |
| ... | ... | ... | ... |

## 推荐调仓周期与运行时间

| 操作 | 触发条件 | 时间 | 说明 |
|------|----------|------|------|
| 选股 + 调仓 | 每月第1个交易日 | 09:35 | 月度主动调仓 |
| 止损巡检 | 每日 | 14:45 | 个股跌幅达阈值则清仓 |
| 涨停打开 | 每日 | 14:00 / 14:50 | 昨日涨停今日打开则卖出 |

## 运行方式

```bash
# 调试模式（完整跑逻辑，不发真实报单）
python kj2025XX.py

# 实盘模式（谨慎！确认逻辑正确后使用）
python kj2025XX.py -m REAL
```

## 版本记录

| 日期 | 变更内容 |
|------|----------|
| YYYY-MM-DD | 初始版本 |
````

### README 维护触发规则

以下情况下**必须同步更新** README，Claude 在修改代码后应主动更新：

| 触发事件 | 需更新的 README 章节 |
|----------|---------------------|
| 修改选股因子、过滤条件、排序逻辑 | 策略总述、策略优势/劣势 |
| 修改参数默认值（`MAX_HOLD`、`TOTAL_BUDGET` 等） | 主要参数 |
| 修改调仓频率、触发时间 | 推荐调仓周期与运行时间 |
| 新增/删除止损、空仓月、熔断等风控模块 | 策略总述、策略优势/劣势 |
| 新增子策略文件（如 `_pb.py`、`_etf.py`） | 策略总述（注明子策略分工）、运行方式 |

**版本记录行**：每次重大逻辑变更时，在"版本记录"表格追加一行，日期用实际修改日期，内容一句话概括变更。

---

## 注意事项

1. **不要修改其他策略的 ledger/state 文件**，每个策略只读写自己的
2. **财务数据批量查询**：`xtdata.get_financial_data()` 支持列表传入，按 200 只分批调用避免超时
3. **下载历史数据**：`xtdata.get_market_data_ex()` 只读本地缓存，查询前必须先调用 `StockMgr.download_history()`
4. **港股 akshare 限流**：每次请求间隔 1-2 秒，遇到失败按指数退避重试（参考 kj202536_hk.py 的 `_fetch_ohlc`）
5. **止损静默期**：止损后至少静默 20 个交易日（约 28 日历天）再重新建仓，避免频繁止损
6. **空仓月机制**：如策略有空仓月，在月初 09:31 检查并清仓，在调仓逻辑中跳过该月
