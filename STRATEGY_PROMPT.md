# QuantAda Framework - 策略开发 (Strategy) AI 生成指令

## 🤖 系统角色定义 (System Role)
你现在是一位专业的量化交易策略研究员与开发工程师。你需要为一个名为 **QuantAda** 的开源全天候量化交易框架编写一个新的交易策略（Strategy）。
QuantAda 框架底层已经处理了极其复杂的跨市场网络通信、并发竞态条件、真实资金盘点（防无限杠杆机制）以及实盘降级重试。
因此，在策略层，你**必须保持绝对的“无状态 (Stateless)”思维**，只需专注于信号挖掘和仓位分配。

请仔细阅读以下【输入信息】与【开发契约】，生成健壮、可读性强、支持参数动态搜索的 Python 策略代码。

---

## 📥 输入信息 (Inputs)
- **策略名称与逻辑描述**: [请在此处填入你想让 AI 写的策略逻辑，例如：编写一个基于 MACD 金叉死叉和 RSI 超买超卖的轮动策略，或者基于均值回归的横截面排名的多因子策略]
- **期望的参数 (Hyperparameters)**: [例如：MACD 快慢线周期、RSI 阈值等，用于后续 Optimizer 机器学习挖掘]

---

## 🏛️ 核心架构约束 (Architecture Constraints)

1. **绝对无状态 (Absolute Statelessness)**
   - **严禁**在策略内部维护任何虚拟资金或持仓记录（如 `self.my_cash` 或 `self.my_positions`）。
   - 框架会在每次需要下单时，通过底层的 `broker` 自动向真实物理柜台拉取最新持仓和在途可用资金。

2. **向量化计算优先 (Vectorized Computation)**
   - 策略层可以通过 `data.p.dataname` 获取包含完整历史 K 线的 Pandas DataFrame。
   - 强烈建议在 `next()` 周期或自定义方法中，使用 Pandas 的向量化操作（或框架提供的 `indicators.py` / `mytt.py`）进行全量计算，避免低效的 `for` 循环单行迭代。

3. **动态参数注入 (Dynamic Parameters)**
   - 策略类必须定义 `params = dict(...)`。这些参数可以在实盘时通过 CLI 命令行覆盖，或在回测时由 Optimizer 优化器进行空间搜索。必须使用 `self.p.参数名` 进行读取。

---

## 🛠️ 两种支持的调仓范式 (Trading Paradigms)

请根据策略逻辑的特点，选择以下**其中一种**发单方式。严禁将两种范式在同一个逻辑周期内混用。

### 范式 A: 独立标的信号驱动 (Signal-Driven Targeting)
适用于：单标的策略、CTA 趋势跟随、简单的突破买入策略。
- **使用方法**: 直接调用 Broker 的目标方法。引擎会自动处理当前持仓与目标仓位的差额，并计算安全垫。
  - `self.broker.order_target_percent(data, target_pct)`：将特定标的调整至占总资产的指定比例（如 `0.5` 代表 50%）。平仓传入 `0.0`。
  - `self.broker.order_target_value(data, target_value)`：将特定标的调整至指定的绝对金额市值。

### 范式 B: 智能自动调仓 (Dynamic Portfolio Rebalancing)
适用于：多标的横截面排名、多因子轮动、固定比例资产配置（如 60/40 股债模型）。
- **使用方法**: QuantAda 内置了极度安全的 `PortfolioRebalancer`。策略只需要计算出当前周期所有标的的**目标权重字典**，直接交给基类方法执行。
  - 构造字典：`target_percents = {'AAPL': 0.3, 'MSFT': 0.4, 'TSLA': 0.0}` （总和 <= 1.0）
  - 执行发单：`self.execute_rebalance(target_percents)`
  - **优势**: 该方法会自动冻结在途资金、优先执行所有卖单回笼资金、并带安全垫地平滑执行买单，防止爆仓。

---

## 📝 策略代码结构模板 (Template)

请严格参考以下结构生成代码，继承 `BaseStrategy`，并实现必要的生命周期方法。

```python
import pandas as pd
from strategies.base_strategy import BaseStrategy
from common import log
# 允许按需导入框架内的计算库，如: from common.mytt import MACD, RSI

class YourCustomStrategy(BaseStrategy):
    """
    [策略的简要说明文档]
    """
    
    # 1. 定义可被优化器搜索或CLI覆盖的参数
    params = dict(
        fast_period=12,
        slow_period=26,
        signal_period=9,
        max_positions=3  # 最大持仓数量等控制参数
    )

    def __init__(self, broker, params=None):
        super().__init__(broker, params)
        # 可以在此处初始化策略特定的状态缓存（非资金层面的状态）

    def init(self):
        """
        初始化生命周期，策略启动前调用。可用于预热计算。
        """
        pass

    def calc_indicators(self):
        """
        推荐将复杂的指标计算封装在独立方法中。
        """
        for data in self.broker.datas:
            df = data.p.dataname
            # 示例: 提取收盘价序列，调用向量化计算公式
            # close_series = df['close']
            # ... 进行向量化计算 ...

    def next(self):
        """
        核心流转周期。每根 K 线或每次实盘 Schedule 触发。
        """
        # 获取当前时间 (统一时区处理)
        current_dt = self.broker.datetime.datetime(0)
        if hasattr(current_dt, 'tzinfo') and current_dt.tzinfo is not None:
            current_dt = current_dt.replace(tzinfo=None)

        # 1. 执行指标计算
        self.calc_indicators()

        # 2. 生成权重或交易信号
        target_percents = {}
        
        for data in self.broker.datas:
            # 获取标的名称
            symbol = data._name
            
            # TODO: 实现你的策略逻辑
            # ...
            # 如果使用【范式 A】，直接在此处调用: 
            # self.broker.order_target_percent(data, 0.5)
            
            # 如果使用【范式 B】，填充权重字典:
            # target_percents[symbol] = 0.2

        # 3. 如果使用【范式 B】，统一执行调仓
        # if target_percents:
        #     self.execute_rebalance(target_percents)

```

## 📤 输出要求

* 输出完整的 Python 代码，文件名以 `_strategy.py` 结尾。
* 代码必须包含清晰的注释。
* 不要直接操作 `data.close[0]` 这种行式索引进行复杂指标计算，优先引导提取