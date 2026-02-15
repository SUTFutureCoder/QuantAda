# QuantAda: A Dual-Core Quantitative Trading Framework

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Paper Status](https://img.shields.io/badge/Paper-Under%20Review-yellow.svg)](YOUR_PAPER_LINK_OR_ARXIV)
[![Starry Intelligence](https://img.shields.io/badge/R%26D-Starry%20Intelligence-purple.svg)](https://www.starryint.hk)

> **Note**: This is the official reference implementation for the paper *"Bridging the Sim-to-Real Gap in Algorithmic Trading: An Entropy-Based Adaptive Optimizer with Survival-First Composite Metrics"*.

**Developed by Starry Intelligence Technology Limited (Hong Kong).**

---

一个优雅、可扩展、可实盘的量化交易框架，实现算法的分模块独立或协作开发。

`Ada` 是 `Adapter`（适配器）的缩写，也借此向计算机先驱 **阿达·洛夫莱斯 (Ada Lovelace)** 及以她命名的 Ada 语言致敬。

本项目旨在对抗市面上普遍存在的“过拟合”与“造神”风气，通过严谨的工程架构与数学逻辑，让量化交易回归敬畏市场、技术为本的初心。

## 核心特性

- **全场景运行模式 (Flexible Execution Modes)**
  - **全球市场与多币种支持**：已打通 IBKR（盈透证券）、GM（万和掘金）实盘券商，支持 A股（100股一手）与美股/外汇（1股取整）的无缝切换，内置跨币种（如 HKD/USD）动态保证金与购买力自动折算。
  - **工业级失效安全**：独创“不死鸟”事件引擎。具备僵尸 ID 自动顺延重连、资金不足自动降级发单（Downgrade）、底层订单取整截断保护，以及防双花（Double Spend）宽限期锁，极致防爆仓。
  - **分布式远程调用**：支持通过 `Launcher` 主动发起远程连接，实现**计算（Linux）与交易（Windows）分离**的实盘部署。
  - **保护自定义标的**：支持通过 `ignored_symbols` 参数配置，保护标的不参与策略计算。保护SGOV、BIL、USFR、SHY等压舱石；尊重客户主观选择实现虚拟多租户分仓；跨越法币-券商通道摩擦，券商内部转换标的，避免银行无法打款。


- **策略与引擎解耦 (Adapter Pattern)**
  - **一次编写，多处运行**：基于适配器模式，纯粹的策略逻辑可无缝运行于 `Backtester` 本地回测或实盘环境。
  - **自动持久化**：支持交易流水与资金快照自动写入 MySQL/SQLite 或投递至消息队列，实现数据沉淀。
  - **本地框架回测**：脱离外部依赖，直接使用本地数据 (`CSV`/`DB`) 进行离线高效策略验证。


- **模块化与工程分离 (SDK Mode)**
  - **数据层**：内置多源数据管理 (Tushare/AkShare)，支持责任链更新与自动缓存。
  - **工程层**：支持通过 `PYTHONPATH` 引用外部库，实现业务策略代码与框架核心的物理隔离。


- **全天候监控 (Monitoring)**
  - **即时推送**：开箱即用的钉钉 (DingTalk) 与企业微信 (WeCom) 集成。
  - **全维感知**：实时推送系统启停、成交详情及异常堆栈，随时掌握策略“心跳”。


- **启发式并行优化器 (Bayesian Optimizer)**
  - **极速并行**：基于 TPE 算法与 Constant-Liar 策略，充分利用多核 CPU 进行并行参数搜索。
  - **智能评估**：根据参数空间熵值动态推导最佳尝试次数，拒绝盲目穷举。


- **科学评价体系 (Mix Score)**
  - **混合评分**：独创 **Mix Score** 指标，综合考量生存能力 (Calmar)、平滑度 (Sharpe) 与进攻性 (Return)。
  - **抗过拟合**：强制推荐 **“样本内训练 + 样本外验证”** 模式，验证策略鲁棒性。


- **交互式看板 (Zero-Config Dashboard)**
  - **可视化调参**：优化任务启动时自动唤起 Web 看板，实时展示 Pareto 前沿面与参数重要性分析。
  - **远程支持**：兼容无头服务器 (Headless) 环境，支持 SSH 隧道远程监控。


- **可插拔风控 (Pluggable Risk Control)**
  - **链式防御**：支持命令行动态挂载多个风控组件（如 `--risk stop_loss,trend_protection`）。
  - **独立配置**：风控规则与策略逻辑解耦，可针对不同账户灵活组合。

- **智能仓位管理 (Smart Rebalancer)**
  - 内置 `PortfolioRebalancer` 组件，专注解决 TopK 轮动策略中的资金分配难题。
  - 自动处理 **"先卖后买"** (Sell-then-Buy) 的资金释放逻辑，最大化资金利用率，彻底消除 Cash Drag。
  - 支持 **"让利润奔跑"** (Let Winners Run) 模式，避免因强制再平衡导致的早期止盈。

![diagram](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/diagram.png?raw=true)

## 快速开始

#### 1\. 环境准备

```bash
# 克隆项目
git clone https://github.com/SUTFutureCoder/QuantAda.git
cd QuantAda

# 推荐创建并激活虚拟环境
python -m venv .venv
source .venv/bin/activate  # on Windows, use `.venv\Scripts\activate`

# 安装依赖
pip install -r requirements.txt
```

#### 2\. 配置

打开 `config.py` 文件，并填入您的 `TUSHARE_TOKEN`。如果您没有，可以前往 [Tushare Pro](https://tushare.pro/user/token)
免费注册获取。

```python
# config.py
TUSHARE_TOKEN = 'your_token_here'
```

数据库记录 (可选): 开启后，回测结果将自动存入数据库。

```python
# config.py
DB_ENABLED = True
# 格式: dialect+driver://username:password@host:port/database
# 示例 (MySQL): 'mysql+pymysql://root:123456@localhost:3306/quantada_db'
# 示例 (SQLite): 'sqlite:///quantada_logs.db'
DB_URL = 'mysql+pymysql://root:yourpassword@localhost:3306/quant'
```

#### 3.a. 运行回测 (内部模式)

此模式适用于直接在框架的 `strategies/`、`stock_selectors/` 等目录中编写逻辑。

使用 `run.py` 脚本执行回测。您可以通过命令行参数灵活地选择策略、标的、资金和手续费。

```bash
#### 3.a. 运行回测 (内部模式)

此模式适用于直接在框架的 `strategies/`、`stock_selectors/` 等目录中编写逻辑。

使用 `run.py` 脚本执行回测。引擎已内置严密的资金盘点与交易截断保护，您只需通过命令行参数组合，即可解锁各种高级交易形态：

```bash
# 1. 极简开局：运行经典的 MACD 单标的趋势策略
python run.py sample_macd_cross_strategy

# 2. 旗舰演示：全自动调仓与底仓物理隔离机制
# (框架会自动保护 SHSE.511880 理财底仓不被卖出，仅用剩余活水资金轮动前3只宽基 ETF)
python run.py sample_auto_rebalance_strategy --symbols=SHSE.510300,SHSE.510500,SZSE.159915,SHSE.511880 --start_date=20230101

# 3. 变量控制：指定标的、设置 50 万初始资金，并限定回测时间以加快运行
python run.py sample_macd_cross_strategy --symbols=SHSE.600519 --cash=500000 --start_date=20230101

# 4. 接入动态选股器：抛弃固定列表，让选股器每天自动从全市场拉取符合条件的票池
python run.py sample_auto_rebalance_strategy --selection=sample_manual_selector --start_date=20240101

# 5. 挂载独立风控：为策略外挂“止盈止损”或“大盘趋势保护”模块 (策略与风控彻底解耦)
python run.py sample_macd_cross_strategy --symbols=SHSE.600519 --risk=sample_stop_loss_take_profit,sample_trend_protection

# 6. 极客调参：通过命令行直接覆写策略与风控的内部运行参数 (Params)
python run.py sample_auto_rebalance_strategy --symbols=SZSE.159915 --params "{'selectTopK': 2, 'roc_period': 10}" --risk_params "{'stop_loss_pct': 0.05}"

# 7. 动态环境部署：通过命令行直接覆写 config.py 中的底层系统配置 (极度适合 CI/CD 与自动化跑批脚本)
python run.py sample_macd_cross_strategy --config "{'LOG': False, 'GM_TOKEN': 'your_token|127.0.0.1:7001'}"

# 查看所有可用参数与帮助说明
python run.py --help
```

`--symbols`和`--risk`可传入多个逗号分隔的标的及风控策略。

#### 3.b. 运行回测 (SDK/插件化模式)

此模式支持将策略、选股器等逻辑放在框架目录之外的独立项目中，实现“依赖倒置”。


1.  **目录结构示例**：

    ```
    /path/to/QuantAda/ (框架目录 A)
    └── run.py

    /path/to/MyProject/ (您的代码库 B)
    └── my_strategies/
        ├── __init__.py
        └── my_cool_strategy.py
            # 假设文件内容如下:
            # from strategies.base_strategy import BaseStrategy
            # class MyCoolStrategy(BaseStrategy):
            #     ...
    ```

2.  **设置 PYTHONPATH**：将框架目录(A)和您的项目目录(B)都添加到 `PYTHONPATH` 环境变量中。

    ```bash
    # (Linux/macOS)
    export PYTHONPATH=/path/to/QuantAda:/path/to/MyProject

    # (Windows CMD)
    set PYTHONPATH=C:\path\to\QuantAda;C:\path\to\MyProject
    ```

3.  **运行外部策略**：在 `run.py` 中使用**带点号的全限定名**来指定您的策略。

    ```bash
    # 切换到框架目录
    cd /path/to/QuantAda

    # 方式1: 提供模块和类的全名 (推荐)
    python ./run.py my_strategies.my_cool_strategy.MyCoolStrategy

    # 方式2: 提供模块名 (my_strategies.my_cool_strategy)，自动推断类名 (MyCoolStrategy)
    python ./run.py my_strategies.my_cool_strategy
    
    # 样例：运行策略和选择标的
    python ./run.py my_strategies.reverse_trader_multiple_actions_strategy --selection=my_selectors.reverse_trader_multiple_actions_selector
    ```

    这种方式同样适用于 `--selection` 和 `--risk` 参数。

4.  **注意事项**

- 建议将解释器指向本框架，并在框架的requirements.txt管理依赖，并在本框架环境中执行策略

- 文件夹/包命名**请勿**和框架相同，建议添加```my_```前缀或```_custom```后缀。

- 如果有自定义指标算法，请新建自定义py脚本，并通过```from common.indicators import *```引入框架的指标算法库。

#### 3.c. 参数优化 (进阶)

告别“看图说话”的手动调参。统一使用 run.py 脚本，只需传入 --opt_params 参数，即可激活优化模式。定义参数搜索空间，利用 AI 算法自动寻找最优解。

为了防止过拟合，框架强制推荐使用**训练集/测试集分离**模式，并默认开启 **“地狱模式”** ——即默认在 2018 年熊市训练生存能力（Calmar），在 2019-2020 年牛市验证盈利能力。

```bash
# 1. 默认模式 (推荐)：
# 不指定时间参数时，自动使用默认的“抗过拟合”周期 (2018训练/2019-2020测试) 和 Calmar 目标
# 只要传入 --opt_params 即自动切换为优化模式
python ./run.py sample_momentum_strategy --symbols SHSE.510300 --opt_params "{'momentum_period': {'type': 'int', 'low': 10, 'high': 60, 'step': 1}}" --n_trials 50
```

```bash
# 2. 自定义周期：
# 如果您需要针对特定时间段（例如近期行情）进行优化，请显式覆盖时间参数
# 示例：在 2021-2022 年训练，2023 年测试
python ./run.py sample_momentum_strategy --selection sample_manual_selector --opt_params "{'momentum_period': {'type': 'int', 'low': 10, 'high': 60}}" --train_period 20210101-20221231 --test_period 20230101-20231231 --n_trials 50
```

```bash
# 3. 自动步数
# 不传入 --n_trials: 算法根据参数空间复杂度自动推算尝试次数
# --metric mix_score 启用混合评分目标（综合 Calmar、Sharpe 和 收益率）
python ./run.py sample_momentum_strategy --selection sample_manual_selector --opt_params "{'momentum_period': {'type': 'int', 'low': 10, 'high': 60}}" --metric mix_score
```

运行结束后，浏览器将自动弹出交互式的**参数优化历史**、**参数切片 (寻找参数平原)** 等图表，助您一眼看穿策略的稳定性。

#### 4\. 部署实盘 (以掘金量化为例)

框架通过 `live_trader` 模块实现与外部平台的松耦合对接，策略代码无需修改即可复用。

1.  **配置PYTHONPATH**：在操作系统的 `高级系统设置→环境变量` 中，添加本框架的项目根目录到 `PYTHONPATH` 中。 （如果使用SDK模式，还需添加您自己的项目目录）。

2.  **创建策略入口**：在掘金新建策略，参考 `live_trader/samples/gm_main_sample.py` 的代码，将 `if __name__ == '__main__'` 上方代码复制到掘金的 `main.py` 文件中。

3.  **配置策略**：修改 `main.py` 中的 `config` 字典，使其与您的回测命令行参数对应。`config` 是连接框架与实盘的唯一“接头”。

    ```python
    # 示例: 对应回测命令 `python run.py MyStrategy --selection=MySelector --cash=500k`
    # 如果使用SDK模式，strategy_name 和 selection_name 也应使用全限定名
    config = {
        'platform': 'gm',
        'strategy_name': 'MyStrategy', # 或 'my_strategies.my_cool_strategy.MyCoolStrategy'
        'selection_name': 'MySelector', # 或 'my_selectors.my_selector_file.MySelector'
        # 'cash': 500000.0,  # 选填，用于虚拟分仓，不填则使用账户全部资金
        'params': { ... } # 策略自定义参数
    }
    ```

4.  **运行**：保存 `main.py` 并启动掘金策略。

### 主动运行券商实盘
支持 **Linux (策略端)** + **Windows (柜台端)** 分布式部署方案，实现计算与交易环境物理隔离。

1. **配置连接**：修改 `config.py` 中的 `BROKER_ENVIRONMENTS`，配置目标机器 IP、Token 及策略 ID。
2. **启动命令**：通过 `--connect` 参数指定 Broker 和环境。
   ```bash
   # 连接 config.py 中定义的 gm_broker:real 环境
   python run.py strategies.sample_macd_cross_strategy --connect=gm_broker:real

   # 连接仿真环境
   python run.py strategies.sample_macd_cross_strategy --connect=gm_broker:sim

## 框架目录说明

```
QuantAda/
├── alarms/                 # 报警监控模块
│   ├── base_alarm.py       # 报警基类
│   ├── dingtalk_alarm.py   # 钉钉适配
│   ├── wecom_alarm.py      # 企业微信适配
│   └── manager.py          # 报警管理器
├── backtest/               # 回测模块
│   └── backtester.py       # 回测执行器
├── common/                 # 通用逻辑模块
│   ├── indicators.py       # 指标算法聚合库，自定义使用Ta-Lib及MyTT
│   ├── mytt.py             # MyTT指标计算库
│   ├── optimizer.py        # 参数优化核心逻辑 (Optuna)
│   └── rebalancer.py       # 智能仓位管理、资金分配器
├── data/                   # 行情数据缓存目录
├── data_providers/         # 主数据源模块
│   ├── akshare_provider.py # AkShare数据源适配器
│   ├── base_provider.py    # 数据源抽象基类
│   ├── csv_provider.py     # CSV数据源适配器
│   ├── gm_provider.py      # 万和掘金券商数据源适配器
│   ├── ibkr_provider.py    # IBKR券商数据源适配器
│   ├── manager.py          # 数据源调度与缓存管理器
│   ├── sxsctushare_provider.py       # 山西证券TuShare数据源适配器
│   ├── tushare_provider.py # TuShare数据源适配器
│   └── yf_provider.py      # yfinance数据源适配器
├── data_extra_providers/   # 额外数据源模块
│   ├── http_extra_provider.py         # HTTP额外数据获取类
│   └── mysql_extra_provider.py        # MySQL额外数据获取类
├── ib_docker/              # ib-gateway服务器部署方案
│   ├── .env                # ibkr账号密码及vnc密码
│   └── docker-compose.yml  # docker-compose up -d一键部署ib-gateway
├── live_trader/            # 实盘交易模块
│   ├── adapters/           # 平台适配器层 (将外部API统一)
│   │   ├── base_broker.py  # Broker 抽象基类
│   │   ├── gm_broker.py    # 掘金(gm)券商平台具体实现
│   │   └── ib_broker.py    # IBKR券商平台具体实现
│   ├── samples/            # 各平台实盘入口文件样例
│   │   └── gm_main_sample.py
│   └── engine.py           # 实盘交易引擎 (驱动策略运行)
├── stock_selectors/        # 自定义选标的包
│   ├── base_selector.py    # 选标的抽象基类
│   └── sample_manual_selector.py      # 手动选择三支标的样例类
├── strategies/             # 策略模块
│   ├── base_strategy.py    # 策略抽象基类
│   ├── sample_custom_indicator_strategy.py # 使用自定义指标计算库的MACD样例策略
│   ├── sample_macd_cross_strategy.py  # MACD样例策略
│   ├── sample_extra_data_strategy.py  # 使用额外数据样例策略
│   └── sample_multi_portfolio_strategy.py  # 多标的等权样例策略
├── recorders/              # 回测记录模块
│   ├── __init__.py
│   ├── base_recorder.py    # 定义接口
│   ├── manager.py          # 统一入口（分发器）
│   ├── db_recorder.py      # 数据库记录实现
│   └── http_recorder.py    # HTTP记录实现示例
├── requirements.txt        # Python依赖包
├── config.py               # 配置文件 (API密钥等)
└── run.py                  # 命令行回测启动器
    
```

## 样例截图

### 终端执行回测
![backtest_mode_in_terminal](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/backtest_mode_in_terminal.png?raw=true)

### 券商平台执行回测
![backtest_mode_in_broker](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/backtest_mode_in_broker.png?raw=true)

### 券商平台执行实盘
![live_mode_in_broker](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/live_mode_in_broker.png?raw=true)

![live_mode_in_broker_ibkr](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/live_mode_in_broker_ibkr.png?raw=true)

### 框架和自定义策略工程分离
![public_private_split](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/public_private_split.png?raw=true)  

### 实时监控并推送实盘操作
![push_live_alarms](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/push_live_alarms.png?raw=true)  

### 基于Optuna优化策略参数
![optimizer](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/optimizer.png?raw=true)  

### 实时Optuna优化进度看板
![optuna-dashboard](https://github.com/SUTFutureCoder/QuantAda/blob/main/.sample_pictures/optuna-dashboard.png?raw=true)  


## 🚀 AI-Native: 基于契约的“零代码”全球券商接入 (Prompt-Driven Adapter Generation)

在传统的量化交易开发中，接入一个新的券商 API 往往是最枯燥且最容易踩坑的“脏活”——你需要应对各种奇葩的底层字段、脆弱的异步回调以及混乱的状态机。

**QuantAda 采用了一种面向 AI 时代的全新扩展范式：我们不手写 Adapter，我们只定义绝对严苛的架构契约，剩下的脏活全部外包给大语言模型（LLM）。**

基于 QuantAda 独特的**无状态自底向上对账机制（Stateless Bottom-Up Reconciliation）**与极度纯粹的底层抽象，我们为开源社区提供了一份“架构师级别的 AI 指令法典” —— `PROMPT_TEMPLATE.md`。

### 🛠️ 如何让 AI 为你写出极其健壮的适配器？

只需喝杯咖啡的时间，你就可以将 QuantAda 接入全球任何一家提供 Python SDK 的券商（如嘉信理财、长桥、盈立等）：

1. **获取法典**：打开项目根目录下的 `PROMPT_TEMPLATE.md`。
2. **喂给算力**：将这份 Prompt 与目标券商的官方 API 文档（或 Github 上的 SDK 示例）打包，发送给 Claude 3.5 Sonnet 或 GPT-4o。
3. **开箱即用**：AI 会严格遵循 QuantAda 的接口签名，吐出一份名为 `[broker]_broker.py` 的代码。将其直接丢进 `live_trader/adapters/` 目录下，并在 `config.py` 中配置，即可瞬间完成实盘点火！

### 🛡️ 为什么 AI 生成的代码在 QuantAda 中绝对安全？

大模型写代码容易产生“幻觉”并把状态写脏，但在 QuantAda 的 `PROMPT_TEMPLATE.md` 的强约束下，AI 必须遵守以下架构红线，这些红线甚至比普通人类开发者更严谨：

* **绝对无状态 (Absolute Statelessness)**：Prompt 强制禁止 AI 在适配器中维护任何本地现金或持仓变量。所有状态必须实时向物理柜台发起反向查询，从根本上杜绝内存账本污染。
* **严格实体消歧义**：强约束 AI 在处理订单回调时，必须采用基础代码的精准匹配（如 `AAPL` == `AAPL`），严禁使用隐式的模糊包含（如 `'C' in 'CSCO'`），彻底封死错单炸弹。
* **异常兜底协议**：强制要求 AI 处理盘口数据断流与 `ZeroDivisionError`，确保引擎的事件循环坚不可摧。

## 免责声明

**使用本框架进行任何真实交易操作前，请务必仔细阅读、理解并同意以下所有条款。**

1. **无任何保证**：本软件按“原样”提供，不作任何形式的保证，无论是明示的还是默示的。作者及贡献者不对软件的完整性、准确性、可靠性、适用性或可用性作任何承诺。

2. **投资风险自负**
   ：金融市场交易存在巨大风险，自动化交易程序可能放大这些风险。使用本框架进行交易所产生的一切财务亏损，包括但不限于因策略错误、代码BUG、数据延迟或错误、网络中断、API接口变更等问题导致的损失，均由使用者本人
   **独立承担全部责任**。

3. **非投资建议**：本框架及其包含的所有示例策略、代码和文档，仅用于技术学习、研究和交流目的，**不构成任何形式的投资建议**
   。作者及贡献者并非投资顾问。任何基于本框架的交易决策，均为您个人行为。

4. **责任限制**：在任何情况下，本项目的作者及贡献者均不对因使用或无法使用本软件而导致的任何直接、间接、附带、特殊、惩罚性或后果性损害承担任何责任。

5. **务必充分测试**：**严禁**在未经过长期、充分的回测和模拟盘测试的情况下，直接将任何策略用于实盘交易。您有责任确保您的策略逻辑在各种市场情况下的稳健性。

**股市有风险，入市需谨慎。一旦您下载、使用或修改本框架，即代表您已完全理解并接受本免责声明的全部内容。**

## 关于作者

- **个人博客**: [project256.com](https://project256.com)
- **GitHub**: [SUTFutureCoder](https://github.com/SUTFutureCoder)

## 引用 (Citation)

如果您在研究中使用了 QuantAda，请引用我们的相关论文：

> **Bridging the Sim-to-Real Gap in Algorithmic Trading: An Entropy-Based Adaptive Optimizer with Survival-First Composite Metrics**
> *Xingchen Lin (Starry Intelligence Technology Limited)*
> *Submitted to IEEE Access, 2026.*

```bibtex
@article{Lin2026QuantAda,
  title={Bridging the Sim-to-Real Gap in Algorithmic Trading: An Entropy-Based Adaptive Optimizer with Survival-First Composite Metrics},
  author={Lin, Xingchen},
  journal={IEEE Access (Under Review)},
  year={2026},
  publisher={IEEE}
}
```

## 许可证 (License)

MIT
