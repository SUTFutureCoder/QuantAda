# QuantAda

[](https://opensource.org/licenses/MIT)
[](https://www.python.org/downloads/release/python-380/)

一个优雅、可扩展、可实盘的量化交易框架，实现算法的分模块独立或协作开发。

`Ada` 是 `Adapter`（适配器）的缩写，也借此向计算机先驱**阿达·洛夫莱斯 (Ada Lovelace)** 及以她命名的Ada语言致敬。

作者看不惯市面上一众机构或开发者，通过过拟合、高年化忽悠小白割韭菜爆金币的风气，让量化交易回归敬畏、诚信、以技术为本的初心。

## 核心特性

  - **策略与引擎解耦**：得益于适配器模式，您只需编写一次纯粹的策略逻辑，即可无缝运行在 `backtrader` 回测引擎和掘金量化等实盘环境中，并支持全球各大券商独立配置特殊规则。
  - **模块化与可扩展**：
      - **数据层**：支持指定主数据源和额外数据源。默认采用责任链模式，按 `PRIORITY` 优先级获取数据，并实现自动增量更新与本地缓存，保证数据获取的稳定与高效。
      - **策略层**：支持将指标计算（`common/indicators`）抽象为公共模块，集成MyTT、Ta-Lib的同时，方便团队成员共享和组合，避免重复造轮子。
      - **引擎层**：通过适配器模式清晰地隔离了回测与实盘，您可以轻松添加对QMT、VN.PY等其他平台的适配器，而无需改动任何策略代码。
  - **插件化开发 (SDK模式)**：支持将框架作为SDK依赖。您可以在自己的代码库中编写策略，并通过`PYTHONPATH`引用，实现业务代码与框架代码的物理隔离，便于版本管理和独立开发。
  - **轻量级与专注**：框架只提供核心的骨架，没有集成任何臃肿或非必要的功能。每一行代码都为专业开发者服务，确保最大的灵活性和透明度。
  - **科学的参数优化**：拒绝“暴力穷举”和“过拟合”。集成 Optuna 贝叶斯优化框架，内置严格的样本内训练与样本外验证切分机制。支持Calmar比率等机构级指标作为优化目标，并提供热力图与参数平原可视化，助您寻找真正穿越牛熊的稳健参数。

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

#### 2\. 配置API密钥

打开 `config.py` 文件，并填入您的 `TUSHARE_TOKEN`。如果您没有，可以前往 [Tushare Pro](https://tushare.pro/user/token)
免费注册获取。

```python
# config.py
TUSHARE_TOKEN = 'your_tushare_token_here'
```

#### 3.a. 运行回测 (内部模式)

此模式适用于直接在框架的 `strategies/`、`stock_selectors/` 等目录中编写逻辑。

使用 `run.py` 脚本执行回测。您可以通过命令行参数灵活地选择策略、标的、资金和手续费。

```bash
# 运行示例MACD策略（使用默认参数）
python ./run.py sample_macd_cross_strategy

# 兼容驼峰类名
python ./run.py SampleMacdCrossStrategy

# 回测贵州茅台(SHSE.600519)，并设置50万初始资金
python ./run.py sample_macd_cross_strategy --symbols SHSE.600519 --cash 500000

# 设置回测开始时间以加快运行
python ./run.py sample_macd_cross_strategy --symbols SHSE.600519 --cash 500000 --start_date 20250101

# 使用风控策略
python ./run.py sample_macd_cross_strategy --symbols SHSE.600519 --risk sample_stop_loss_take_profit

# 使用风控策略并传入自定义参数
python ./run.py sample_macd_cross_strategy --symbols SHSE.600519 --risk sample_stop_loss_take_profit --risk_params "{'stop_loss_pct':0.03,'take_profit_pct':0.08}"

# 设置多个标的及指定数据源
python ./run.py sample_multi_portfolio_strategy --symbols=SHSE.510300,SZSE.000001,SHSE.600519 --data_source=tushare

# 自定义选股策略
python ./run.py sample_multi_portfolio_strategy --selection sample_manual_selector

# 命令行覆盖策略params参数
python ./run.py sample_momentum_strategy --symbols SHSE.600519 --params "{'momentum_period': 10}"

# 使用额外数据源
python ./run.py sample_extra_data_strategy --symbols=SZSE.000001

# 查看所有可用参数
python ./run.py --help
```

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
    ```

    这种方式同样适用于 `--selection` 和 `--risk` 参数。

4.  **注意事项**

- 建议将解释器指向本框架，并在框架的requirements.txt管理依赖，并在本框架环境中执行策略

- 文件夹/包命名**请勿**和框架相同，建议添加```my_```前缀或```_custom```后缀。

- 如果有自定义指标算法，请新建自定义py脚本，并通过```from common.indicators import *```引入框架的指标算法库。


#### 3.c. 参数优化 (进阶)

告别“看图说话”的手动调参。使用 `optimize.py` 脚本，您可以定义参数搜索空间，利用 AI 算法自动寻找最优解。

为了防止过拟合，框架强制推荐使用**训练集/测试集分离**模式。

#### 3.c. 参数优化 (进阶)

告别“看图说话”的手动调参。使用 `optimize.py` 脚本，您可以定义参数搜索空间，利用 AI 算法自动寻找最优解。

为了防止过拟合，框架强制推荐使用**训练集/测试集分离**模式，并默认开启**“地狱模式”**——即默认在 2018 年熊市训练生存能力（Calmar），在 2019-2020 年牛市验证盈利能力。

```bash
# 1. 默认模式 (推荐)：
# 不指定时间参数时，自动使用默认的“抗过拟合”周期 (2018训练/2019-2020测试) 和 Calmar 目标
python ./optimize.py sample_momentum_strategy --symbols SHSE.510300 --opt_params "{'momentum_period': {'type': 'int', 'low': 10, 'high': 60, 'step': 1}}" --n_trials 50
```

```bash
# 2. 自定义周期：
# 如果您需要针对特定时间段（例如近期行情）进行优化，请显式覆盖时间参数
# 示例：在 2021-2022 年训练，2023 年测试
python ./optimize.py sample_momentum_strategy --symbols SHSE.510300 --opt_params "{'momentum_period': {'type': 'int', 'low': 10, 'high': 60}}" --train_period 20210101-20221231 --test_period 20230101-20231231 --n_trials 50
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

## 框架目录说明

```
QuantAda/
├── backtest/               # 回测模块
│   └── backtester.py       # 回测执行器
├── common/                 # 通用逻辑模块
│   ├── indicators.py       # 指标算法聚合库，自定义使用Ta-Lib及MyTT
│   └── mytt.py             # MyTT指标计算库
├── config.py               # 配置文件 (API密钥等)
├── data/                   # 行情数据缓存目录
├── data_providers/         # 主数据源模块
│   ├── akshare_provider.py # AkShare数据源适配器
│   ├── base_provider.py    # 数据源抽象基类
│   ├── csv_provider.py     # CSV数据源适配器
│   ├── manager.py          # 数据源调度与缓存管理器
│   ├── sxsc_tushare_provider.py       # 山西证券TuShare数据源适配器
│   └── tushare_provider.py # TuShare数据源适配器
├── data_extra_providers/   # 额外数据源模块
│   ├── http_extra_provider.py         # HTTP额外数据获取类
│   └── mysql_extra_provider.py        # MySQL额外数据获取类
├── stock_selectors/        # 自定义选标的包
│   ├── base_selector.py    # 选标的抽象基类
│   └── sample_manual_selector.py      # 手动选择三支标的样例类
├── strategies/             # 策略模块
│   ├── base_strategy.py    # 策略抽象基类
│   ├── sample_custom_indicator_strategy.py # 使用自定义指标计算库的MACD样例策略
│   ├── sample_macd_cross_strategy.py  # MACD样例策略
│   ├── sample_extra_data_strategy.py  # 使用额外数据样例策略
│   └── sample_multi_portfolio_strategy.py  # 多标的等权样例策略
├── live_trader/            # 实盘交易模块
│   ├── adapters/           # 平台适配器层 (将外部API统一)
│   │   ├── base_broker.py  # Broker 抽象基类
│   │   └── gm_broker.py    # 掘金(gm)平台具体实现
│   ├── samples/            # 各平台实盘入口文件样例
│   │   └── gm_main_sample.py
│   └── engine.py           # 实盘交易引擎 (驱动策略运行)
├── requirements.txt        # Python依赖包
├── optimize.py             # 参数优化启动器 (Optuna)
└── run.py                  # 命令行回测启动器
    
```

## 样例截图

### 终端执行回测
![backtest_mode_in_terminal](https://github.com/SUTFutureCoder/QuantAda/blob/main/sample_pictures/backtest_mode_in_terminal.png?raw=true)

### 券商平台执行回测
![backtest_mode_in_broker](https://github.com/SUTFutureCoder/QuantAda/blob/main/sample_pictures/backtest_mode_in_broker.png?raw=true)

### 券商平台执行实盘
![live_mode_in_broker](https://github.com/SUTFutureCoder/QuantAda/blob/main/sample_pictures/live_mode_in_broker.png?raw=true)

### 框架和自定义策略工程分离
![public_private_split](https://github.com/SUTFutureCoder/QuantAda/blob/main/sample_pictures/public_private_split.png?raw=true)  

### 参数优化启动器
![optimizer](https://github.com/SUTFutureCoder/QuantAda/blob/main/sample_pictures/optimizer.png?raw=true)  


在框架执行自定义策略命令样例  

```python ./run.py strategies_custom.reverse_trader_multiple_actions_strategy --selection=stock_selectors_custom.reverse_trader_multiple_actions_selector```

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

## 许可证 (License)

MIT
