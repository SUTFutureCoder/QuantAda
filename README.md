# QuantAda

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/release/python-380/)

一个优雅、可扩展的量化交易框架，实现算法的分模块独立或协作开发。

`Ada` 是 `Adapter`（适配器）的缩写，也借此向计算机先驱**阿达·洛夫莱斯 (Ada Lovelace)** 及以她命名的Ada语言致敬。

作者看不惯市面上一众机构或开发者，通过高年化忽悠小白爆金币的风气，让量化交易回归敬畏、诚信、以技术为本的初心。

## 核心特性

- **策略与引擎解耦**：得益于适配器模式，您只需编写一次纯粹的策略逻辑，即可无缝运行在 `backtrader` 回测引擎和掘金量化等实盘环境中。
- **模块化与可扩展**：
    - **数据层**：支持指定主数据源和额外数据源。默认采用责任链模式，按 `PRIORITY` 优先级获取数据，并实现自动增量更新与本地缓存，保证数据获取的稳定与高效。
    - **策略层**：支持将指标计算（`common/indicators`）抽象为公共模块，集成MyTT、Ta-Lib的同时，方便团队成员共享和组合，避免重复造轮子。
    - **引擎层**：通过适配器模式清晰地隔离了回测与实盘，您可以轻松添加对QMT、VN.PY等其他平台的适配器，而无需改动任何策略代码。
- **轻量级与专注**：框架只提供核心的骨架，没有集成任何臃肿或非必要的功能。每一行代码都为专业开发者服务，确保最大的灵活性和透明度。

## 快速开始

#### 1. 环境准备

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

#### 2. 配置API密钥

打开 `config.py` 文件，并填入您的 `TUSHARE_TOKEN`。如果您没有，可以前往 [Tushare Pro](https://tushare.pro/user/token)
免费注册获取。

```python
# config.py
TUSHARE_TOKEN = 'your_tushare_token_here'
```

#### 3. 运行回测

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

# 设置多个标的及指定数据源
python ./run.py sample_multi_portfolio_strategy --symbols=SHSE.510300,SZSE.000001,SHSE.600519 --data_source=tushare

# 自定义选股策略
python ./run.py sample_multi_portfolio_strategy --selection sample_manual_selector

# 使用额外数据源
python ./run.py sample_extra_data_strategy --symbols=SZSE.000001

# 查看所有可用参数
python ./run.py --help
```

#### 4. 部署实盘 (以掘金量化为例)

框架通过 `live_trader` 模块实现与外部平台的松耦合对接，策略代码无需修改即可复用。

1.  **配置PYTHONPATH**：在操作系统的 `高级系统设置→环境变量` 中，添加本框架的项目根目录到 `PYTHONPATH` 中。
2.  **创建策略入口**：在掘金新建策略，参考 `live_trader/samples/gm_main_sample.py` 的代码，将 `if __name__ == '__main__'` 上方代码复制到掘金的 `main.py` 文件中。
3.  **配置策略**：修改 `main.py` 中的 `config` 字典，使其与您的回测命令行参数对应。`config` 是连接框架与实盘的唯一“接头”。

    ```python
    # 示例: 对应回测命令 `python run.py MyStrategy --selection=MySelector --cash=500k`
    config = {
        'platform': 'gm',
        'strategy_name': 'MyStrategy',
        'selection_name': 'MySelector',
        # 'cash': 500000.0,  # 选填，用于虚拟分仓，不填则使用账户全部资金
        'params': { ... } # 策略自定义参数
    }
    ```
4.  **运行**：保存 `main.py` 并启动掘金策略。

## 目录说明

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
└── run.py                  # 命令行回测启动器
    
```

## 免责声明 (Disclaimer)

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
