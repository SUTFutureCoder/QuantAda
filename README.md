# QuantAda

基于适配器模式的回测实盘分离量化框架  
Ada是Adapter的缩写，也是向埃达及埃达语言的致敬

# 目标

- 回测和实盘分离，但策略共享
- M系列没有GM包，便于策略开发
- 适配器模式对接多个实盘系统
- 市面上拥有界面的框架越来越臃肿，本框架仅提供核心功能（无开发经验不友好）

# 目录结构

QuantAda/  
├── run.py # 主执行入口  
├── config.py # 配置文件 (例如API Token)  
├── requirements.txt # 项目依赖  
|  
├── core/  
│ ├── __init__.py  
│ ├── strategy.py # 策略抽象基类  
│ └── signals.py # 交易信号枚举  
|  
├── strategies/  
│ ├── __init__.py  
│ └── macd_crossover.py # 【共享】具体的MACD策略逻辑  
|  
├── engines/  
│ ├── __init__.py  
│ ├── backtest_engine.py # Backtrader回测引擎/适配器  
│ ├── live_engine_gm.py # 掘金量化实盘引擎/适配器  
│ └── live_engine_qmt.py # (预留) QMT实盘引擎/适配器  
|  
└── data/  
└── data_loader.py # 数据加载模块  
  
