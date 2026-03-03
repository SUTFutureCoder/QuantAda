# QuantAda Vibe Coding Prompts

本目录用于“复制即用”的 AI Prompt 模板，目标是降低接口对接与命令编排成本。

## 目录结构
- `broker/BROKER_PROMPT.md`: 新券商适配器生成
- `strategy/STRATEGY_PROMPT.md`: 新策略生成
- `command/COMMAND_PROMPT.md`: 自动生成可执行 `run.py` 命令（回测/优化/实盘）
- `data_provider/DATA_PROVIDER_PROMPT.md`: 新数据源适配器生成
- `selector/SELECTOR_PROMPT.md`: 新选股器生成
- `risk_control/RISK_CONTROL_PROMPT.md`: 新风控模块生成
- `alarm/ALARM_PROMPT.md`: 新报警通道适配器生成
- `metric/METRIC_PROMPT.md`: 新优化评分函数生成（用于 `--metric`）
- `recorder/RECORDER_PROMPT.md`: 新 Recorder 生成（交易与绩效落库/落消息）
- `sdk_plugin/SDK_PLUGIN_PROMPT.md`: 外部项目插件化接入与命令生成
- `debug_fix/DEBUG_FIX_PROMPT.md`: 基于命令+日志的定位修复模板

## 使用建议
1. 先选最贴近任务的子目录和 Prompt 文件。
2. 按模板填写输入区块（目标、参数、日志、约束）。
3. 把完整文本发给 AI，然后让 AI 直接改代码并验证。
4. 最后用 `debug_fix/DEBUG_FIX_PROMPT.md` 做回归和稳定性检查。

## 当前框架行为基线（2026-03）
1. Broker/Engine 已切换为**无状态执行**：不再使用 `deferred`/`buffered` 队列保存历史买入意图。
2. 买单拒绝后在同回调内**当场降级重提**（默认最多 5 次）；达到上限后放弃本 K，下一根 K 重新决策。
3. 卖出侧遵循可卖仓位约束（A 股等 T+1 场景应基于 `sellable`/`available_now` 等字段），避免“仓位不足”反复拒单。
4. 适配器层禁止维护本地资金/仓位缓存；实时状态必须以柜台查询为准。
5. 实盘每个自然日首次 `run` 会在**拉数据前**执行隔夜在途委托清理（可用 `config.KEEP_OVERNIGHT_ORDERS` 保留隔夜单）。
6. 隔夜清理失败会最多重试 5 次；若仍未清空，在继续本轮执行前会记录详细日志并推送 ERROR 级别报警。
7. `get_pending_orders` 统一契约要求包含 `id` 字段，并由适配器实现 `cancel_pending_order(order_id)` 支持按单撤单。
