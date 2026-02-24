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
