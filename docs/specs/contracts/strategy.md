# Strategy Contract

本文件覆盖 `strategies/base_strategy.py` 及当前策略层约定。

## 1. Base Contract
1. 策略必须继承 `BaseStrategy`
2. 参数通过类属性 `params = {...}` 暴露
3. 运行时通过 `self.p.xxx` 读取参数
4. 生命周期核心方法:
- `init()`
- `next()`

## 2. Stateless Constraints
1. 不在策略内部维护虚拟现金、虚拟仓位、跨 K 买入意图队列。
2. 不设计“本 bar 卖出、下个回调强制补买”的 replay 状态机。
3. 若本 bar 买不进，交给下一根 K 重新生成目标。

## 3. Trading Universe Contract
1. 发单逻辑优先遍历 `self.tradable_datas`
2. `self.tradable_datas` 会自动应用三级 `ignored_symbols` 豁免:
- 全局配置
- 环境透传配置
- 策略本地配置
3. 如果只是做只读预计算，可以遍历 `self.broker.datas`

## 4. Supported Trading Paradigms
1. Arbitrary target / signal-driven:
- `self.broker.order_target_percent(data, target_pct)`
- `self.broker.order_target_value(data, target_value)`
2. Equal-weight rebalance:
- `self.execute_rebalance(target_symbols, top_k, rebalance_threshold)`

## 5. Current Rebalance Semantics
1. `execute_rebalance()` 当前是等权接口，不是权重字典接口。
2. `target_symbols` 传 `data` 对象列表，不传 symbol 字符串。
3. `top_k` 代表目标持仓槽位数。
4. 需要不等权目标时，应改用 `order_target_percent/value`。

## 6. Isolated Capital Semantics
1. 策略调仓使用真实持仓 + 在途订单做 bottom-up 盘点。
2. 被豁免的底仓不会计入策略可分配资金。
3. 若 broker 提供 `get_rebalance_cash()`，策略计划口径优先使用该值。
