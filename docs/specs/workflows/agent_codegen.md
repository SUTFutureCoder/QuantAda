# Agent Codegen Workflow

本文件描述 QuantAda 当前推荐的 agent 生成与修复流程。

## 1. Fast Generation Order
1. 先读相关 `docs/specs/*`
2. 再读相关 `agent_prompts/*`
3. 再读对应基类接口、加载器、运行时调用链
4. 最后再开始生成代码

## 2. Why This Order
1. spec 负责定义正式约束
2. prompt 负责给出高质量输入模板与输出格式
3. 基类与测试负责确认当前真实实现

## 3. Required Output Discipline
1. 优先最小有效改动
2. 保持无状态与自愈语义
3. 不引入旧 deferred / buffered 队列设计
4. 所有行为变更都应补 focused tests 或更新断言

## 4. Divergence Handling
1. 若 prompt 与代码不一致:
- 以代码/tests 为准
- 同步更新 spec 与 prompt
2. 若 spec 与代码不一致:
- 先确认是实现 drift 还是 spec 过期
- 结论明确后，在同一变更中完成修复

## 5. Practical Checklist
1. 是否读了对应 spec
2. 是否读了对应 prompt
3. 是否读了 base contract / loader / runtime code
4. 是否遵守当前 live runtime semantics
5. 是否补了针对性测试
6. 是否在最终说明里区分“已验证”和“未验证”
