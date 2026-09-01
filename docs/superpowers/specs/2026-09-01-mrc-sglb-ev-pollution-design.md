# MRC+SGLB EV 污染受控实验设计

## 目标与待证命题

实现真正的 `MRC+SGLB`：源 NIC 仍按 MRC 的 64-EV `SKIP_ONCE` 规则选择并在报文中携带逻辑 EV，交换机则启用默认 SGLB、独立重选实际 next-hop。四组 `RR`、`MRC`、`SGLB`、`MRC+SGLB` 使用相同 traffic、拓扑、队列、默认 `dcqcn_variant` 和可靠性语义。

实验只在给定假设下作场景性结论：端到端拥塞反馈 RTT 小于 15 us 的 SGLB 远端状态更新周期。它要依次验证：

1. MRC-only 中逻辑 EV 与实际物理路径保持一一对应，首个 ECN 反馈返回后跳过对应 EV 即可避开坏路径。
2. MRC+SGLB 中 SGLB 破坏该绑定；在远端状态更新前，替代 EV 仍可能被映射到同一坏路径。
3. 单个坏物理路径因此使多个逻辑 EV 进入 SKIP，形成 EV-state pollution。
4. 实际绕路时间由首次有效 MRC 反馈时刻推迟到 SGLB 远端 profile 生效时刻；持续 ECN 同时压低 QP 级 cwnd。
5. 在该受控场景中，关闭 SGLB 的 MRC 可能获得更低 FCT/ECN、更高吞吐；不宣称其普遍优于 SGLB。

## 实现边界

新增公开模式 `-lb mrc-sglb`。它复用 `LB_MRC` 的所有端侧默认值和诊断，但将交换机策略设为默认 SGLB。`-lb mrc`、`-lb sglb` 及其他模式的行为不得改变。

事件追踪限定为显式指定的 foreground flow，避免全局逐包日志。每条相关事件必须包含时间、flow、sequence、逻辑 EV、报文 path-id、实际 source-ToR uplink/spine、是否 ECN、MRC 状态转换、当前 cwnd，以及 SGLB profile version/age 或候选状态。由这些字段直接计算：

- `mapping_mismatch`: 同一 EV 被观察到走不同实际物理路径，或实际路径不等于 MRC identity path；
- `polluted_ev_count`: 因同一坏物理路径 ECN 而被置 SKIP 的不同 EV 数；
- `first_mrc_reaction_us`: 首次有效 MRC SKIP 时间；
- `last_bad_path_after_reaction_us`: 首次反应后最后一次命中坏路径的时间；
- `effective_avoidance_us`: 此后持续不再命中坏路径的最早时间；
- `steering_delay_us = effective_avoidance_us - hotspot_on_us`。

## 两级实验

### 机制级受控 case

使用现有 64-path 两层单平面 Clos，避免改变当前 MRC 固定 64 EV 与 SGLB top-K 语义。只运行一条足够长的 foreground flow，并对一个确定 spine 对应的远端下行链路施加可开关背景流。目标流先在空载状态运行，热点在流进行中开启；另跑全程空载对照。SGLB local update 保持默认 1 us，远端 update 显式设为 15 us；hop/switch latency 选择能使测得的反馈 RTT 小于 15 us 的现有配置，但报告使用实测 RTT，不把 8 us 写成事实。

热点必须满足：只使一条目标物理路径持续越过 ECN 阈值，其他路径保留容量，且在 update 前窗口内目标流有足够报文触发多个替代 EV。若默认随机 dispatch 使单次事件不可复现，runner 固定 seed 并从预声明的小型 seed 集合中选择第一个满足该机械条件的 seed；选择规则和所有未选 seed 都保留，不能按 FCT 结果挑 seed。

### 64-path 性能扩展

在相同 64-path 拓扑上使用多个长流/混合流量，固定三 seed，分别运行空载与排队热点。比较四方案的逐流配对 FCT（mean/p50/p99）、goodput、总 ECN/每 MiB ECN、TRIM/重传、cwnd 最低值与恢复时间。主比较是 `MRC` 对 `MRC+SGLB`；`RR` 是无状态端侧基线，`SGLB` 用于判断组合结果是否退化为网侧主导。第三与第四相同是待检验假设，不预设为验收条件。

## 判据与边界

机制证明必须同时满足：实测反馈 RTT `< 15 us`；MRC-only 的首个有效 SKIP 后不再命中对应坏物理路径；MRC+SGLB 在该时刻后、远端更新生效前仍由至少两个不同替代 EV 命中同一坏路径；其有效绕路时刻与远端 profile 生效时刻对齐而非与 RTT 对齐。

性能结论只在机制判据成立的 cell 上报告。若 `MRC+SGLB` 没有劣于 MRC，则如实报告未观察到性能反转，并区分是污染存在但不足以影响完成时间，还是机制前提未建立。空载对照应不出现系统性污染或显著性能差距。

所有原始命令、binary/traffic hash、逐事件 CSV、逐 cell 汇总和报告均保留；runner 必须拒绝配置不符、flow 未完成或 RTT 前提不成立的 cell。
