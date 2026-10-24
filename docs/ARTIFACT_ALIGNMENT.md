# MP-RDMA、REPS 与 CC 对齐

参考仓库： https://github.com/tommasobo/REPS_EuroSys_Artifact

固定提交：`b751588745b57e8264ce22d9a40df67bdedd62c0`。2026-10-20 用 `git ls-remote origin HEAD` 核实其仍为远端 HEAD。
本次对齐选路机制，然后对齐 DCTCP 分支；NSCC 是独立 CC，不是 LB。
官方脚本也有 `-sender_cc_algo mprdma` 的配置，不能将其与 `DCTCP` 混称为同一分支。

## 实际变化

| 项目 | 原适配 | artifact 路径 |
|---|---|---|
| 路径表示 | MP/REPS 主要保存物理 Spine 编号 | 保存 entropy，经过现有 FreeBSD hash 映射到物理路径，ACK/NACK 回显实际 entropy |
| MP-RDMA LB | 纸面机制适配：ACK-credit FIFO、awnd、32-PSN剪枝、1%探测等 | 按 `processAck` 的 cwnd/flight 条件决定反馈是否可复用；`processEv_mprdma` 更新 entropy，`nextEntropy_mprdma` 选择；不叠加另一套纸面适配信用 |
| REPS | fresh缓存与history近似、按时间退出 | 对齐 `FREEZING` 分支的8项环形缓存、fresh消费、冻结重用、反馈驱动退出、按BDP计数的探索 |
| Freezing进入 | 超时或自定义trim/missing扩展 | 原生RTO进入；普通NACK不直接进入，PRETRIM更不被当作路径故障 |
| Freezing退出时间 | 100 μs适配 | 默认10,000 μs，对应作者静态默认10^10 ps；部分作者故障脚本显式使用100 μs，不能混为默认值 |
| DCTCP ACK | 对每个SACK新确认包单独调用 | 对一条ACK的新确认字节聚合调用一次，使用该ACK的CE；不伪造额外entropy回显 |
| NSCC | 未接入 | 移植上游延迟估计、比例/公平/快速增长、ECN减窗、quick-adapt、忽略区间及周期调整 |

代码：`experiments/pretrim/src/artifact_lb.h`、`artifact_cc.h`、`artifact_events.inc`。
不是重新实现整套作者UEC传输，也不是声称完整复现原MP-RDMA论文；对齐对象是该固定artifact的对应分支。

## CC公式与参数

`dctcp_variant` 使用上游 `updateCwndOnAck_DCTCP` 的整数字节运算：

- 无CE：`cwnd += newly_acked_bytes * MTU / cwnd`。
- CE：`cwnd = max(MTU, cwnd - newly_acked_bytes / 3)`。
- 普通有效NACK：减去被NACK报文对应字节，最低一个MTU。
- PRETRIM有效NACK：保留可靠恢复，不直接loss-based减窗。这是明确保留的本地扩展，不是作者原生规则。

NSCC按上游 `initNsccParams`/`initNscc` 和ACK/NACK函数移植，参考RTT=12 μs、参考带宽100G，
当前网络带宽400G、初始化网络RTT=12 μs；目标排队延迟6 μs、调整周期12 μs，
delay-alpha=0.0125、gamma=0.8。base RTT按真实ACK样本向下更新，BDP/maxwnd同步更新。
初始cwnd显式保持当前50包，不按性能重新标定；NSCC的这项值是显式实验参数，不称作者默认值。

两个CC在本模型使用4096B逻辑Payload为窗口字节单位；不是将4192B物理内存或4230B线速字节
混入窗口。物理序列化、缓存计账和应用发送上限不变。该单位映射与作者原始MTU/头部划分不同。

## 保留的模型与适配边界

- 原128 NIC、400G、20MiB MMU、ECN/PFC、32KiB滞回、NIC RX/消费及有限控制服务器不变。
- 当前Unordered接收、有限资源、64-bit SACK、attempt去重、选择性恢复和1ms RTO保留。
- SACK新增确认字节用于CC聚合，但补充SACK位不会生成独立原生路径信用。没有移植作者的接收端ACK压缩/`recvd_bytes`全部接口。
- flight资格仍映射到本模型可靠发送账本（包括已预约WQE），不是声称与作者NIC所有队列状态逐位等价。
- 重传仍走现有选择性恢复与live pacing，不将作者UEC整体替换进来；尚未声称作者重传调度器全量等价。
- 每QP独立随机流用于复现和隔离ECN随机性；不保证与作者全局`rand()`的同seed逐包序列一致。
- Hybrid背景始终per-flow ECMP/DCQCN；对齐的CC和LB仅控制被测A2A。Pure MP/REPS使用指定CC；Pure ECMP默认仍DCQCN，显式NSCC可选择。
- GLB、Hybrid负载、trimming门控和水线不改。

## 使用与验证

常规runner默认 `--lb-implementation artifact --a2a-cc dctcp_variant`；NSCC使用 `--a2a-cc nscc`。
`--reps-freeze-us`默认10000，结果必须记录实际值。旧直接命令默认legacy，旧回归可显式使用
`--lb-implementation legacy`。Limited OOO只允许legacy。不要跨配置混用性能结果。

```bash
python3 experiments/pretrim/build.py --jobs 2
python3 -m unittest discover -s experiments/pretrim/tests -p 'test_*.py'
python3 experiments/pretrim/run_regular.py --workload B3 --lb reps \
  --trim-threshold 100 --output experiments/pretrim/runs/reps-waterline100
```

当前仅保留 Leaf 固定出口水线；默认 100 KiB，`--trim-threshold off` 用于无裁剪对照。
旧 Global、Ingress-risk 和并集模式、对应脚本及数据已清理。

### 证据层次

1. 算法单元测试：MP反馈资格、REPS冻结/退出/探索、合法重复反馈和PRETRIM免直接降窗。
2. 上游函数对照：1000次DCTCP ACK、2000次NSCC ACK及穿插NACK，cwnd/base RTT/平均delay逐步匹配；
   REPS环形缓存与未修改的上游buffer实现对照。引用代码及BSD-2-Clause许可证随源码保留。
3. 真实网络测试：OPS/MP/REPS × DCTCP/NSCC六组合；另有MP/REPS真实Payload丢失和ACK blackout，
   验证合法RTO及REPS冻结、唯一交付、物理守恒和最终排空。
4. 性能证据使用同构建、同负载及同 seed 的无裁剪/固定水线配对，不预设优于OPS。
