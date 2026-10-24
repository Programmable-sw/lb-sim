# PFC 扩散与预 Trimming 仿真

基于 htsim，验证接收端消费受限时，Leaf 预 Trimming 能否限制 PFC 向上游扩散、
改善受害通信，并量化受阻通信的裁剪与可靠恢复代价。

## 模型

- **网络**：128 NIC、8 Leaf、8 Spine，400 Gbit/s；链路传播 500 ns，交换流水线 200 ns。
- **交换机**：每台 20 MiB Buffer，ECN 100/400 KiB、Pmax 20%；动态 PFC
  `XOFF = 8192 B + shared_free / 8`，XOFF–XON 滞回 32 KiB（受 XON 下限约束）。
- **端侧**：有限 NIC RX 与控制处理资源，真实 host 消费和 PFC；Unordered 接收、
  16K PSN 追踪、64-bit SACK、选择性重传及 live pacing。
- **Hybrid 路由**：A2A 使用 OPS、MP-RDMA 或 REPS，默认 CC 为 `dctcp_variant`；
  背景长短流使用 per-flow ECMP/DCQCN。OPS 为每包随机 entropy 后哈希选路。

预 Trimming 仅作用于收到真实下游 PFC PAUSE 的 Leaf 出口，在完整包准入前判断：

```text
出口 PAUSE 有效 && 队列内存占用 + 新包内存大小 > 水线
```

满足条件时裁剪 Payload，Header 经有限控制队列到达接收端，触发 PRETRIM-NACK 和可靠恢复。
新发与重传包使用相同条件，已排队的 Payload 不回收。PRETRIM-NACK 不直接降低 cwnd。

## 构建与运行

需要 Python 3、make 和支持 C++11 的编译器。在仓库根目录执行：

```bash
python3 experiments/pretrim/build.py --jobs 2
python3 -m unittest discover -s experiments/pretrim/tests -p 'test_*.py'

python3 experiments/pretrim/run_regular.py \
  --workload B3 --lb ops --trim-threshold 100 --seed 41 \
  --output experiments/pretrim/runs/B3-ops-s41 --execute
```

`--trim-threshold` 单位为 KiB，默认 `100`；`off` 关闭裁剪，`0` 表示在 PAUSE 期间
裁剪所有新完整包。`--normal` 使用正常 NIC 消费，作为无故障参考。
省略 `--execute` 只生成流量和运行命令。每次执行使用独立输出目录。

## 负载与观测指标

常规负载提供 B3（Hybrid）、W1（单受阻 NIC）、W2（A2A）及 W3-2/W3-4（多受阻 NIC）。
B3 共 658 QP、约 235 MiB Payload，包含两组独立 A2A、背景长流及周期突发短流。
默认 NIC112/113 在 50–250 μs 停止 host 消费，PFC 由真实 RX 占用达到 XOFF 自动触发。

| 通信 | B3 业务组成 | 指标 |
|---|---|---|
| A2A-A | 18 NIC、306 QP，包含受阻 NIC112/113 | 完整 JCT |
| A2A-B | 16 个健康 NIC、240 QP | 完整 JCT |
| Victim | 发往同 Leaf 健康 NIC 的长短流 | 长、短流分别计算平均 FCT |
| Blocked | 发往受阻 NIC 的长短流 | 长、短流分别计算平均 FCT |

暂停时长配对示例：

```bash
python3 experiments/pretrim/run_pause_sweep.py \
  --trim-threshold off 100 --pause-us 50 100 200 \
  --output experiments/pretrim/runs/pause-comparison --execute
```

## 验证流程

1. 检查实际配置与路径，确认唯一可靠交付、缓存守恒和最终排空。
2. 核对事件链：消费下降 → NIC PFC → Leaf 积压 → 上游 PFC → Victim 实际等待。
3. 同 LB、同 seed、同负载配对无故障参考、无裁剪对照和预 Trimming 实验。
4. 比较通信完成时间，同时记录 Fabric PAUSE、裁剪、重复裁剪、重传、RTO 和资源拒收。

保留未完成与负收益样本；不同构建分开统计。减少 PFC 不直接等于改善业务性能。
输出目录保存命令、构建哈希、实际配置、逐流结果和 trace；`runs/` 与构建产物不纳入 Git。

实现依据与适配边界：[MP-RDMA/REPS 与 CC](docs/ARTIFACT_ALIGNMENT.md)、
[OPS](docs/OPS_ARTIFACT_ALIGNMENT.md)、[水线接口](docs/WATERLINE_INTERFACE.md)。
