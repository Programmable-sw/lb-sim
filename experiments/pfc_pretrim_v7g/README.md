# V7G 预 Trimming 仿真

验证预 Trimming 能否抑制 PFC 扩散、改善 Victim 通信，并量化受阻业务的恢复代价。

## 配置

- 128 NIC、8 Leaf、8 Spine；400 Gbit/s，传播 500 ns，流水线 200 ns。
- 每交换机 20 MiB；ECN 100/400 KiB、Pmax=20%。
- 动态 XOFF=`8192+shared_free/8`，PFC 滞回 32 KiB。
- NIC RX 512 KiB，全局 metadata 4096 项；RTO 1 ms。
- 默认 Unordered Transport；保留 Limited OOO。额外 Trim-induced CE 关闭。
- B3：A2A 使用 OPS/MP-RDMA/REPS，背景长短流使用 ECMP；W1/W2/W3 由被测 LB 控制全部流。

水线：`G64/80/100` 为 Global 固定 egress 水线；`S100/200/400` 为 Selective 固定水线（KiB）。
`IR` 为 ingress-risk，Gamma=514352 B；`T32/T128` 为 Gamma=32768/131072 B。

## 构建与运行

在仓库根目录执行：

```bash
python3 experiments/pfc_pretrim_v7g/current/build.py --jobs 2
python3 -m unittest discover -s experiments/pfc_pretrim_v7g/current/tests -p 'test_*.py'

# 默认只生成输入与命令；确认后增加 --execute 执行单点。
python3 experiments/pfc_pretrim_v7g/current/run_regular.py \
  --workload B3 --lb ops --policy IR --seed 41 \
  --output experiments/pfc_pretrim_v7g/current/runs/B3-ops-IR-s41
```

负载支持 `B3/W1/W2/W3-2/W3-4`。每点使用独立输出目录，不覆盖已执行结果。

## 验证流程

1. 核对流量、实际路径、构建哈希及 resolved 配置。
2. 检查唯一交付、RX/MMU 守恒、可靠恢复和最终排空。
3. 验证真实链条：消费下降 → NIC PFC → Leaf 积压 → 上游 PFC → Victim 实际等待。
4. 同 LB、同 seed 配对：NoFault/B0（`--normal`）、Fault/B0、Fault/Trimming，其他配置不变。
5. 比较 A2A-A/B JCT、Victim/Blocked 长短流 FCT、makespan，以及 PAUSE、Trim、重传和 RTO。

没有真实扩散或 Victim 受害时，不计为隔离成功；减少 PFC 不等于应用获益。
未完成流和负收益全部保留。多 seed 先计算每 seed 指标，再做配对统计，不混用历史构建。

输出包括 `RUN.json`、`RESOLVED_CONFIG.json`、`FLOWS.csv`、`RESULT.json` 和压缩 trace。
`results/`、`references/`、`.build/`、`runs/` 不纳入源码提交。
