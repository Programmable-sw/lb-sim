# 预 Trimming 水线接口

当前唯一模式为 Leaf 逐出口真实 PFC 门控，默认启用 100 KiB。

```text
trim = enabled
       && target_egress_actual_pause
       && queue_memory_bytes + full_packet_memory_bytes > threshold_bytes
```

决策位于完整包准入前。裁剪后 Header 进入现有有限控制通道；既有 Payload
不会回收，新发及重传包使用相同条件。MMU、PFC、CC、PRETRIM-NACK 和可靠恢复未调整。
Spine 不执行预裁剪，其他未被 PAUSE 的出口也不执行预裁剪。

| 命令行 | 行为 |
|---|---|
| 省略 `--trim-threshold` | 启用 100 KiB |
| `--trim-threshold 400` | 启用 400 KiB |
| `--trim-threshold 0` | PAUSE 期间所有新完整包均符合裁剪条件 |
| `--trim-threshold off` | 无裁剪对照 |

`--policy`、`--trim-risk-mode`、`--trim-gamma-bytes`、早期水线和并集模式参数已移除，
旧选项会被拒绝，避免静默回退。

历史保留结果仍含 B0/S0/S100/S200/S400 等标签及原始命令；这些标签是数据 provenance，
不是当前可选模式。当前脚本以 `waterline100` 等名称生成实验目录。

验证：默认/off/0 的真实网络测试、移除选项拒绝测试，以及三种 LB 的旧版固定
100/400 KiB 与新版六组微型对照。六组 summary 的保留字段逐项一致。
OPS 旧 fixture 的两个计数和一条完成事件已按冻结的新 OPS 构建校正，并核对新旧执行结果一致。

结果清理仅删除明确属于 Global、Ingress-risk 和并集模式的运行，混合 CSV 按模式过滤，
保留逐出口固定水线及无裁剪数据。已删除模式的快照和可执行文件不再作为运行入口。
Git 的历史对象未重写，未自动暂存、提交或推送。
