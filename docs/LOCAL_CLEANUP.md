# 水线模式清理

2026-10-24：当前主实现仅保留 Leaf 对应出口真实 PFC 门控的固定水线，默认 100 KiB。
Global、Ingress-risk、Ingress+Egress 并集的入口、计算实现及关联 runner 已移除。

本地结果及历史归档中删除 2,388 个明确的退役模式运行/快照/可执行文件目标，
原文件大小合计 18,200,401,220 B（约 16.95 GiB）；主清理中的 137 个混合 CSV
及补充归档表按模式过滤，共移除 843,420 行。大表采用流式过滤，保留其固定出口水线与无裁剪行。
删除的数据没有额外备份；未跟踪的原始数据不能由 Git 恢复。

最新 27 点暂停时长结果保留于
`experiments/pretrim/runs/b3-newops-pause-s400-v1/ALL_RESULTS.csv`，
含 3 LB × 50/100/200 μs × 无裁剪/100/400 KiB，均保留原构建与命令。
历史命名中的 S100/S400 只作为原数据标签，当前接口不再使用 policy 名称。

删除清单与过滤统计见本地 `experiments/pretrim/runs/MODE_CLEANUP_MANIFEST.json`。
Git 历史和暂存区未改写，未自动提交或推送。

构建成功；22 项测试通过。三种 LB 的 100/400 KiB 六组微型新旧对照中，
保留的 summary 字段逐项一致。接口细节见 [水线接口](WATERLINE_INTERFACE.md)。
