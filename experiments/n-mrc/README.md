# n-MRC 实验分支说明

这里记录当前实验脚本里会跑到的负载均衡方案。原来的 full-snapshot `n-mrc` 已改名为 `netaware`；新的 `n-mrc` 是端侧 EV 轮询与源 Leaf SGLB 换路融合方案。`mrc` 是 MRC 文献复现 baseline。

## Canonical six 三 seed 对比

`run_nmrc_canonical_six_compare.py` 固定比较默认
`n-mrc encoded + better_ge3`、NetAware、SGLB、逐包 AR、MRC 和 REPS。
场景为健康 permutation、非对称 permutation、WebSearch 80% proxy、
P16/256 MiB 背景关/开，以及 P4/64 MiB 背景关；固定 seeds 为
`13,29,47`。同一 `(scenario, seed)` 的六个方案严格复用一份 traffic
matrix。前三个场景使用 p99 FCT，后三个 All-to-All 场景使用 CCT；排名先在
每个 `(scenario, seed)` 内除以该块最优值，再计算几何平均。
每个 `(scenario, scheme)` 的三-seed 绝对主指标也使用几何平均汇总；报告不使用
中位数，同时保留每个 seed 的原始值和 min/max 供离散性检查。

运行命令：

```bash
python3 experiments/n-mrc/run_nmrc_canonical_six_compare.py \
  --seeds 13,29,47 --workers 3 --timeout 3600 \
  --out experiments/n-mrc/output/nmrc_canonical_six_default_p8_3seed
```

runner 会按 simulator、traffic 和完整命令的 SHA-256 指纹复用已完成单元，
并拒绝非 canonical seed、重复或缺失的完成 flow ID、不完整的 Exact+Bounded
诊断、非正 primary metric 以及不完整的 108-cell 结果矩阵。三个 All-to-All
场景统一使用 `100000us` 安全截止；只读取已完成 CCT，不改变完成时刻。
FastCNP 即使在对应 QP 完成后才物理到达，仍计入 `fastcnp_arrived` 并释放报文，
同时单列 `fastcnp_after_done`；这类报文不再启动 EV cooldown。

## `ecmp`

Source 端在 flow 初始化时给该 flow 固定一个 `pathid`。交换机转发时在当前 ECMP next-hop 集合里用 `freeBSDHash(flow_id, pathid, hash_salt)` 取模选端口。同一 flow 的 packet 默认保持同一个 source `pathid`，路径变化主要来自每跳交换机的 hash 映射，而不是 source 逐包换 EV。

## `ecmp_rr`

交换机侧逐跳 round-robin。每台交换机维护本地 `_crt_route` 游标，在当前目的地址对应的 ECMP next-hop 集合中轮转选择端口；游标走过若干轮后会重新打乱候选端口顺序。`RR_ECMP` 模式下 ToR 做 round-robin，非 ToR 层仍退回 hash ECMP。

## `ops`

Source 端逐包随机喷洒。每次发送 data packet 时直接从 `path_space` 中随机选一个 EV/pathid 写入 packet，没有 ACK 反馈状态、没有 bad/clean 缓存，也不保证单个 flow 均匀遍历全部 EV。

## `rr`

Source 端 RR 是 MRC 的无状态对照。RR 与 MRC 使用相同的 per-QP 确定性 EV permutation、相同游标起点以及 encoded identity EV→path 映射；RR 不维护 ACTIVE/COOLING/FAILED/PROBING 路径质量状态，也不根据 ACK/ECN/NACK/RTO 改变候选集合。因此在 EV 数不超过 MRC 的 32 个 active EV 上限、且 MRC 尚未收到有效状态更新时，两者对新 data packet 的 EV/path 序列逐包一致。MRC 收到有效反馈后可以跳过或替换 EV，RR 则继续原始轮转。

## `reps`

Source 端维护一个固定大小的 clean-EV ring buffer。ACK 如果没有 `ECN_ECHO`，source 就把 ACK 携带的 `pathid` 写入 buffer；后续发送 packet 时优先取 buffer 里尚未消费的 clean EV。buffer 为空时退回随机选 path；可配置 warmup packet 数，在 warmup 阶段先随机探索。

## `avail`

`avail` 使用 source-ToR 生成的 ToR-pair bad bitmap。Source 端逐包选择 EV/pathid；source ToR 按 destination ToR 聚合返回反馈，ACK 带 ECN 或 TRIM NACK 时都会把对应 EV 标为 bad。达到 packet 数或时间触发条件后，把整个窗口的 bitmap 放进 ACK 反馈给 source NIC。

ECN+TRIM 是默认语义；显式 `-avail_ecn_only` 仅用于恢复旧 ECN-only 行为的消融实验。两种模式都不改变反馈周期、窗口后 all-GOOD 重置或全部 bad 时的保活 fallback。512-node、seeds 13/29/47 消融中，ECN+TRIM 对 healthy/degraded/mixed 无影响，path-hotspot p99 中位数改善 `0.04%`，incast p99 中位数改善 `2.0%`但不同 seed 方向不一致。结果见 `output/avail_trim_bad_ablation_512/avail_trim_bad_ablation_for_gpt.md`。

Source 端按 `(src ToR, dst ToR)` 只保存一份 availability bitmap，同一 ToR-pair 下不同 flow 直接读取这份共享反馈，不再复制 per-QP bitmap。每个 QP 只维护一个 selection counter，用 `src/dst/flow_id/priority/profile_version/epoch` 对 `0..P-1` 做可随机访问的虚拟 permutation；bitmap 中 bad 的 EV 被跳过，全部 bad 时才做保活 fallback。因此逻辑路径状态是 `P bits / ToR-pair`，每 QP selector 状态为 `O(1)`。

Avail 默认采用 fixed5：packet trigger 为 `1`，普通反馈和 TRIM 反馈都按固定 `5us` 节奏发布。旧的 bounded-window 配置保留为显式兼容入口：`-stor_feedback_pkts 32 -stor_feedback_min_us 5 -stor_feedback_max_us 20 -stor_trim_feedback_min_us 5`。因为 Avail 和 Grade 共用 STOR feedback 管线，这组参数对两者都生效；NetAware 使用独立 snapshot 接口，新 n-MRC 不使用周期 feedback interval。

## `grade`

`grade` 中 source ToR 根据 ACK 和 TRIM NACK 给每个 EV 维护一个多级状态。端侧仍然逐包填 EV，交换机仍然按 EV/hash 转发；Grade 只负责观测和反馈，不替端侧选路。代码内部继续复用 STOR 状态和反馈类型。

STOR 状态按 `(src ToR, dst ToR, ev_index)` 共享。同一对 ToR 之间的多个 flow 会使用同一份 EV 状态。当前实现里，端侧预先拥有一组 EV set；每个 EV 可以映射到一条稳定路径。真实 RoCE/NIC 里可以把 EV 理解成 UDP source port、IPv6 flow label 或其它会进入 ECMP hash 的 entropy 字段。仿真里仍用 `pathid` 表示。

Avail 和 Grade 共用 STOR feedback 管线，当前默认统一为 fixed5：

```text
Avail feedback_pkts = 1
Grade feedback_pkts = 1
stor_feedback_min_us = 5
stor_feedback_max_us = 5
stor_trim_feedback_min_us = 5
```

普通 ACK/ECN ACK 仍按 packet trigger 或 interval piggyback 整个 EV level vector。默认下 TRIM 不再绕过 5us 下限，因此三个反馈边界一致。反馈节奏与计分惩罚仍是两件事：`original`/`balanced` 让 TRIM 比 ECN 扣分更重，`simple` 则让二者使用相同扣分。

fixed5 只统一 Avail 和 Grade 的反馈触发节奏，不改变 packet-aging、全表反馈、评分公式或 endpoint selector 语义。共享 STOR 接口仍可显式切换到 `32 pkt + 5--20us`；NetAware 的 full snapshot 固定为 packet trigger 1、5us，新 n-MRC 则由实际换路事件触发通知。

Grade 默认只更新一个逻辑 4-bit score；显式 `-grade_complex_score` 时才恢复三个 8-bit 状态：

- `score`：路径信用分，越大越好。
- `ecn_acc`：complex mode 的近期 ECN 累计量。
- `trim_acc`：complex mode 的近期 TRIM 累计量。

当前默认是筛选出的 simplified p4 scorer。用户构想图对应的复杂公式仍可通过内部调试 profile `-stor_score_profile original` 复现：

```text
Clean ACK: score += 4
ECN ACK:   ecn_acc += 16; penalty = 8  + (ecn_acc  >> 3)
TRIM NACK: trim_acc += 32; penalty = 32 + (trim_acc >> 2)
levels:    GOOD >= 200, DEGRADED >= 120, BAD >= 50, AVOID < 50
```

显式 `-grade_complex_score` 使用原来的 `balanced` complex scorer。它采用 `score + ecn_acc + trim_acc` 三字段结构和 shift-decay：

```text
Clean ACK:
  ecn_acc  = ecn_acc  - (ecn_acc  >> 3)
  trim_acc = trim_acc - (trim_acc >> 2)
  score    = min(score + 4, 255)

ECN ACK:
  ecn_acc  = ecn_acc  - (ecn_acc  >> 3)
  trim_acc = trim_acc - (trim_acc >> 2)
  ecn_acc  = min(ecn_acc + 24, 255)
  penalty  = 16 + (ecn_acc >> 3)
  score    = max(score - penalty, 0)

TRIM NACK:
  ecn_acc  = ecn_acc  - (ecn_acc  >> 3)
  trim_acc = trim_acc - (trim_acc >> 2)
  trim_acc = min(trim_acc + 48, 255)
  penalty  = 48 + (trim_acc >> 2)
  score    = max(score - penalty, 0)
```

`score` 再映射成四个等级：

```text
GOOD      score >= 240
DEGRADED  score >= 160
BAD       score >= 80
AVOID     otherwise
```

端侧不重新计算 score，只消费 ToR piggyback 回来的等级。当前统一使用 scale-invariant shuffled ticket bucket，桶大小为 `K = 4 * path_count`，默认权重是：

```text
GOOD / DEGRADED / BAD / AVOID = 4 / 2 / 1 / 0
```

bucket 按路径相对权重做 largest-remainder 分配；绝对同比缩放的权重会得到相同 ticket allocation。未打乱的 base bucket 只在 `(src ToR,dst ToR)` profile 中保存一次。每个 QP 不再保存自己的 K 项 bucket，而是用一个 selection counter 和 `src/dst/flow_id/priority/profile_version/epoch` 虚拟打乱 bucket 下标并按需查询。AVOID 权重为 0，但 graded STOR 每 `K` 次选择轮转 probe 一个 AVOID EV，让 packet-aging profile 仍有机会收到 clean 反馈并恢复。

公开实验名 `avail` 对应内部 binary profile：source ToR 根据 ECN ACK 或 TRIM NACK 把对应路径置 bad，反馈窗口后清空 bad bitmap；端侧对共享 bitmap 做 unweighted virtual permutation。`-avail_ecn_only` 仅保留旧 ECN-only 行为用于消融。公开实验名 `grade` 默认对应 simplified p4 graded profile 和 weighted virtual permutation；`-grade_complex_score` 才切回 balanced complex scorer。

Grade 默认 simplified profile 为：

```text
-stor_score_profile simple
-stor_simple_score_params clean_gain congestion_penalty good degraded bad

default simple parameters:
  score range = 0..15
  clean ACK   = +1
  ECN/TRIM    = -4
  levels      = 12 / 7 / 3
```

`simple` 不维护 `ecn_acc`/`trim_acc`，恢复仍由 clean business-packet feedback 驱动，并没有删除 packet aging。512-node、7 workload、3-seed 对比中，`penalty=4, thresholds=12/7/3` 相比图片中的 `original` 明显降低 degraded/path-hotspot 尾部；相比 balanced，它改善 path hotspot 和 degraded-permutation 极端尾部，但 degraded-tornado 略退、32:1 incast 更差。基于当前机制简化目标，它已升为 Grade 默认，balanced 只作为显式 complex reference。历史筛选结果见 `output/stor_simplified_score_comparison/stor_simplified_score_comparison_for_gpt.md`。

### STOR aging profiles

STOR 当前可以用 `-stor_aging packet|time_ewma|hybrid` 比较三种恢复模型。

`packet` 是 baseline：EV score 只随 clean ACK、ECN ACK、TRIM NACK 更新。路径不收到包时不会自动恢复，适合长期慢链路，但瞬时 burst 后可能恢复偏慢。

`time_ewma` 在处理反馈前按时间衰减坏证据并恢复 score，参数为 `-stor_time_ewma_us ecn trim score`。它用来验证自动时间恢复是否会让长期坏路径过早回流。

`hybrid` 保留 packet 信号作为强证据，时间只负责 BAD/AVOID hold-down 过期后的低频 probe 资格，参数为 `-stor_hybrid_hold_us bad avoid` 和 `-stor_hybrid_probe interval_pkts clean_promote`。AVOID 到期后最多先回到 BAD，依靠低权重探测和 clean 包继续恢复。

诊断输出包括：

```text
RoceDiag ... stor_selected_good/degraded/bad/avoid=...
StorDiag stor_min_score=... stor_avoid_entries=... stor_avoid_exits=...
         stor_clean_signals=... stor_ecn_signals=... stor_trim_signals=...
```

小规模验证矩阵：

```bash
python3 experiments/n-mrc/run_stor_aging_compare.py
```

该脚本同时跑长期慢 uplink 和短 burst 恢复场景，并在 `summary.csv`/`comparison_report.md` 中列出 FCT、CCT、ECN/TRIM/NACK 和 STOR 最低分数/AVOID 进出次数。

## `netaware`

`netaware` 是原来的 full path-profile snapshot 机制：source leaf 为 `(src leaf, dst leaf)` 计算所有候选 `path_id` 的四级路径 profile，并通过 ACK 的 `netaware_feedback` 字段完整同步给 NIC。相比 `mrc` 让端侧按 ACK/NACK/RTO 管理 active/backup EV；`netaware` 让 leaf 交换机维护 ToR-pair 的路径状态，并把结果反馈给 NIC。

1.利用交换机的周期端口 snapshot 减少端到端信号的滞后性；
2.多流/多 NIC 共享交换机视图的路径表，让每个流有即时回避拥塞能力而非牺牲业务包的探索；
3.实现端侧可见的 EV-PATH 映射，协助 cc 使网络不再黑盒。

标准二层拓扑固定为 64 台 Spine；每台 Leaf 有 64 个主机下联和 64 个
Spine 上联。默认规模是 256 节点。自动生成的二层拓扑只接受
256/512/1024/2048/4096/8192 节点，对应 4/8/16/32/64/128 台 Leaf；
物理跨 Leaf 路径数始终为 64。其他规模必须使用显式拓扑配置文件。

仿真默认配置是：

```text
queue_type = composite_ecn_lb
cc         = dctcp_variant
rx_mode    = sp
ecn Kmax   = 0.8 * queue
EV/pathid  = source 端逐包填写，交换机按单 EV 哈希
```

EV 空间仍然用 `pathid` 表示。没有显式 `-paths` 时，source-controlled LB
会按拓扑自动校准 EV 数量。标准二层网络有 64 条跨 Leaf 物理路径，因此
默认 EV set 大小为 64；实验若显式设置更小的活跃 EV 数，必须标注为 EV
集合消融，不能当作另一种物理拓扑。

交换机分工：

```text
spine:
  每 5us 更新面向 destination 的下游端口 export snapshot。

leaf:
  每 1us 更新本地 leaf-to-spine 端口 snapshot。
  对每个 dst ToR 和每个 EV 计算 path level。
  一个 EV 对应的路径由两段队列组成：
    1. 本 leaf 到 selected spine 的 leaf uplink
    2. selected spine 到 dst leaf 的 spine downlink
  默认先把两段队列分别映射为 queue pressure，再用 noisy-OR 耦合为整条路径的 score，最后量化为 GOOD/DEGRADED/BAD/AVOID。
```

当前阈值和反馈节奏是：

```text
netaware_score_mode              = sglb_quantized
netaware_path_coupling           = noisy_or
netaware_score_q_range           = 0.20 / 0.80
netaware_score_util_range        = 0.90 / 1.00（接口保留，默认权重为 0）
netaware_score_weights           = 0.50 / 0.50 / 0.00 / 0.00
netaware_score_level_thresholds  = 0.10 / 0.40 / 0.60
netaware_level_weights           = 4 / 2 / 1 / 0
netaware_weight_adaptation       = good_share_cap
leaf local update            = 1us
spine export update          = 5us
feedback_pkts               = 1
feedback min/max            = 5/5us
```

score 公式为：

```text
local_hop  = local_q_pressure
remote_hop = remote_q_pressure
path_score = 1 - (1 - local_hop) * (1 - remote_hop)
```

queue pressure 按 composite RED 的 `Kmin=0.20`、`Kmax=0.80` 线性归一化。默认不用 30us average-utilization 参与评分，因为它对 30--40us 短流反应过慢；CLI 仍可显式设置 util 权重做消融。`additive` 和 `bottleneck` coupling 也保留为诊断模式。

当前状态传播采用与 SGLB 相同的周期缓存抽象：spine 首次按需建立 destination export，随后每 5us 更新；leaf 只读取该缓存，不再直接读取 spine 的实时 queue。仿真仍通过交换机对象共享 cache，不生成实际控制报文，因此没有计入 spine-to-leaf 通告的传播、排队、丢失和带宽成本。

量化规则固定为四级：`score < degraded -> GOOD`，`score < bad -> DEGRADED`，`score < avoid -> BAD`，`score >= avoid -> AVOID`。`worst_hop` 仍保留为兼容模式，但新的连续评分目标和 512-node 参数实验都应使用 `sglb_quantized`。

`netaware` 在 ACK 回到源端 leaf、准备下发给源 NIC 时重新计算完整 EV/path profile，并把四级 level vector piggyback 到 ACK 的 `netaware_feedback` 字段上。STOR 和 NetAware 不共享反馈语义：STOR 是 endpoint/business-packet observation based，可能只观察到实际经过的部分 EV；NetAware 是 switch/peer-state based，每次反馈都是覆盖全部 candidate path_id 的 full snapshot。

NIC 端只消费 NetAware snapshot，不直接看交换机队列。端侧按 `(src ToR, dst ToR)` 共享唯一 EV level profile 和 base weighted bucket，同一 ToR-pair 下所有 flow/QP 复用同一份反馈和 ticket allocation。收到新的 `netaware_feedback` 后，NIC 直接用最新完整 profile 替换旧 profile，不做 STOR 那类 hold/aging/probing/partial update。每个 QP 只维护 selection counter，通过虚拟 permutation 打乱共享 bucket 下标。

默认 selector 仍是 `shuffled_bucket`，默认初始等级基数仍是 GOOD/DEGRADED/BAD/AVOID = `4/2/1/0`，默认 weight adaptation 为 GoodCap（`good_share_cap`）。GoodCap 在 GOOD 路径稀少、单条 GOOD 的归一化概率超过 `(1 + nGOOD/P) / P` 时，把 4210 分布向全路径均匀分布软化；全 GOOD 时不改变均匀 spraying。固定 4210 仅作为显式消融保留：

```text
固定4210:
  -netaware_weight_adaptation off
```

除 `off` 和 `good_share_cap` 外不再提供其它软加权模式。

如果所有路径总权重为 0，则虚拟遍历完整路径空间做保活 fallback，避免死锁。GoodCap 的 seed-13 六场景基数扫描仍以 4210 的最坏场景保护最好，因此没有把基数改成 6210；完整记录见 `output/nmrc_goodcap_base_sweep_128_seed13/nmrc_goodcap_base_sweep_for_gpt.md`。

共享虚拟 selector 的历史 512-node、三 seed 对比见 `output/nmrc_shared_virtual_selector_comparison/nmrc_shared_virtual_selector_comparison_for_gpt.md`。这些历史目录名中的 `nmrc` 指现在的 NetAware；MRC 没有参与这次状态重构，仍保留 per-QP permutation、active/backup 和 EV 状态。

当前 1us leaf-local/5us spine-export 周期缓存模型与 ECMP-RR、OPS、REPS、MRC、SGLB、AR 的 512-node 七负载对比见 `output/nmrc_periodic_cache_packet_lb_512/nmrc_periodic_cache_packet_lb_512_for_gpt.md`。

## ECMP / CONGA / MP-RDMA / REPS 对齐实验

该对比固定使用二层 leaf-spine；`htsim_roce` 的默认层数也设为 2。普通
ECMP 使用与 ns-3.19 相同的 MurmurHash3 类五元组哈希。REPS 的 EV 先按
ECMP 哈希映射到物理路径，交换机不再用普通逐流 ECMP 二次覆盖该选择。
CONGA 的拥塞表按“源 leaf、目的 leaf”共享，而不是每 QP 私有；flowlet 的
最终选路在数据包抵达源 leaf、经过主机链路串行化之后完成，因此能读取该 leaf
已累积的本地 DRE，而不是在端点创建包时过早固定路径。并列最优路径与
ns-3.19 的 CONGA 实现一致，使用带种子的随机选择。

`run_mprdma_bursty_a2a.py` 默认使用与 ns-3 smoke test 相同的 16 主机、4 leaf、4 spine 两层拓扑，400 Gbps、500 ns 链路、0 ns 交换处理延迟、1 MiB all-to-all 集合流和 4–16 KiB ECMP 短流。四种方案共用 `dctcp_variant`、`shared_buffer_ecn`、20,000 byte ECN 阈值和动态 PFC。

CONGA 使用 Q=3、DRE 时间常数 160 us（32 us 更新周期、alpha=0.2）和 500 us flowlet timeout。报文的 LBTag 由实际 path-id 对应的 source-leaf uplink 生成；ACK 返回 DRE 路径最大值并替换该 uplink 的旧估计。ECN 只进入公共拥塞控制，不再被错误地写成 CONGA metric=255。

seed 13 的最新对齐矩阵位于 `output/htsim_two_tier_aligned_matrix_20260930/results.csv`。每个点均完成 240 条集合流及全部短流，RTO、lossy drop 和 composite drop 均为 0。这个 16 节点同步连续流烟测没有产生 CONGA flowlet timeout；因此单种子的并列路径哈希方差可能改变 CONGA 与 ECMP 的相对次序。5% 点的三种子结果保存在 `output/htsim_two_tier_seed_check_load5_20260930/results.csv`。

### 交换机共享缓冲与动态 PFC

`shared_buffer_ecn` 是增量队列模式；原 `composite_ecn_lb`、`lossless_input_ecn` 均保留。每台 `FatTreeSwitch` 分别维护 ingress port×PG、egress port×queue、ingress/egress service pool、总 ingress buffer 和 per-port headroom 账本。准入顺序与 ns-3.19 一致，先检查 egress，再检查 ingress，成功后原子更新两侧账本；出队按相反方向释放。默认共享池为 9 MiB。400 Gbps、500 ns、4096-byte payload 的自动 headroom 为 58,288 bytes，即 `2*link_flight + 2*(payload+48)`。

每条 ingress link 在 htsim 中对应一个 `LosslessInputQueue`。其动态 PAUSE 门限为：

```text
2 * 1048 + ingress_alpha * max(0, ingress_sp_limit - ingress_sp_used)
```

默认 `ingress_alpha=1/16`。1048 bytes 是参考 `SwitchMmu` 的固定保证单元，不随 4096-byte payload 改变。PFC 按 8 个 PG 独立维护，PAUSE 帧持续 5 us；重复 PAUSE 会刷新自动恢复计时器，显式 RESUME 会取消计时器。egress queue 0 严格优先，queue 1–7 round-robin，暂停只阻塞对应 PG。ECN 在释放当前包后按该 egress queue 的 shared bytes 判断；`Kmin=Kmax=20,000` 时为确定性阈值。

配置入口：

```text
-queue_type shared_buffer_ecn
-shared_buffer_mb 9
-shared_ingress_alpha 0.0625
-shared_egress_alpha 1.0
-shared_headroom_bytes 58288
-lossless_ecn_bytes 20000 20000
```

`shared_egress_alpha` 直接参与单个 egress queue 的动态准入；超过 egress service-pool、port、queue 或动态阈值会被诊断为 overflow。日志中的 `SharedBufferDiag` 给出单交换机峰值、越界次数、最小动态 PAUSE 门限及 PAUSE/RESUME 数量。

共享缓冲重测结果位于 `output/htsim_shared_buffer_matrix_final/results.csv`。

代码入口上，NetAware 使用独立的 `LB_NETAWARE`、NetAware selector 和 ACK snapshot；Avail 和 Grade 都复用 `LB_STOR`、`choose_stor_path` 和 source-ToR 观测管线，分别选择 binary bitmap 与 graded weighted profile。

NetAware 的 feedback cadence 固定在运行时实现中，不再保留反馈周期扫描入口。修改端侧选路或反馈实现后必须先运行 `make -C sim`，再链接 `sim/datacenter/htsim_roce`，避免使用陈旧的 `roce.o`。

早期二值 availability 只是 NetAware 的第一版基线。当前 NetAware 使用 GOOD/DEGRADED/BAD/AVOID 四级 profile。

## `n-mrc`

`n-mrc` 保留原版四级判定，是默认 N-MRC。源 Leaf 将当前路径与候选路径量化为 GOOD/DEGRADED/BAD/AVOID；满足原有严格升档条件时，换路与 FastCNP 成对发生。FastCNP 的 `PATH_REROUTE` 只通知源端冷却原 EV，不触发 DCQCN 降速；普通 ECN/ECN_ECHO 仍独立承担拥塞控制。

默认配置为 `encoded + better_ge3 + FastCNP on + rr_cooldown`。端侧按确定性打乱的 EV 顺序逐包轮询，收到换路通知后跳过原 EV 一个完整轮次。

## `n-mrc-fixed0.5`

该分支是原 `n-mrc4` 的直接改名，保留已完成实验的实际语义。只有以下三个条件同时满足时才执行 reroute + FastCNP：

```text
original_score >= 0.5
candidate_score < 0.5
original_score - candidate_score >= 0.25
```

默认 `absolute_threshold=0.5`、`relative_delta=0.25`。可用 `-nmrc_absolute_threshold` 和 `-nmrc_relative_delta` 显式调整；候选路径仍必须低于绝对阈值。

## `n-mrc-delta`

该分支只比较相对路径质量，不设绝对 0.5 门槛：

```text
original_score - candidate_score >= delta
→ reroute + FastCNP
```

默认 `delta=0.25`，用 `-nmrc_relative_delta` 调整。换路和 FastCNP 是同一配对动作；若 FastCNP 无法沿反向路径注入，则当前包不执行该次换路。

公开 N-MRC 入口只保留 `n-mrc`、`n-mrc-fixed0.5` 和 `n-mrc-delta`。旧实验名 `n-mrc-allcool-rr-reset`、`n-mrc1/2/4/5/6/7` 不再是可运行方案。

### 后续考虑方向

以下仅记录机制方向，当前不提供 `-lb` 入口：

1. **分段 delta**：以 0.5 为分界，在 0.5 前后使用不同 delta，以较小的高拥塞区 delta 加快绕路，同时用较大的低拥塞区 delta 抑制瞬时抖动。
2. **换路与冷却分级**：原路径绝对分数超过 0.5 即可换路，但只有相对分差达到更高阈值时，FastCNP 才携带 `cooldown 标志位`；无该标志的 FastCNP 只记录换路，不让端侧冷却 EV。

## `mrc`

`mrc` 是 MRC 文献简要复现，唯一公开入口是 `-lb mrc`。MRC 论文整理稿见 `docs/papers/MRC.md`。`-lb rr` 使用同类的确定性 per-QP 64-path 顺序，但不拥有或更新 MRC 路径质量状态，因此是首次有效反馈前的无学习 baseline。

Source 端为每个 RoCE QP 固定初始化 64 个独立 EV，全部从 GOOD/active 开始，没有 backup。单平面二层 Clos 中 EV `i` 一对一直接编码 physical path-id `i`；`src/dst/flow_id` 只打乱遍历顺序，不改变映射。

Data packet 携带实际选择的 `mrc_ev`，receiver 在 ACK/NACK 中回显该 EV。带 `ECN_ECHO` 的 ACK 与 TRIM NACK 对精确 EV 执行 OCP SKIP_ONCE：GOOD→SKIP，并设置一个 token；SKIP 期间重复反馈只计入 ignored 诊断，不 rearm。rotation 到达该 EV 的名义槽位时恢复为 GOOD，但当前包继续找下一个原有 GOOD。一次发送最多恢复一个 SKIP；若全体 SKIP，则普通 rotation 扫描回本次恢复的 EV后发送，不使用特殊 all-cooling fallback。OOO/SACK NACK 只进入选择重传，不改变 EV 拥塞状态。

### MRC 固定配置

当前不再提供 active-EV 或拥塞策略消融参数。所有 MRC 拥塞实验统一使用 64 active EV、0 backup、identity mapping 和 SKIP_ONCE。

每个 RR、MRC 目标流完成时仍输出 `MrcFlowDiag`。旧 active-EV/cooldown 对照 runner 已删除；历史结果只作为既有产物保留，不用于说明当前运行语义。
高压力下从短流无收益到长流开始受益的聚焦复跑见
[`mrc_pressure_transition_example.md`](mrc_pressure_transition_example.md)。
相同 5 ms 输入下 MRC 与默认 SGLB 的逐流严格配对结果见
[`mrc_sglb_pressure_transition_example.md`](mrc_sglb_pressure_transition_example.md)。
该轮不包含路径失效或动态故障实验。

OOO/SACK NACK 只进入 SP/SACK selective retransmission queue，不改变 EV 拥塞状态，也不触发 Go-Back-N replay。当前拥塞性能模式不启用故障恢复：正常 LOSS/RTO 不把 EV 转入故障状态，也不调度 probe；故障状态类型只作为未来接口保留。

`dctcp_variant` 独立维护 QP 级 congestion window。clean ACK 执行 `cwnd += 1/cwnd`，ECN ACK 执行 `cwnd -= 0.5`；OOO、TRIM、LOSS NACK 和 RTO 都执行 `cwnd -= 1`。默认 Exact+Bounded 传输不维护 `inflate`，发送额度为 `awnd=cwnd-inflight`，唯一 PSN 首次被 ACK/SACK 后才释放额度。MRC path state 与这个 QP 级窗口更新彼此独立。

`-lb mrc` 未显式覆盖时使用：

```text
queue_type              = composite_ecn_lb
roce_rx_mode             = sp
roce_sack_bitmap_bits    = 64
cc                       = dctcp_variant
roce_transport_semantics = mrc_exact_bounded
roce_trim_recovery       = exact
ecn_thresh               = 0.8
mrc_logical_evs          = 64
mrc_active_evs           = 64
mrc_backup_evs           = 0
mrc_congestion_reaction  = skip_once
mrc_failure_recovery     = disabled
```

上表描述的是只写 `-lb mrc` 时的 CLI 默认值。`dctcp_variant` 会在 MRC 根据 ECN 冷却精确 EV 的同时调整 source congestion window；显式使用 `-cc none` 时仍保留 MRC path-state 更新，但不做额外的发送窗口控制。当前 canonical MRC 性能矩阵使用 `dctcp_variant`，`cc=none` 只作为拥塞控制 ablation。

显式使用 `-cc dcqcn` 会切换到 rate-based DCQCN。该模式当前按
`400 Gbit/s / 7 us RTT / 350000-byte BDP` 校准，默认使用
`initial_alpha=0.6`、`min_rate=80 Gbit/s`、`alpha/rate interval=28 us`、
`CNP interval=16.8 us` 和 `byte_counter=1.4 MB`；它不改变 MRC 或 n-MRC
的路径选择和反馈语义。完整扫描与 ECMP 验证结果保存在
`output/nmrc_ecmp_dcqcn_400g_calibration/`。

运行时 `MrcEvModelDiag` 应报告 `mrc_active_evs=64 mrc_backup_evs=0`、identity mapping 和 alias ratio 1；`MrcPolicyDiag` 应报告 `policy=skip_once all_skip_resolution=ordinary_rotation`；`MrcFailureRecoveryDiag` 应报告 `enabled=0`。默认 `FinalCcMrcConfig` 继续报告 `dctcp_variant_inflate=disabled mrc_ecn_trim_penalty=mode_uniform roce_trim_recovery=exact`。

此前大量 Natural+Cumulative 数据使用旧传输底座。该底座暂留为显式历史复现入口：

```text
-roce_transport_semantics legacy
-roce_trim_recovery cumulative
```

此时 `FinalCcMrcConfig` 报告 `dctcp_variant_inflate=natural`。该入口不影响新的默认 Exact+Bounded。

## `conweave`

Source 端保留当前 `pathid`，通过 ACK RTT 触发重路由。收到 ACK 后如果 RTT 超过阈值，并且距离上次 reroute 已超过最小间隔，就随机选择一个不同于当前 path 的新 `pathid`。它不做逐包 spraying，而是在 RTT 异常时切换 flow 使用的路径。

## `adaptive-routing`

交换机侧逐跳选路。每次需要从 ECMP next-hop 集合中选端口时，交换机比较候选端口的本地状态，默认用队列拥塞比较函数选最空的一组，并在并列候选中随机挑一个。默认实验使用 packet 粒度；切到 flowlet 粒度时，同一 flowlet 会保持上次端口，只有间隔超过 sticky delta 且新端口更优时才更新。

## `drill`

交换机侧逐跳低状态采样。每次转发时随机抽两个 next-hop，再加上该 destination 上一次记住的候选端口；交换机比较这些候选的队列拥塞，选择最优端口，并把该端口写回 per-destination memory，作为下一次采样的历史候选。

## `sglb`

交换机侧逐跳评分选路。默认将本端出口和下游出口的归一化队列压力按下式合成，分数越低越好：

```text
pressure(q) = clamp((q - 0.20) / 0.60, 0, 1)
score = 1 - (1 - local_pressure) * (1 - remote_pressure)
```

默认按 `0.10/0.40/0.60` 量化为四档。八档是嵌套细化，边界为 `0.05/0.10/0.25/0.40/0.50/0.60/0.80`，由 `-sglb_nmrc_levels 8` 显式启用。原本地/下游 queue、util、busy 五因子评分由 `-sglb_score_mode legacy` 显式启用。

默认 local quality cache 每 1us 更新，GCN export cache 每 5us 更新，超过 30us 未更新的 GCN snapshot 会 aging 失效。当前实现直接读取邻居交换机对象里的周期缓存，不模拟 GCN 控制报文的传播和资源开销。

转发候选仍沿用 SGLB top-K：先加入最低 quality 的完整等级；候选少于默认 `K=3` 时继续整档加入下一等级。同等级不按瞬时 raw queue 排名，也不截断；候选满足 K 后随机喷洒。SGLB 还维护 neighbor availability，链路不可用时对应 next-hop 会被硬排除。此前的 best-level-only 和 within-grade ranked 实验模式均已删除。
