# n-MRC on htsim

本仓库在 Broadcom `csg-htsim` 的 RoCE datacenter 仿真器基础上，评估 n-MRC 相关的多路径负载均衡方案。

原 full-snapshot `n-mrc` 已改名为 `netaware`：leaf 汇总源、目的两段链路的拥塞情况，把完整路径状态反馈给网卡。新的 `n-mrc` 让端侧轮询路径；当第一跳 Leaf 发现当前路径明显较差时，可以把当前包改送到更好的路径，并通知端侧暂时避开原路径。普通 ECN 仍负责拥塞控制。

独立 `mrc` 模式的 RoCE/队列默认底座、encoded EV 状态机以及 `-lb mrc` 到 MRC 论文机制的映射，见 `MRC_IMPLEMENTATION.md`。MRC 参考文献的整理稿见 `docs/papers/MRC.md`。各负载均衡方案、NetAware full snapshot 和新 n-MRC FastCNP 路径通知的实际逻辑，见 `experiments/n-mrc/README.md`。

## 仓库结构

- `sim/`：htsim C++ 离散事件仿真器。
- `sim/datacenter/htsim_roce`：编译后的 RoCE 仿真程序。
- `experiments/n-mrc/run_final_512_comparison.py`：256 节点多场景主实验脚本。文件名保留了开发阶段的 512-node resource gate 命名；正式运行时通过 `--nodes 256` 固定实验规模。
- `experiments/n-mrc/feedback_eval_common.py`：主实验使用的拓扑参数和 traffic matrix 生成函数。
- `experiments/n-mrc/experiment_metrics.py`：主实验使用的仿真日志和诊断指标解析函数。

## avail 机制简介

avail 把一个 EV 看作一条完整路径。源 ToR 根据 ECN 和 TRIM 判断哪些路径最近发生过拥塞，再把坏路径列表反馈给同一对 ToR 之间的所有流。端侧逐包换路时跳过这些路径；下一轮反馈会重新判断，避免路径被永久排除。如果所有路径都被标为坏，端侧仍会继续发送，防止连接卡死。

## grade 方案设计

grade 不直接把路径分成“可用”和“不可用”，而是根据正常 ACK、ECN 和 TRIM 给每条路径维护一个分数，再分为 GOOD、DEGRADED、BAD 和 AVOID 四档。端侧把更多流量发到状态好的路径，同时保留少量探测，让已经恢复的坏路径有机会重新加入。

## netaware 方案设计

netaware 由 source leaf 维护完整的路径视图。它同时观察本地上行和 spine 到目的 leaf 的下游队列，把两段链路合并成每条端到端路径的状态，再反馈给网卡。端侧根据 GOOD、DEGRADED、BAD 和 AVOID 四档状态加权分流；当好路径很少时，GoodCap 会限制流量过度集中，避免刚变好的路径马上形成新热点。

## n-MRC 方案设计

n-MRC 让端侧按确定性顺序逐包轮询 EV。第一跳 Leaf 会检查当前出口和其他可用出口；只有至少三条路径严格优于当前路径时，才把当前包改送到更好的路径。换路成功后，Leaf 通过 FastCNP 通知端侧暂时停用原 EV。普通 ECN 仍按原来的端到端方式工作，换路通知不直接降低 DCQCN 窗口。

## 后续尝试方向

- 分离换路和端侧冷却：路径稍差时只为当前包换路；差距足够大时才通知端侧冷却原 EV，减少轻微波动造成的大量路径停用。
- 使用分段相对阈值：健康区域使用较大的换路门槛，减少无效切换；拥塞区域使用较小的门槛，更快绕开明显变差的路径。

## 已实现负载均衡方案

| 方案 | 机制简述 |
| --- | --- |
| `ecmp` | 每条 flow 使用固定 pathid，交换机用 ecmp hash 映射到下一跳。 |
| `ecmp_rr` | 每个交换机在当前 ecmp next-hop 集合内做 round-robin；多 tier / bundle 场景下，每一跳分别轮转本跳可选端口。 |
| `ops` | 源端每个 packet 随机选择 EV/pathid，实现无反馈 packet spraying。 |
| `rr` | 源端按确定性序列轮转 EV/pathid，实现无反馈但覆盖更均匀的 source-controlled rr；随机 EV spraying 对应 `ops`。 |
| `reps` | ACK 回传近期未 ECN 的 pathid，源端优先复用 clean EV；没有反馈时随机。 |
| `avail` | 源 ToR 根据 ECN 和 TRIM 标记最近拥塞的路径，端侧逐包选路时暂时避开它们。路径状态会重新评估，不会永久拉黑。 |
| `grade` | 源 ToR 给路径维护四档状态。端侧多用好路径、少用差路径，并保留少量探测来发现路径恢复。 |
| `netaware` | source leaf 同时查看本地上行和 spine 下游的队列，为每条端到端路径生成四档状态并反馈给网卡。网卡按状态加权分流；GoodCap 避免少数好路径瞬间吸收过多流量。 |
| `n-mrc` | 端侧逐包轮询路径。第一跳 Leaf 发现至少三条路径严格优于当前路径时，为当前包选择更好的出口，并通知端侧暂时停用原路径。全部路径都在冷却时仍选择最早恢复的路径发送。 |
| `n-mrc-fixed0.5` | 原实验方案 `n-mrc4` 的正式名称。默认要求原路径分数不低于 `0.5`、候选路径低于 `0.5`，且质量差至少为 `0.25`，满足时将当前包换路和 FastCNP cooldown 通知作为配对动作。 |
| `n-mrc-delta` | 不设置绝对拥塞门槛，只在候选路径比原路径至少好 `delta=0.25` 时，将当前包换路并配对发送 FastCNP cooldown 通知。 |
| `mprdma` | MP-RDMA 多路径传输：数据包使用 VP/path-id，ACK 回显 VP，发送端按 ACK 时钟选择路径，并对超过乱序窗口的慢 VP 暂停发送；拥塞控制仍由独立的 `-cc dctcp_variant` 负责。 |
| `mrc` | 每个 QP 固定维护 64 个全部 active 的 EV，EV `i` 与物理 path-id `i` 一一映射并按 per-QP 确定性顺序轮转。ECN 与 TRIM 对精确 EV 执行 OCP SKIP_ONCE：该 EV 只失去下一次名义发送机会，SKIP 期间重复反馈不 rearm；一次发送最多恢复一个 SKIP EV。当前仅验证拥塞性能，不开放故障恢复 CLI。 |
| `conweave` | RTT 超过阈值后切换 pathid，减少持续走拥塞路径的概率。 |
| `adaptive-routing` | 交换机按本地队列拥塞情况在可用下一跳中选择端口。 |
| `conga` | 源端按论文默认 500us flowlet 粘滞选择路径；交换机以 32us 周期、alpha=0.2 维护逐出口 DRE，按 3 bit 量化并在路径上取最大值，ACK 将该路径估计反馈给源端。短流虽然使用 ECMP，其链路字节仍进入 DRE。 |
| `drill` | 交换机结合随机候选和历史候选端口，优先选择拥塞较低的下一跳。 |
| `sglb` | 默认使用 real-GCN、原始队列占用率线性平均、四档分级、整档扩展至至少 20 个候选，再随机逐包选择。1us 为质量缓存刷新间隔，15us 为 GCN 发送间隔限制，30us 为远端快照老化阈值；这些参数不代表无流量时自动周期刷新，详见下文。 |

从粒度上看，`ops`、`rr`、`reps`、`avail`、`grade`、`n-mrc` 和 `mrc` 都是源端逐包选择 EV/pathid；`ecmp` 使用固定 pathid，`conweave` 只在 RTT 触发时切换 pathid，不做逐包 spraying。`ecmp_rr`、`drill`、`sglb` 和当前 `adaptive-routing` 默认都属于交换机侧逐包/逐跳选择 next-hop；`adaptive-routing` 仍可通过 `-ar_granularity flowlet` 切到 flowlet sticky。

### SGLB 当前代码语义

连续分数消融版：在 `-lb sglb` 后指定 `-sglb_topk K`，直接按连续分数升序保留 K 条可用候选，同分使用随机键，候选内随机逐包选择。该入口忽略分级候选策略和 `min_choices`；原有等级仅保留用于 GCN 通知和诊断，不参与候选排序。默认分级配置为只保留最佳等级，并在其中随机选 1 条，符合论文中的质量分级语义。评分、缓存刷新和 GCN 机制保持相同。

以下依据 `main_roce.cpp` 的初始化调用及 `fat_tree_switch.cpp` 的 `configure_sglb_scheme_defaults(false)`、`sglb_quality_snapshot()`、`sglb_observe_real_gcn_export()` 和 `sglb_route()`，描述未显式覆盖参数的 `-lb sglb`。

- 默认因子为 `REAL_GCN_RAW_LINEAR`，要求两层 Clos。分数为 `0.5 * local_queue_fraction + 0.5 * remote_queue_fraction`，使用原始队列占用率；下游导出取通向目的地的候选队列最小占用率。
- 四档分数区间依次为 GOOD `[0, 0.05)`、DEGRADED `[0.05, 0.10)`、BAD `[0.10, 0.20)`、AVOID `[0.20, 1]`。AVOID 是质量等级，不是故障硬屏蔽。
- 默认 `whole_grade_min`、`min_choices=20`：从最好等级开始逐档加入整个等级，累计至少 20 条后停止。若可用路径不足 20 条则全部纳入；同档不会截断，因此候选数可以大于 20。最终在候选集中随机选择，默认不是严格 Top-K。
- 初始化预装所有目的 Leaf 的导出/接收 profile，并构建源 Leaf 的候选质量缓存。初始化不等于启动持续刷新定时器。
- 本地质量缓存由访问触发：距离上次计算不足 1us 时复用，否则当次访问重算。当前没有独立的 1us 质量表扫描定时器；GCN 接收只更新远端 profile，不立即重算本地质量缓存。
- 默认 real-GCN 分支由经过交换机的数据包调用导出观察逻辑；观察间隔受 1us 限制。观察到变化后发送 GCN，相邻发送受 15us 限制；不足间隔时安排一次延迟发送。延迟发送使用已观察的状态，不重新采样；无 dirty 状态时不发送。因此 15us 不是无条件周期广播，也不是固定传播延迟。实际控制包还经过队列和链路。
- 远端快照超过 30us 后读取时视为不可用；默认评分中的缺失远端项取零。旧分支的 `sglb_schedule_periodic_gcn()` / `sglb_periodic_refresh_exports()` 不应当作默认 real-GCN 分支持续刷新的证据。

历史核对：提交 `eecaccb` 将默认候选策略设为整档最少 20 条、随机派发和 independent GCN cadence。工作区历史计划 `docs/superpowers/plans/2026-06-18-sglb-async-quality-table.md` 记录了 1/15/30us 参数，但其测试通过时间推进后再次调用选路来验证缓存刷新，不能证明存在无流量的本地周期扫描。

## 当前 RoCE 传输、队列和 TRIM 处理

主实验中的所有方案使用同一套 RoCE 传输配置，负载均衡方案只决定路径怎么选，不单独改变队列、拥塞窗口或重传规则。

`dctcp_variant` 为每个 QP 维护一个以数据包为单位的拥塞窗口。窗口初始值按当前拓扑的 1 BDP 计算，最小为 1 个包，不设额外的固定上限。没有拥塞标记的新 ACK 按标准加性增长逐步扩大窗口；ECN ACK 将窗口减少 0.5 个包；确认一次有效丢包的 OOO、TRIM、LOSS NACK 或 RTO 将窗口减少 1 个包。发送端实际可用额度是 `cwnd - inflight`，只有一个 PSN 第一次被 ACK 或 SACK 确认时才释放额度，因此重复 ACK 不会额外扩大可发送数据量。

Fabric 使用 `composite_ecn_lb` 队列，默认容量同样为 1 BDP。完整数据包进入低优先级队列，ACK、NACK 和被 trim 后只剩包头的数据包进入高优先级队列，发送时优先处理这些头部包。除 ToR 到主机的下行外，fabric 队列从容量的 20% 开始按占用率概率标记 ECN，到 80% 时全部标记。主机发送侧使用 `prio` 队列，控制包优先于普通数据包。

低优先级队列装满后不直接丢弃整包，而是把到达包或队列中的一个数据包去掉 payload，只保留携带路径、PSN 和发送轮次信息的包头。接收端收到这个包头后返回 TRIM NACK，并明确指出缺失的 PSN。发送端只重传这个 PSN，不做累计回退；64-bit SACK 用来确认同一窗口中已经正确到达的其他包。

TRIM 同时作用于传输恢复和负载均衡。对传输层来说，它触发精确重传，并在这次失败首次被接受时收紧 QP 拥塞窗口；重复或过期的 TRIM 不会重复扣减窗口。对 `avail`、`grade`、`mrc` 和 `n-mrc` 来说，TRIM 还会更新对应路径的状态，但路径处罚与 QP 拥塞窗口各自维护。即使窗口已经没有普通发送额度，每个 QP 仍允许最多 1 MTU 的恢复包发出，避免重传被窗口本身卡住。

这套行为对应 `sp` 接收、64-bit SACK、`mrc_exact_bounded` 传输和 exact-PSN TRIM recovery。此前的 Natural+Cumulative 行为只作为历史结果复现入口保留：

```bash
-roce_transport_semantics legacy -roce_trim_recovery cumulative
```

该入口使用旧的累计 TRIM 恢复和 Natural inflate，不能与当前的 Exact+Bounded 模式混用。

## 标准仿真拓扑

所有二层主实验统一使用固定 64-Spine 的全带宽 Leaf–Spine：每台 Leaf 有
64 个 400Gbps 主机下联和 64 个 400Gbps Spine 上联，并分别连接 64 台主机
和全部 64 台 Spine。网络规模只通过 Leaf 数量变化：

```text
spines         = 64
hosts_per_leaf = 64
leaves         = nodes / 64
physical_paths = 64
```

默认规模为 256 节点。自动生成的二层拓扑只接受
256/512/1024/2048/4096/8192 节点，分别对应 4/8/16/32/64/128 台 Leaf；
Spine 数和物理路径数始终为 64。其他规模会直接报错，不再回退到旧拓扑。
`-paths` 小于 64 只允许用于明确标注的 EV/候选集消融，不能解释为物理拓扑
路径数。显式拓扑配置文件仍可定义自定义结构。

## 256 节点主实验

主实验固定比较 `ops`、`reps`、`mrc`、`sglb` 和 `n-mrc`，使用 seeds `13,29,47`。健康 P2P 额外运行 `ecmp`，用于计算相对 ECMP 的加速比。

| 场景族 | Traffic / message size | 说明 |
| --- | --- | --- |
| 健康 P2P | permutation、tornado，4/8/16 MiB；WebSearch 40/60/80 | 全链路等速。 |
| 非对称 P2P | permutation、tornado，4/8/16 MiB；WebSearch 40/60/80 | 随机稀疏选择约 3% 的 ToR 上行并将带宽减半。 |
| 健康 All-to-All | 64/256/1024 MiB，P=4/8/16 | 使用 CCT 作为主指标。 |
| 非对称 All-to-All | 64/256/1024 MiB，P=4/8/16 | 慢 ToR 上行叠加周期背景流。 |
| Mixed deployment | permutation、tornado，4/8/16 MiB | 同批连接同时启动；每第 10 个连接固定使用 ECMP 并标记 `bg=1`，其余连接使用被测方案并标记 `bg=0`。两类流量共享链路和队列，分别统计最大 FCT。 |

完整三 seed 矩阵共有 690 个实验单元。P2P 报告 FCT，All-to-All 报告 CCT，mixed deployment 分别保留 `bg=0` 和 `bg=1` 指标。

主实验通用参数：

- 链路速率：400Gbps。
- 链路延迟：每跳 wire latency 0.5us，每台 switch latency 0.5us；2-tier 为 7us，3-tier 为 11us。
- 队列：`composite_ecn_lb`，数据包可被 trim，ACK/NACK 等头部包使用高优先级队列。
- ECN：composite RED 的 Kmax 为队列容量的 `0.8`；NetAware 队列压力按 `0.2–0.8` 归一化，当前默认 SGLB 使用原始队列占用率。
- Host queue：`prio`，data 低优先级，ACK/NACK 高优先级。
- MTU：4096 bytes。
- 接收与恢复：`sp`、64-bit SACK、`mrc_exact_bounded`、exact-PSN TRIM recovery。
- 拥塞控制：`dctcp_variant`。
- 流量：`tornado` 或 `permutation`，每个 host 一个 foreground flow；`tornado` 目的端为 `(src + nodes/2) % nodes`，`permutation` 使用固定 seed 的随机一一映射。
- 图表指标：avg FCT、p99 FCT、p99.9 FCT。

非对称场景使用 `ceil(0.03 * leaves * spines)` 条慢 ToR 上行，带宽除以 2，并通过 `random-sparse` 按 seed 分散选择。

## 环境准备

克隆当前实验分支：

```bash
git clone -b main-htsim https://github.com/Programmable-sw/lb-sim.git csg-htsim
cd csg-htsim
git remote add upstream https://github.com/Broadcom/csg-htsim.git || true
git fetch --all --prune
```

Ubuntu/Debian 依赖：

```bash
sudo apt-get update
sudo apt-get install -y build-essential make g++ git python3 python3-pip
```

构建仿真器：

```bash
make -C sim -j"$(nproc)"
```

检查 RoCE 仿真程序和脚本：

```bash
test -x sim/datacenter/htsim_roce
python3 -m py_compile \
  experiments/n-mrc/run_final_512_comparison.py \
  experiments/n-mrc/feedback_eval_common.py \
  experiments/n-mrc/experiment_metrics.py
```

## 运行实验

先用 dry-run 检查 256 节点、三 seed 的 690 条命令。dry-run 不启动仿真：

```bash
python3 experiments/n-mrc/run_final_512_comparison.py \
  --nodes 256 \
  --seeds 13,29,47 \
  --dry-run \
  --out experiments/n-mrc/output/n-mrc-results
```

运行完整实验：

```bash
python3 experiments/n-mrc/run_final_512_comparison.py \
  --nodes 256 \
  --seeds 13,29,47 \
  --workers 4 \
  --timeout 3600 \
  --out experiments/n-mrc/output/n-mrc-results
```

脚本会校验完整结果并复用指纹一致的已完成单元。如需重跑已有单元，加入 `--force`：

```bash
python3 experiments/n-mrc/run_final_512_comparison.py \
  --nodes 256 \
  --seeds 13,29,47 \
  --workers 4 \
  --timeout 3600 \
  --force \
  --out experiments/n-mrc/output/n-mrc-results
```

## 输出文件

`experiments/n-mrc/output/n-mrc-results/` 包含：

- `manifest.json`：实验规模、方案、seed 和单元数量。
- `traffic/`：每个场景和 seed 共用的 traffic matrix。
- `raw/`：每个实验单元的命令、stdout 和解析结果。
- `results.csv`：逐 seed 的完整结果。
- `summary.csv`：三 seed 聚合结果。
- `report.md`：自动生成的汇总报告。
- `COMPLETE`：完整矩阵通过校验后的完成标记。

整个 `experiments/n-mrc/output/` 目录由 `.gitignore` 排除，不上传到 Git。

## 核心代码

- `experiments/n-mrc/run_final_512_comparison.py`：256 节点主场景矩阵、命令拼接、断点复用和结果校验。
- `experiments/n-mrc/feedback_eval_common.py`：拓扑参数及 permutation、tornado、WebSearch、All-to-All traffic 生成。
- `experiments/n-mrc/experiment_metrics.py`：RoCE、队列和 n-MRC 诊断指标解析。
- `sim/datacenter/main_roce.cpp`：RoCE CLI 参数、LB 模式选择、EV 空间按拓扑自动校准。
- `sim/roce.cpp` / `sim/roce.h`：源端 `ecmp`、`ops`、`reps`、`avail`/`grade`（内部复用 STOR）、`mrc` 选路状态机和 ACK/NACK feedback 更新。
- `sim/rocepacket.h`：RoCE packet/ACK/NACK 上携带 pathid、Grade/Avail 的 STOR feedback 和独立 n-MRC snapshot。
- `sim/datacenter/fat_tree_switch.cpp` / `sim/datacenter/fat_tree_switch.h`：交换机 ecmp / adaptive-routing / drill / sglb 转发、source-controlled pathid 分段映射、Avail/Grade 的 source-ToR feedback 生成。
- `sim/datacenter/fat_tree_topology.cpp` / `sim/datacenter/fat_tree_topology.h`：generated fat-tree 拓扑、bundle/radix 参数、慢链路注入。
- `MRC_IMPLEMENTATION.md`：独立 `mrc` 模式的实现细节、默认 CC/队列/参数、已实现机制和当前近似范围。

## 清理

```bash
gio trash experiments/n-mrc/output/n-mrc-results
make -C sim clean
```

## htsim 背景

htsim 是一个高性能离散事件仿真器，设计目标是快速研究拥塞控制算法行为。本 fork 保留 htsim/RoCE 仿真器结构，并在其上加入 n-MRC 相关实验。

### CC 命名与 DCQCN 审阅（2026-09-30）

`-cc dctcp_variant` 是逐 ACK 窗口控制：clean ACK 加 1/cwnd，ECN ACK 减 0.5 包；并非 DCQCN。旧 CLI 名称 `dcqcn_variant` 和 `dcqcn-variant` 仅保留为兼容别名，输出统一使用新名称。历史实验输出不改写。

`-cc dcqcn` 当前是简化速率模型，不能作为严格论文复现：ECN ACK 在发送端被当作 CNP，缺少接收端独立 CNP 生成/抑制；alpha 和升速计时仅在 ACK 处理时执行；字节阈值按确认量计数；恢复阶段使用单一计数器且缺少 hyper-increase。默认 NACK-as-CNP 也是扩展。

当前参数：g=1/256，initial alpha=0.6，AI=4 Gbps，最低速率=80 Gbps，alpha/升速间隔=28 us，CNP 抑制间隔=16.8 us，字节阈值=1,400,000 B，快速恢复=5 次。这些是现有配置，不代表原文推荐；80 Gbps 下限尤其会限制高并发拥塞退让。另有独立窗口上限，需结合有效传输模式审阅。

参考：DCQCN SIGCOMM 2015 原文 §3、§5，https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/lossless.pdf 。本次仅统一命名并审阅，不修改 DCQCN 行为或参数。
