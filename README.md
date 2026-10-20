# V7G：PFC 扩散与预 Trimming 仿真

基于 htsim，研究接收端消费受限引发的 PFC 扩散，以及预 Trimming 对受害通信、
受阻通信和可靠恢复的影响。不以零 PFC 或所有业务均获益作为预设结论。

## 当前实现

- 网络：128 NIC、8 Leaf、8 Spine，400 Gbit/s；传播 500 ns、交换流水线 200 ns。
- 交换机：20 MiB Buffer、动态 PFC、ECN 100/400 KiB（Pmax=20%），有限 Header 控制通道。
- 端侧：真实 NIC RX/消费/PFC、live pacing、ACK/SACK、NACK、选择性重传和 RTO。
- 传输：Unordered（16K PSN 追踪、64-bit SACK），保留 Limited OOO 对照。
- 路由：ECMP、随机逐包 OPS，以及 MP-RDMA/REPS 适配；Hybrid 按 QP 分派 A2A 与 ECMP 背景流。
- 预裁剪：Global/Selective 固定 egress 水线与 Selective Ingress-risk。

MP-RDMA/REPS 为当前仿真适配，不宣称完整复现官方实现；GLB 仅有行为近似，
完整交换机 GLB、SGLB/SyncMesh 和 NSCC 未接入。

## 构建与运行

需要 Python 3、make 和支持 C++11 的编译器。在仓库根目录执行：

```bash
python3 experiments/pfc_pretrim_v7g/current/build.py --jobs 2
python3 -m unittest discover -s experiments/pfc_pretrim_v7g/current/tests -p 'test_*.py'

# 默认仅生成流量和命令；确认后增加 --execute 执行单点。
python3 experiments/pfc_pretrim_v7g/current/run_regular.py \
  --workload B3 --lb ops --policy IR --seed 41 \
  --output experiments/pfc_pretrim_v7g/current/runs/B3-ops-IR-s41
```

常规负载包括 Hybrid A2A + 长短流（B3）、单受阻 NIC（W1）、A2A（W2）
和多受阻 NIC（W3）。结果目录保留命令、构建哈希、实际配置、逐流 FCT 和事件日志。

## 仿真验证流程

1. 核对输入流量、实际路径和配置，先通过唯一交付、缓存守恒及最终排空检查。
2. 验证因果链：消费下降 → NIC PFC → Leaf 积压 → Fabric PFC → Victim 实际等待。
3. 同 LB、同 seed 配对 NoFault/B0、Fault/B0 和 Fault/Trimming，其他参数保持一致。
4. 比较两组完整 A2A JCT、Victim/Blocked 长短流 FCT，同时统计 PAUSE、裁剪、重传和 RTO。
5. 保留未完成与负收益样本；先计算每 seed 指标，再做配对统计，不混用不同构建结果。

减少 PFC 不等于改善应用性能；没有真实扩散或受害证据时，不认定为隔离成功。

## htsim 上游背景

htsim is a high performance discrete event simulator, inspired by ns2, but much faster, primarily intended to examine congestion control algorithm behaviour.  It was originally written by [Mark Handley](http://www0.cs.ucl.ac.uk/staff/M.Handley/) to allow [Damon Wishik](https://www.cl.cam.ac.uk/~djw1005/) to examine TCP stability issues when large numbers of flows are multiplexed.  It was extended by [Costin Raiciu](http://nets.cs.pub.ro/~costin/) to examine [Multipath TCP performance](http://nets.cs.pub.ro/~costin/files/mptcp-nsdi.pdf) during the MPTCP standardization process, and models of datacentre networks were added to [examine multipath transport](http://nets.cs.pub.ro/~costin/files/mptcp_dc_sigcomm.pdf) in a variety of datacentre topologies.  [NDP](http://nets.cs.pub.ro/~costin/files/ndp.pdf) was developed using htsim, and simple models of DCTCP, DCQCN were added for comparison.  Later htsim was adopted by Correct Networks (now part of Broadcom) to develop [EQDS](http://nets.cs.pub.ro/~costin/files/eqds.pdf), and switch models were improved to allow a variety of forwarding methods.  Support for a simple RoCE model, PFC, Swift and HPCC were added.

## Getting Started

There are some limited instructions in the [wiki](https://github.com/Broadcom/csg-htsim/wiki).  

htsim is written in C++, and has no dependencies.  It should compile and run with g++ or clang on MacOS or Linux.  To compile htsim, cd into the sim directory and run make.

To get started with running experiments, take a look in the experiments directory where there are some examples.  These examples generally require bash, python3 and gnuplot.
