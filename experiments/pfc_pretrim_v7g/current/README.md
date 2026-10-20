# V7G 当前可提交版本（不含特殊性实验）

完整验证顺序、配对口径和输出检查见 [仿真验证流程](../README.md)。本页说明源码模块与运行入口。

这是当前随机OPS/Common-NACK版本的独立源码与常规实验入口，不是新算法。
从 `v7g-ops-random-entropy-v1/source_snapshot` 按字节复制共享内核，
原归档二进制 SHA256 为
`e3c001ae86c97b6d3e703424ee48d3eb125ba6fd39860f31d9840eaa0af444e3`。
`SOURCE_IDENTITY.json`记录逐文件哈希，旧源码、二进制及结果不覆盖。

## 已实现范围

|层次|实际实现|
|---|---|
|LB|ECMP固定per-flow；OPS每包随机16-bit entropy经FreeBSD哈希；MP-RDMA的awnd/ACK-credit/剪枝与恢复适配；REPS缓存/探索/freezing适配|
|GLB|仅行为近似，保留代码，不作为正式GLB实现或排名|
|未实现|完整交换机GLB、SGLB/SyncMesh；当前未接入官方NSCC|
|CC|Hybrid A2A为Common-NACK DCTCP变体；背景ECMP/DCQCN。Pure模式保留自身发送控制，不宣称跨LB统一全部协议|
|反馈|真实反向ACK/64-bit SACK/NACK/CNP；普通有效NACK降窗，PRETRIM免直接loss降窗；attempt去重和RTO兜底|
|交换机|真实端口队列/serializer/400G链路、ECN、共享MMU/reserve/headroom、动态PFC、Global/Selective/Ingress-risk裁剪|
|NIC|live pacing，新发/重传共享发送预算；512KiB RX硬容量、重复payload驻留去重、host消费、真实RX XOFF/XON；有限控制服务|
|接收端|Limited OOO的64/128窗口；Unordered的16K PSN追踪/64-bit SACK；全局4096项metadata，不是每NIC4096项|

官方REPS参考版本为 `b751588745b57e8264ce22d9a40df67bdedd62c0`。
当前MP-RDMA/REPS仍为V7G适配，尚未原样对齐该artifact。NSCC源码存在于
上游，但本提交不把“参考仓库有代码”写成“当前数据路径已接入”。

## 模块位置

- `src/v7g_hybrid_b.cpp`：实际事件入口、交换机admission/MMU/PFC、NIC serializer与控制反馈接线。
- `src/v7g_unordered_protocols.h`：MP/REPS/DCQCN状态、资源与风险预算。
- `src/unordered_transport.h`、`unordered_events.inc`：乱序/SACK、接收容量、选择性恢复与共享QP定时。
- `src/common_nack_cc.h`：公共A2A CC与PRETRIM语义。
- `src/ops_random_entropy.h`：独立可复现的随机entropy与ECMP哈希。
- `src/hybrid*_dispatch.h`：按QP分派A2A与背景的LB/CC。
- `src/light_hybrid_observer.h`：轻量实际路径、PAUSE交集、资源与每QP指标。

共享内核中的IR_OR_S400开关及被动诊断字段按原样保留，避免改动已经验证的
网络语义；**常规runner不暴露该特殊方案，不包含C1/C2工作负载或扫描脚本**。

## 固定常规网络

128 NIC / 8 Leaf / 8 Spine，每Leaf16 NIC，400G，传播500ns、流水线200ns。
每交换机20MiB；ECN100/400KiB、Pmax20%；XOFF=8192+shared_free/8，
XOFF-XON滞回32KiB。RX512KiB、全局metadata4096；1个控制服务器、
等待上限4096包、服务7.8125ns/包。额外trim-induced CE关闭。
完整包payload/memory/wire分别4096/4192/4230B；escape header实际控制池
记账294B，不能把结构体的header.memory=256B等同于实际控制池扣账。

## 常规负载

|负载|配置|
|---|---|
|B3|658QP，235.01953125MiB，117活跃NIC；A2A-A18成员/306QP，A2A-B16成员/240QP；48长流、64四轮突发短流|
|W1|单受阻NIC112；32条4MiB长流，R-short/Victim-short/Control-short共128条短流|
|W2|16成员A2A，每QP512KiB，附同Leaf Victim短流|
|W3-2/W3-4|同Leaf2/4个受阻NIC，160QP；R-short按已验证W3映射分配到各受阻NIC|

B3长流20us开始、2MiB/25G；A2A100us开始、256KiB/100G；短流在
100/160/220/280us启动，每轮4R/8H/4O，固定4–128KiB序列。
故障NIC在50–250us消费0G，其他时间400G。NoFault只取消消费扰动。
B3两组A2A使用被测OPS/MP-RDMA/REPS，全部背景固定ECMP；Pure W负载各LB
控制全部流，不能称为“Victim固定ECMP”的Hybrid实验。同Leaf流在模型中也经过Spine。

## 构建与运行

无需历史 `results/`、作者仓库或预生成`.a`。复用本仓库已跟踪的htsim核心源码。

```bash
python3 experiments/pfc_pretrim_v7g/current/build.py --jobs 2
python3 -m unittest discover -s experiments/pfc_pretrim_v7g/current/tests -p 'test_*.py'

# 默认只生成流量和完整命令，不自动运行矩阵。
python3 experiments/pfc_pretrim_v7g/current/run_regular.py \
  --workload B3 --lb ops --policy IR --seed 41 \
  --output experiments/pfc_pretrim_v7g/current/runs/B3-ops-IR-s41

# 确认后，在同一命令末尾增加 --execute 执行一个点。
# 长任务可用 systemd-run --user 包装该命令，非依赖工具会话的后台子进程。
```

Limited OOO示例：`--workload W3-2 --lb mprdma --transport limited-ooo
--rx-window 64 --sender-cap 128`。Unordered最终解析为16384追踪范围，
不是把CLI的`--rx-window 128`当成其实际窗口。0表示关闭额外通用sender cap。
T32/T128表示ingress Gamma32768/131072B；IR默认Gamma514352B，不是固定egress阈值。

常规脚本的流量已与冻结CSV逐字段核对。新入口是当前内核的可复现运行入口，
不自动重跑或改写历史W1/W2/W3正式数据；历史统计版本和构建身份保持独立。
本目录不含二进制、原始trace、旧结果、可视化数据、上游仓库副本或特殊场景脚本。
