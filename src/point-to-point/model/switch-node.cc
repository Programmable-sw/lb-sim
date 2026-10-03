#include "switch-node.h"

#include "assert.h"
#include "ns3/boolean.h"
#include "ns3/conweave-routing.h"
#include "ns3/double.h"
#include "ns3/flow-id-tag.h"
#include "ns3/int-header.h"
#include "ns3/ipv4-header.h"
#include "ns3/ipv4.h"
#include "ns3/letflow-routing.h"
#include "ns3/packet.h"
#include "ns3/pause-header.h"
#include "ns3/settings.h"
#include "ns3/uinteger.h"
#include "ppp-header.h"
#include "qbb-header.h"
#include "qbb-net-device.h"
#include "cn-header.h"

namespace ns3 {

TypeId SwitchNode::GetTypeId(void) {
    static TypeId tid =
        TypeId("ns3::SwitchNode")
            .SetParent<Node>()
            .AddConstructor<SwitchNode>()
            .AddAttribute("EcnEnabled", "Enable ECN marking.", BooleanValue(false),
                          MakeBooleanAccessor(&SwitchNode::m_ecnEnabled), MakeBooleanChecker())
            .AddAttribute("CcMode", "CC mode.", UintegerValue(0),
                          MakeUintegerAccessor(&SwitchNode::m_ccMode),
                          MakeUintegerChecker<uint32_t>())
            .AddAttribute("AckHighPrio", "Set high priority for ACK/NACK or not", UintegerValue(0),
                          MakeUintegerAccessor(&SwitchNode::m_ackHighPrio),
                          MakeUintegerChecker<uint32_t>());
    return tid;
}

SwitchNode::SwitchNode() {
    m_ecmpSeed = m_id;
    m_isToR = false;
    m_node_type = 1;
    m_isToR = false;
    m_drill_candidate = 2;
    m_mmu = CreateObject<SwitchMmu>();
    m_glbRouting = CreateObject<GlbRouting>(); // 实例化 GLB Routing
    // 绑定回调供 GLB Routing 发送控制报文
    m_glbRouting->SetSwitchSendCallback(MakeCallback(&SwitchNode::DoSwitchSend, this));
    // ====== 新增：将 MMU 指针绑定给 GLB ======
    m_glbRouting->SetMmu(m_mmu);
    // ====== 新增：绑定获取 TxBytes 的回调 ======
    m_glbRouting->SetGetTxBytesCallback(MakeCallback(&SwitchNode::GetTxBytesOutDev, this));
    // ====== 新增绑定 ======
    m_glbRouting->SetGetPortPausedCallback(MakeCallback(&SwitchNode::IsPortPaused, this));
    // Conga's Callback for switch functions
    m_mmu->m_congaRouting.SetSwitchSendCallback(MakeCallback(&SwitchNode::DoSwitchSend, this));
    m_mmu->m_congaRouting.SetSwitchSendToDevCallback(
        MakeCallback(&SwitchNode::SendToDevContinue, this));
    // ConWeave's Callback for switch functions
    m_mmu->m_conweaveRouting.SetSwitchSendCallback(MakeCallback(&SwitchNode::DoSwitchSend, this));
    m_mmu->m_conweaveRouting.SetSwitchSendToDevCallback(
        MakeCallback(&SwitchNode::SendToDevContinue, this));

    for (uint32_t i = 0; i < pCnt; i++) {
        m_txBytes[i] = 0;
    }
}

/**
 * @brief Load Balancing
 */
uint32_t SwitchNode::DoLbFlowECMP(Ptr<const Packet> p, const CustomHeader &ch,
                                  const std::vector<int> &nexthops) {
    // pick one next hop based on hash
    union {
        uint8_t u8[4 + 4 + 2 + 2];
        uint32_t u32[3];
    } buf;
    buf.u32[0] = ch.sip;
    buf.u32[1] = ch.dip;
    if (ch.l3Prot == 0x6)
        buf.u32[2] = ch.tcp.sport | ((uint32_t)ch.tcp.dport << 16);
    else if (ch.l3Prot == 0x11)  // XXX RDMA traffic on UDP
        buf.u32[2] = ch.udp.sport | ((uint32_t)ch.udp.dport << 16);
    else if (ch.l3Prot == 0xFC || ch.l3Prot == 0xFD || ch.l3Prot == 0xFA)  // ACK or NACK or Bitmap FB
        buf.u32[2] = ch.ack.sport | ((uint32_t)ch.ack.dport << 16);
    else {
        std::cout << "[ERROR] Sw(" << m_id << ")," << PARSE_FIVE_TUPLE(ch)
                  << "Cannot support other protoocls than TCP/UDP (l3Prot:" << ch.l3Prot << ")"
                  << std::endl;
        assert(false && "Cannot support other protoocls than TCP/UDP");
    }

    uint32_t hashVal = EcmpHash(buf.u8, 12, m_ecmpSeed);
    uint32_t idx = hashVal % nexthops.size();
    return nexthops[idx];
}

/*-----------------CONGA-----------------*/
uint32_t SwitchNode::DoLbConga(Ptr<Packet> p, CustomHeader &ch, const std::vector<int> &nexthops) {
    return DoLbFlowECMP(p, ch, nexthops);  // flow ECMP (dummy)
}

/*-----------------Letflow-----------------*/
uint32_t SwitchNode::DoLbLetflow(Ptr<Packet> p, CustomHeader &ch,
                                 const std::vector<int> &nexthops) {
    if (m_isToR && nexthops.size() == 1) {
        if (m_isToR_hostIP.find(ch.sip) != m_isToR_hostIP.end() &&
            m_isToR_hostIP.find(ch.dip) != m_isToR_hostIP.end()) {
            return nexthops[0];  // intra-pod traffic
        }
    }

    /* ONLY called for inter-Pod traffic */
    uint32_t outPort = m_mmu->m_letflowRouting.RouteInput(p, ch);
    if (outPort == LETFLOW_NULL) {
        assert(nexthops.size() == 1);  // Receiver's TOR has only one interface to receiver-server
        outPort = nexthops[0];         // has only one option
    }
    assert(std::find(nexthops.begin(), nexthops.end(), outPort) !=
           nexthops.end());  // Result of Letflow cannot be found in nexthops
    return outPort;
}

/*-----------------DRILL-----------------*/
uint32_t SwitchNode::CalculateInterfaceLoad(uint32_t interface) {
    Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[interface]);
    NS_ASSERT_MSG(!!device && !!device->GetQueue(),
                  "Error of getting a egress queue for calculating interface load");
    return device->GetQueue()->GetNBytesTotal();  // also used in HPCC
}

uint32_t SwitchNode::DoLbDrill(Ptr<const Packet> p, const CustomHeader &ch,
                               const std::vector<int> &nexthops) {
    // find the Egress (output) link with the smallest local Egress Queue length
    uint32_t leastLoadInterface = 0;
    uint32_t leastLoad = std::numeric_limits<uint32_t>::max();
    auto rand_nexthops = nexthops;
    std::random_shuffle(rand_nexthops.begin(), rand_nexthops.end());

    std::map<uint32_t, uint32_t>::iterator itr = m_previousBestInterfaceMap.find(ch.dip);
    if (itr != m_previousBestInterfaceMap.end()) {
        leastLoadInterface = itr->second;
        leastLoad = CalculateInterfaceLoad(itr->second);
    }

    uint32_t sampleNum =
        m_drill_candidate < rand_nexthops.size() ? m_drill_candidate : rand_nexthops.size();
    for (uint32_t samplePort = 0; samplePort < sampleNum; samplePort++) {
        uint32_t sampleLoad = CalculateInterfaceLoad(rand_nexthops[samplePort]);
        if (sampleLoad < leastLoad) {
            leastLoad = sampleLoad;
            leastLoadInterface = rand_nexthops[samplePort];
        }
    }
    m_previousBestInterfaceMap[ch.dip] = leastLoadInterface;
    return leastLoadInterface;
}

/*------------------ConWeave Dummy ----------------*/
uint32_t SwitchNode::DoLbConWeave(Ptr<const Packet> p, const CustomHeader &ch,
                                  const std::vector<int> &nexthops) {
    return DoLbFlowECMP(p, ch, nexthops);  // flow ECMP (dummy)
}

/*------------------GLB-------------------*/
uint32_t SwitchNode::DoLbGlb(Ptr<Packet> p, CustomHeader &ch, const std::vector<int> &nexthops) {
    if (m_isToR && nexthops.size() == 1) {
        if (m_isToR_hostIP.find(ch.sip) != m_isToR_hostIP.end() &&
            m_isToR_hostIP.find(ch.dip) != m_isToR_hostIP.end()) {
            return nexthops[0];  // pod 内流量，直接返回
        }
    }

    uint32_t outPort = m_glbRouting->RouteInput(p, ch, nexthops);
    if (outPort == 0xFFFFFFFF) {
        // 控制包，内部消耗，返回特殊标识使其在 SendToDevContinue 或底层被丢弃
        return 0xFFFFFFFF;
    }
    return outPort;
}

/*------------------Reps----------------*/
uint32_t SwitchNode::DoLbReps(Ptr<const Packet> p, const CustomHeader &ch,
                                  const std::vector<int> &nexthops) {
    return DoLbFlowECMP(p, ch, nexthops);  // 无需修改交换机
}

/*------------------ToR Bitmap----------------*/
uint32_t SwitchNode::DoLbBitmap(Ptr<const Packet> p, const CustomHeader &ch, const std::vector<int> &nexthops) {
    if (m_isToR && nexthops.size() == 1) {
        if (m_isToR_hostIP.find(ch.sip) != m_isToR_hostIP.end() &&
            m_isToR_hostIP.find(ch.dip) != m_isToR_hostIP.end()) {
            return nexthops[0];
        }
    }
    // 读取端侧打上的下标，作为唯一熵值进行哈希
    BitmapSprayTag tag;
    if (p->PeekPacketTag(tag)) {
        union { uint8_t u8[4]; uint32_t u32; } buf;
        buf.u32 = tag.GetPathId();
        uint32_t hashVal = EcmpHash(buf.u8, 4, m_ecmpSeed);
        return nexthops[hashVal % nexthops.size()];
    }
    // 若无 BitmapSprayTag，降级为正常 ECMP
    return DoLbFlowECMP(p, ch, nexthops);
}
/*----------------------------------*/

void SwitchNode::CheckAndSendPfc(uint32_t inDev, uint32_t qIndex) {
    Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
    bool pClasses[qCnt] = {0};
    m_mmu->GetPauseClasses(inDev, qIndex, pClasses);
    for (int j = 0; j < qCnt; j++) {
        if (pClasses[j]) {
            uint32_t paused_time = device->SendPfc(j, 0);
            m_mmu->SetPause(inDev, j, paused_time);
            m_mmu->m_pause_remote[inDev][j] = true;
            /** PAUSE SEND COUNT ++ */
        }
    }

    for (int j = 0; j < qCnt; j++) {
        if (!m_mmu->m_pause_remote[inDev][j]) continue;

        if (m_mmu->GetResumeClasses(inDev, j)) {
            device->SendPfc(j, 1);
            m_mmu->SetResume(inDev, j);
            m_mmu->m_pause_remote[inDev][j] = false;
        }
    }
}
void SwitchNode::CheckAndSendResume(uint32_t inDev, uint32_t qIndex) {
    Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
    if (m_mmu->GetResumeClasses(inDev, qIndex)) {
        device->SendPfc(qIndex, 1);
        m_mmu->SetResume(inDev, qIndex);
    }
}

/********************************************
 *              MAIN LOGICS                 *
 *******************************************/

// This function can only be called in switch mode
bool SwitchNode::SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet,
                                         CustomHeader &ch) {
// 拦截 lb_mode=13 且当前为目的 ToR 的下行流量
    if (Settings::lb_mode == 13 && m_isToR) {
        if (m_isToR_hostIP.count(ch.dip) > 0 && m_isToR_hostIP.count(ch.sip) == 0) {
            BitmapSprayTag tag;
            if (packet->PeekPacketTag(tag)) {

                uint32_t source_key = ch.sip;
                auto src_tor = Settings::hostIp2SwitchId.find(ch.sip);
                if (src_tor != Settings::hostIp2SwitchId.end()) {
                    source_key = src_tor->second;
                }

                // ===== 1. 初始化 =====
                if (m_dtor_path_states.find(source_key) == m_dtor_path_states.end()) {
                    m_dtor_path_states[source_key].set();
                    m_dtor_total_pkt_cnt[source_key] = 0;
                    if (Settings::dtor_feedback_mode != 0) {
                        m_last_feedback_time[source_key] = Simulator::Now();
                    }
                }

                uint32_t path_index = tag.GetPathId() % 256;
                uint8_t ecn = ch.GetIpv4EcnBits(); 

                // ===== 2. 状态更新与周期计数 =====
                m_dtor_total_pkt_cnt[source_key]++;
                
                if (ecn != 0) {
                    m_dtor_path_states[source_key].reset(path_index);
                } else {
                    m_dtor_path_states[source_key].set(path_index);
                }

                // ===== 3. 反馈触发 =====
                bool should_feedback = false;
                if (Settings::dtor_feedback_mode == 0) {
                    const uint32_t EPOCH_THRESHOLD = 1000;
                    if (m_dtor_total_pkt_cnt[source_key] >= EPOCH_THRESHOLD) {
                        m_dtor_path_states[source_key].set();
                        m_dtor_total_pkt_cnt[source_key] = 0;
                    }
                    should_feedback = (Simulator::Now() - m_last_feedback_time[source_key] > MicroSeconds(20));
                } else {
                    Time elapsed = Simulator::Now() - m_last_feedback_time[source_key];
                    bool packet_trigger = m_dtor_total_pkt_cnt[source_key] >= Settings::dtor_feedback_pkts &&
                                          elapsed >= MicroSeconds(Settings::dtor_feedback_min_us);
                    bool time_trigger = m_dtor_total_pkt_cnt[source_key] > 0 &&
                                        elapsed >= MicroSeconds(Settings::dtor_feedback_max_us);
                    should_feedback = packet_trigger || time_trigger;
                }

                // ===== 4. 发送反馈 =====
                if (should_feedback) {
                    m_last_feedback_time[source_key] = Simulator::Now();
                    
                    Ptr<Packet> feedback_pkt = Create<Packet>(0);
                    BitmapFeedbackTag f_tag;
                    f_tag.m_bitmap = m_dtor_path_states[source_key];
                    feedback_pkt->AddPacketTag(f_tag);

                    qbbHeader seqh;
                    seqh.SetSeq(0);
                    seqh.SetPG(ch.udp.pg);
                    seqh.SetSport(ch.udp.dport);
                    seqh.SetDport(ch.udp.sport);
                    seqh.SetIntHeader(ch.udp.ih);
                    seqh.SetIrnNack(0);
                    seqh.SetIrnNackSize(0);
                    feedback_pkt->AddHeader(seqh);

                    CustomHeader f_ch;
                    f_ch.sip = ch.dip; 
                    f_ch.dip = ch.sip; 
                    f_ch.l3Prot = 0xFA; 
                    f_ch.ack.sport = ch.udp.dport;
                    f_ch.ack.dport = ch.udp.sport; 
                    f_ch.ack.pg = ch.udp.pg;
                    f_ch.ack.seq = 0;
                    f_ch.ack.flags = 0;
                    f_ch.ack.irnNack = 0;
                    f_ch.ack.irnNackSize = 0;
                    f_ch.ack.ih = ch.udp.ih;

                    Ipv4Header ipHeader;
                    ipHeader.SetSource(Ipv4Address(ch.dip));
                    ipHeader.SetDestination(Ipv4Address(ch.sip));
                    ipHeader.SetProtocol(0xFA); 
                    ipHeader.SetPayloadSize(feedback_pkt->GetSize());
                    ipHeader.SetTtl(64);
                    feedback_pkt->AddHeader(ipHeader);

                    PppHeader ppp;
                    ppp.SetProtocol(0x0021); 
                    feedback_pkt->AddHeader(ppp);

                    SendToDev(feedback_pkt, f_ch);

                    if (Settings::dtor_feedback_mode != 0) {
                        m_dtor_path_states[source_key].set();
                        m_dtor_total_pkt_cnt[source_key] = 0;
                    }
                }
            }
        }
    }
    SendToDev(packet, ch);
    return true;
}

void SwitchNode::SendToDev(Ptr<Packet> p, CustomHeader &ch) {
    /** HIJACK: hijack the packet and run DoSwitchSend internally for Conga and ConWeave.
     * Note that DoLbConWeave() and DoLbConga() are flow-ECMP function for control packets
     * or intra-ToR traffic.
     */

    // Conga
    PaperLbTag paperTag;
    bool forceEcmp = p->PeekPacketTag(paperTag) && paperTag.lb == 0;
    if (Settings::lb_mode == 3 && !forceEcmp) {
        m_mmu->m_congaRouting.RouteInput(p, ch);
        return;
    }

    // ConWeave
    if (Settings::lb_mode == 9) {
        m_mmu->m_conweaveRouting.RouteInput(p, ch);
        return;
    }

    // Others, GLB also 交由 GetOutDev 处理
    SendToDevContinue(p, ch);
}

void SwitchNode::SendToDevContinue(Ptr<Packet> p, CustomHeader &ch) {
    int idx = GetOutDev(p, ch);
    
    // 如果是 GCN/LSN 控制包，在数据面终结，不进行物理转发
    if (idx == (int)0xFFFFFFFF) {
        return; 
    }
    
    if (idx >= 0) {
        NS_ASSERT_MSG(m_devices[idx]->IsLinkUp(), "The routing table look up should return link that is up");

        // Short flows deliberately use ECMP in the hybrid experiment, but their bytes still
        // consume the links measured by CONGA's DRE. Account them on source-ToR and spine
        // inter-switch outputs, matching the links RouteInput measures for CONGA traffic.
        PaperLbTag paperTag;
        bool forceEcmp = p->PeekPacketTag(paperTag) && paperTag.lb == 0;
        if (Settings::lb_mode == 3 && forceEcmp && ch.l3Prot == 0x11) {
            std::map<uint32_t, uint32_t>::const_iterator destination =
                Settings::hostIp2SwitchId.find(ch.dip);
            if (destination != Settings::hostIp2SwitchId.end() && m_id != destination->second) {
                m_mmu->m_congaRouting.AccountBytes(p->GetSize(), idx);
            }
        }
        
        // 恢复原有的 qIndex 优先级判断逻辑
        uint32_t qIndex;
        if (ch.l3Prot == 0xFF || ch.l3Prot == 0xFE ||
            (m_ackHighPrio &&
             (ch.l3Prot == 0xFD ||
              ch.l3Prot == 0xFC ||
              ch.l3Prot == 0xFA))) {  // QCN, PFC, ACK/NACK, or bitmap feedback
            qIndex = 0;               // 最高优先级队列
        } else {
            qIndex = (ch.l3Prot == 0x06 ? 1 : ch.udp.pg);  // 其他协议走普通队列
        }

        DoSwitchSend(p, ch, idx, qIndex);  
        return;
    }
    
    std::cout << "WARNING - Drop occurs in SendToDevContinue()" << std::endl;
    return;
}

int SwitchNode::GetOutDev(Ptr<Packet> p, CustomHeader &ch) {
    // ======== 提前拦截 GLB 广播控制包，避免走入底层单播 ========
    if (Settings::lb_mode == 10) {
        GcnTag gcnTag;
        LsnTag lsnTag;
        if (p->PeekPacketTag(gcnTag) || p->PeekPacketTag(lsnTag)) {
            m_glbRouting->RouteInput(p, ch, std::vector<int>()); // 内部解析并刷新状态
            return 0xFFFFFFFF; // 明确丢弃，控制面终结
        }
    }
    // look up entries
    auto entry = m_rtTable.find(ch.dip);

    // no matching entry
    if (entry == m_rtTable.end()) {
        std::cout << "[ERROR] Sw(" << m_id << ")," << PARSE_FIVE_TUPLE(ch)
                  << "No matching entry, so drop this packet at SwitchNode (l3Prot:" << ch.l3Prot
                  << ")" << std::endl;
        assert(false);
    }

    // entry found
    const auto &nexthops = entry->second;
    bool control_pkt =
        (ch.l3Prot == 0xFF || ch.l3Prot == 0xFE || ch.l3Prot == 0xFD ||
         ch.l3Prot == 0xFC || ch.l3Prot == 0xFA);

    PaperLbTag paperTag;
    bool forceEcmp = p->PeekPacketTag(paperTag) && paperTag.lb == 0;
    if (Settings::lb_mode == 0 || control_pkt || forceEcmp) {
        return DoLbFlowECMP(p, ch, nexthops);     
    }

    switch (Settings::lb_mode) {
        case 2:
            return DoLbDrill(p, ch, nexthops);
        case 3:
            return DoLbConga(p, ch, nexthops); /** DUMMY: Do ECMP */
        case 6:
            return DoLbLetflow(p, ch, nexthops);
        case 9:
            return DoLbConWeave(p, ch, nexthops); /** DUMMY: Do ECMP */
        case 10:
            return DoLbGlb(p, ch, nexthops);
        case 14: // MP-RDMA VP is the UDP source port, hashed by ECMP.
        case 11:
            return DoLbReps(p, ch, nexthops);   /* REPS, do ECMP */
        case 13:
            return DoLbBitmap(p, ch, nexthops);     /* Bitmap 单字段哈希 */
        default:
            std::cout << "Unknown lb_mode(" << Settings::lb_mode << ")" << std::endl;
            assert(false);
    }
}

/*
 * The (possible) callback point when conweave dequeues packets from buffer
 */
void SwitchNode::DoSwitchSend(Ptr<Packet> p, CustomHeader &ch, uint32_t outDev, uint32_t qIndex) {
    // admission control
    FlowIdTag t;
    p->PeekPacketTag(t);
    uint32_t inDev = t.GetFlowId();

    /** NOTE:
     * ConWeave control packets have the high priority as ACK/NACK/PFC/etc with qIndex = 0.
     */
    if (inDev == Settings::CONWEAVE_CTRL_DUMMY_INDEV) { // sanity check
        // ConWeave reply is on ACK protocol with high priority, so qIndex should be 0
        assert(qIndex == 0 && m_ackHighPrio == 1 && "ConWeave's reply packet follows ACK, so its qIndex should be 0");
    }

    if (qIndex != 0) {  // not highest priority
        if (m_mmu->CheckEgressAdmission(outDev, qIndex,
                                        p->GetSize())) {  // Egress Admission control
            if (m_mmu->CheckIngressAdmission(inDev, qIndex,
                                             p->GetSize())) {  // Ingress Admission control
                m_mmu->UpdateIngressAdmission(inDev, qIndex, p->GetSize());
                m_mmu->UpdateEgressAdmission(outDev, qIndex, p->GetSize());
            } else { /** DROP: At Ingress */
#if (0)
                // /** NOTE: logging dropped pkts */
                // std::cout << "LostPkt ingress - Sw(" << m_id << ")," << PARSE_FIVE_TUPLE(ch)
                //           << "L3Prot:" << ch.l3Prot
                //           << ",Size:" << p->GetSize()
                //           << ",At " << Simulator::Now() << std::endl;
#endif
                Settings::dropped_pkt_sw_ingress++;
                return;  // drop
            }
        } else { /** DROP: At Egress */
#if (0)
            // /** NOTE: logging dropped pkts */
            // std::cout << "LostPkt egress - Sw(" << m_id << ")," << PARSE_FIVE_TUPLE(ch)
            //           << "L3Prot:" << ch.l3Prot << ",Size:" << p->GetSize() << ",At "
            //           << Simulator::Now() << std::endl;
#endif
            Settings::dropped_pkt_sw_egress++;
            return;  // drop
        }

        CheckAndSendPfc(inDev, qIndex);
    }

    m_devices[outDev]->SwitchSend(qIndex, p, ch);
}

void SwitchNode::SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p) {
    FlowIdTag t;
    p->PeekPacketTag(t);
    if (qIndex != 0) {
        uint32_t inDev = t.GetFlowId();
        if (inDev != Settings::CONWEAVE_CTRL_DUMMY_INDEV) {
            // NOTE: ConWeave's probe/reply does not need to pass inDev interface,
            // so skip for conweave's queued packets
            m_mmu->RemoveFromIngressAdmission(inDev, qIndex, p->GetSize());
        }
        m_mmu->RemoveFromEgressAdmission(ifIndex, qIndex, p->GetSize());
        if (m_ecnEnabled) {
            bool egressCongested = m_mmu->ShouldSendCN(ifIndex, qIndex);
            if (egressCongested) {
                PppHeader ppp;
                Ipv4Header h;
                p->RemoveHeader(ppp);
                p->RemoveHeader(h);
                h.SetEcn((Ipv4Header::EcnType)0x03);
                p->AddHeader(h);
                p->AddHeader(ppp);
            }
        }
        // NOTE: ConWeave's probe/reply does not need to pass inDev interface
        if (inDev != Settings::CONWEAVE_CTRL_DUMMY_INDEV) {
            CheckAndSendResume(inDev, qIndex);
        }
    }

    // HPCC's INT
    if (1) {
        uint8_t *buf = p->GetBuffer();
        if (buf[PppHeader::GetStaticSize() + 9] == 0x11) {  // udp packet
            IntHeader *ih = (IntHeader *)&buf[PppHeader::GetStaticSize() + 20 + 8 +
                                              6];  // ppp, ip, udp, SeqTs, INT
            Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(m_devices[ifIndex]);
            if (m_ccMode == 3) {  // HPCC
                ih->PushHop(Simulator::Now().GetTimeStep(), m_txBytes[ifIndex],
                            dev->GetQueue()->GetNBytesTotal(), dev->GetDataRate().GetBitRate());
            }
        }
    }
    m_txBytes[ifIndex] += p->GetSize();
}

uint32_t SwitchNode::EcmpHash(const uint8_t *key, size_t len, uint32_t seed) {
    uint32_t h = seed;
    if (len > 3) {
        const uint32_t *key_x4 = (const uint32_t *)key;
        size_t i = len >> 2;
        do {
            uint32_t k = *key_x4++;
            k *= 0xcc9e2d51;
            k = (k << 15) | (k >> 17);
            k *= 0x1b873593;
            h ^= k;
            h = (h << 13) | (h >> 19);
            h += (h << 2) + 0xe6546b64;
        } while (--i);
        key = (const uint8_t *)key_x4;
    }
    if (len & 3) {
        size_t i = len & 3;
        uint32_t k = 0;
        key = &key[i - 1];
        do {
            k <<= 8;
            k |= *key--;
        } while (--i);
        k *= 0xcc9e2d51;
        k = (k << 15) | (k >> 17);
        k *= 0x1b873593;
        h ^= k;
    }
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

void SwitchNode::SetEcmpSeed(uint32_t seed) { m_ecmpSeed = seed; }

void SwitchNode::AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx) {
    uint32_t dip = dstAddr.Get();
    m_rtTable[dip].push_back(intf_idx);
}

void SwitchNode::ClearTable() { m_rtTable.clear(); }

uint64_t SwitchNode::GetTxBytesOutDev(uint32_t outdev) {
    assert(outdev < pCnt);
    return m_txBytes[outdev];
}

bool SwitchNode::IsPortPaused(uint32_t port) {
    // 端口 0 通常未被实际用于数据转发，物理有效端口从 1 开始
    if (port == 0 || port >= GetNDevices()) return false;
    
    // 获取对应的底层物理网卡设备
    Ptr<QbbNetDevice> qbbDev = DynamicCast<QbbNetDevice>(GetDevice(port));
    
    if (qbbDev) {
        // 遍历所有可能的业务队列 (跳过 q=0，因为 0 队列是控制面专用的免丢包高优队列)
        for (uint32_t q = 1; q < QbbNetDevice::qCnt; ++q) {
            if (qbbDev->IsPaused(q)) {
                // 只要有任何一个数据队列被下游发送的 PFC 暂停了，就认为该链路不可走
                return true; 
            }
        }
    }
    return false;
}

} /* namespace ns3 */
