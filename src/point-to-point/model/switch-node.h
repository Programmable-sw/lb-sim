#ifndef SWITCH_NODE_H
#define SWITCH_NODE_H

#include <ns3/node.h>

#include <unordered_map>
#include <unordered_set>
#include <bitset>

#include "qbb-net-device.h"
#include "switch-mmu.h"
#include "glb-routing.h"

namespace ns3 {

class Packet;

class SwitchNode : public Node {
    static const unsigned qCnt = 8;    // Number of queues/priorities used
    static const unsigned pCnt = 128;  // port 0 is not used so + 1	// Number of ports used
    uint32_t m_ecmpSeed;
    std::unordered_map<uint32_t, std::vector<int> >
        m_rtTable;  // map from ip address (u32) to possible ECMP port (index of dev)

    // monitor uplinks
    uint64_t m_txBytes[pCnt];  // counter of tx bytes, for HPCC

   protected:
    bool m_ecnEnabled;
    uint32_t m_ccMode;
    uint32_t m_ackHighPrio;  // set high priority for ACK/NACK
    // lb_mode=13: dToR 监控路径状态 <源 ToR/host key, 256位Bitmap>
    std::unordered_map<uint32_t, std::bitset<256>> m_dtor_path_states; 
    // lb_mode=13: 控制反馈频率 <源 IP, 上次反馈时间>
    std::unordered_map<uint32_t, Time> m_last_feedback_time;
    // lb_mode=13: 周期包计数器 <源 ToR/host key, 周期包数>
    std::unordered_map<uint32_t, uint32_t> m_dtor_total_pkt_cnt;

   private:
    int GetOutDev(Ptr<Packet>, CustomHeader &ch);
    void SendToDev(Ptr<Packet> p, CustomHeader &ch);
    void SendToDevContinue(Ptr<Packet> p, CustomHeader &ch);
    static uint32_t EcmpHash(const uint8_t *key, size_t len, uint32_t seed);
    void CheckAndSendPfc(uint32_t inDev, uint32_t qIndex);
    void CheckAndSendResume(uint32_t inDev, uint32_t qIndex);

    /* Sending packet to Egress port */
    void DoSwitchSend(Ptr<Packet> p, CustomHeader &ch, uint32_t outDev, uint32_t qIndex);

    // PFC 感知
    bool IsPortPaused(uint32_t port);

    /*----- Load balancer -----*/
    // Flow ECMP (lb_mode = 0)
    uint32_t DoLbFlowECMP(Ptr<const Packet> p, const CustomHeader &ch,
                          const std::vector<int> &nexthops);
    // DRILL (lb_mode = 2)
    uint32_t DoLbDrill(Ptr<const Packet> p, const CustomHeader &ch,
                       const std::vector<int> &nexthops);     // choose egress port
    uint32_t m_drill_candidate;                               // always 2 (power of two)
    std::map<uint32_t, uint32_t> m_previousBestInterfaceMap;  // <dip, previousBestInterface>
    uint32_t CalculateInterfaceLoad(uint32_t interface);      // Get the load of a interface
    // Conga (lb_mode = 3)
    uint32_t DoLbConga(Ptr<Packet> p, CustomHeader &ch, const std::vector<int> &nexthops);
    // Conga (lb_mode = 6)
    uint32_t DoLbLetflow(Ptr<Packet> p, CustomHeader &ch, const std::vector<int> &nexthops);
    // ConWeave (lb_mode = 9)
    uint32_t DoLbConWeave(Ptr<const Packet> p, const CustomHeader &ch,
                          const std::vector<int> &nexthops);  // dummy
    // Glb (lb_mode = 10)
    uint32_t DoLbGlb(Ptr<Packet> p, CustomHeader &ch, 
                        const std::vector<int> &nexthops);
    // Reps (lb_mode = 11)
    uint32_t DoLbReps(Ptr<const Packet> p, const CustomHeader &ch,
                           const std::vector<int> &nexthops);
    // dToR-Assisted Bitmap (lb_mode = 13)
    uint32_t DoLbBitmap(Ptr<const Packet> p, const CustomHeader &ch,
                           const std::vector<int> &nexthops);
   
   public:
    // Ptr<BroadcomNode> m_broadcom;
    Ptr<SwitchMmu> m_mmu;
    Ptr<GlbRouting> m_glbRouting; // GLB 路由实例
    bool m_isToR;                                 // true if ToR switch
    std::unordered_set<uint32_t> m_isToR_hostIP;  // host's IP connected to this ToR

    static TypeId GetTypeId(void);
    SwitchNode();
    void SetEcmpSeed(uint32_t seed);
    void AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx);
    void ClearTable();
    bool SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch);
    void SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p);
    uint64_t GetTxBytesOutDev(uint32_t outdev);
};

} /* namespace ns3 */

#endif /* SWITCH_NODE_H */
