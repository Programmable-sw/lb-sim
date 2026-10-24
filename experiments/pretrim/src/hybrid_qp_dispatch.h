#ifndef V7G_HYBRID_QP_DISPATCH_H
#define V7G_HYBRID_QP_DISPATCH_H
#include <stdexcept>
#include <string>

namespace pretrim {
struct HybridQpConfig {
    std::string protocol, lb, cc;
};

inline HybridQpConfig hybrid_qp_dispatch(bool hybrid, const std::string& role,
                                        const std::string& protocol,
                                        const std::string& lb,
                                        const std::string& cc) {
    if (!hybrid) return {protocol, lb, cc};
    if (protocol != "mprdma" && protocol != "reps")
        throw std::invalid_argument("Hybrid requires native mprdma or reps");
    if (role == "background") return {"dcqcn", "ecmp", "dcqcn"};
    if (role != "healthy" && role != "diagnostic")
        throw std::invalid_argument("unsupported Hybrid QP role");
    return {protocol, protocol, "fixed"};
}
}
#endif
