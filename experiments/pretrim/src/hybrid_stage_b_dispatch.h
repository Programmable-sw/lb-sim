#ifndef V7G_HYBRID_STAGE_B_DISPATCH_H
#define V7G_HYBRID_STAGE_B_DISPATCH_H
#include "hybrid_qp_dispatch.h"
namespace pretrim {
inline HybridQpConfig hybrid_stage_b_dispatch(bool hybrid, const std::string& role,
 const std::string& protocol, const std::string& lb, const std::string& cc,bool common_transport=false) {
 if(hybrid && protocol=="ops") {
  if(role=="background")return {"dcqcn","ecmp","dcqcn"};
  if(role!="healthy" && role!="diagnostic")throw std::invalid_argument("unsupported Hybrid QP role");
  return {"dcqcn","ops",common_transport?"dctcp_variant":"dcqcn"};
 }
 auto c=hybrid_qp_dispatch(hybrid,role,protocol,lb,cc);if(common_transport&&hybrid&&role!="background")c.cc="dctcp_variant";return c;
}
}
#endif
