#ifndef V7G_SPECIAL_UNION_H
#define V7G_SPECIAL_UNION_H
#include <cstdint>
namespace pfc_pretrim_v7 {
struct SpecialUnionDecision {bool trim,ingress_only,egress_only,both;};
inline SpecialUnionDecision special_union(bool leaf,bool actual_pause,bool eligible,
                                         bool ingress_risk,uint64_t current_queue){
 const bool allowed=leaf&&actual_pause&&eligible;
 const bool i=allowed&&ingress_risk,e=allowed&&current_queue>=400ULL*1024;
 return {i||e,i&&!e,e&&!i,i&&e};
}
}
#endif
