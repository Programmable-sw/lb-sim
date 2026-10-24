#ifndef V7G_OPS_RANDOM_ENTROPY_H
#define V7G_OPS_RANDOM_ENTROPY_H
#include <cstdint>
namespace pretrim {
// Same uint32 mixing as workspace/csg-htsim sim/datacenter/fat_tree_switch.h.
inline uint32_t ops_freebsd_hash(uint32_t flow,uint32_t entropy,uint32_t salt){
 uint32_t a=0x9e3779b9u+flow,b=0x9e3779b9u+salt,c=entropy;
 a-=b;a-=c;a^=c>>13;b-=c;b-=a;b^=a<<8;c-=a;c-=b;c^=b>>13;
 a-=b;a-=c;a^=c>>12;b-=c;b-=a;b^=a<<16;c-=a;c-=b;c^=b>>5;
 a-=b;a-=c;a^=c>>3;b-=c;b-=a;b^=a<<10;c-=a;c-=b;c^=b>>15;
 return c;
}
struct OpsEntropyChoice{uint32_t entropy,path;};
class OpsRandomEntropy{
 uint64_t state_;uint32_t flow_,salt_;
 uint64_t random64(){
  uint64_t z=(state_+=UINT64_C(0x9e3779b97f4a7c15));
  z=(z^(z>>30))*UINT64_C(0xbf58476d1ce4e5b9);
  z=(z^(z>>27))*UINT64_C(0x94d049bb133111eb);return z^(z>>31);
 }
public:
 OpsRandomEntropy(uint32_t seed,uint32_t flow,uint32_t src,uint32_t dst):
  state_((uint64_t(seed)<<32)^uint64_t(flow)^UINT64_C(0x4f50535f55445031)^
         (uint64_t(src)<<16)^uint64_t(dst)),flow_(flow),
  salt_(ops_freebsd_hash(seed,src/16,0x4f505331u)){}
 uint32_t salt()const{return salt_;}
 // Author artifact OBLIVIOUS uses rand()%_no_of_paths; experiment scripts
 // set -paths 65535. Preserve the model's isolated per-QP random stream.
 OpsEntropyChoice next(){uint32_t e=uint32_t(random64()%65535u);return {e,ops_freebsd_hash(flow_,e,salt_)%8};}
};
}
#endif
