#include "ops_random_entropy.h"
#include <cassert>
#include <array>
#include <iostream>
int main(){
 using namespace pfc_pretrim_v7;
 OpsRandomEntropy a(41,17,3,112), b(41,17,3,112), c(42,17,3,112);
 std::array<unsigned,8> counts{}; bool repeated=false,different=false;unsigned prev=99;
 for(unsigned i=0;i<65536;++i){
  auto x=a.next(),y=b.next(),z=c.next();
  assert(x.entropy==y.entropy&&x.path==y.path);
  assert(x.path==ops_freebsd_hash(17,x.entropy,a.salt())%8);
  assert(x.entropy<=65535&&x.path<8);
  different|=x.path!=z.path;repeated|=prev==x.path;prev=x.path;counts[x.path]++;
 }
 assert(different&&repeated);
 for(auto n:counts)assert(n>7000&&n<9500);
 OpsRandomEntropy first(41,17,3,112),other(41,18,3,113),again(41,17,3,112);
 for(int i=0;i<100;++i){other.next();assert(first.next().entropy==again.next().entropy);}
 assert(ops_freebsd_hash(1,2,3)==ops_freebsd_hash(1,2,3));
 std::cout<<"OPS entropy/hash invariants passed\n";
}
