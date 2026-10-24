import subprocess,tempfile,unittest
from pathlib import Path
BASE=Path(__file__).resolve().parents[1]
class ArtifactTests(unittest.TestCase):
 def test_real_nack_callback_exits_expired_freeze(self):
  # Compile the actual production callback with a minimal sender context.
  # No network state is invented: this unit exercises the feedback already RX'd.
  text=(BASE/'src/artifact_events.inc').read_text()
  method=text[text.index('void artifact_nack_feedback('):text.index('\nvoid artifact_timeout(')]
  source=r'''
#include "artifact_lb.h"
#include "artifact_cc.h"
#include "protocol_wiring.h"
#include <memory>
#include <map>
#include <vector>
#include <cassert>
using namespace std;
#define LIGHT_TRACE(...) do {} while(0)
const unsigned PAYLOAD=4096;
struct SimPacket{int flow=0,entropy=123;uint64_t psn=7,attempt=0;bool pretrim_origin=false;};
struct Flow{string name="test";uint64_t outstanding_bytes=4096;set<uint64_t>outstanding_psns{7};};
struct Harness{
 vector<Flow>flows{Flow()};map<uint64_t,uint64_t>latest_retrans_attempt{{7,0}};
 map<int,unique_ptr<pretrim::ArtifactDctcp>>artifact_dctcp;
 map<int,unique_ptr<pretrim::ArtifactNscc>>artifact_nscc;
 map<int,unique_ptr<pretrim::ArtifactReps>>artifact_reps;map<int,pretrim::ArtifactMp>artifact_mp;
 string artifact_cc_name="dctcp_variant";
 struct Clock{uint64_t now(){return 111;}}ev;
 string qp_protocol(int){return "reps";}
 uint64_t artifact_cc_window(int id){return artifact_dctcp[id]->window_bytes();}
 Harness(){artifact_dctcp[0].reset(new pretrim::ArtifactDctcp(50,4096));
 artifact_reps[0].reset(new pretrim::ArtifactReps(8,4,100));
 artifact_reps[0]->feedback(123,false,0);artifact_reps[0]->timeout(10);}
''' + method + r'''
};
int main(){for(bool pretrim:{false,true}){Harness h;auto p=make_shared<SimPacket>();
p->pretrim_origin=pretrim;auto before=h.artifact_cc_window(0);h.artifact_nack_feedback(p);
assert(!h.artifact_reps[0]->frozen());assert(h.artifact_reps[0]->explore_left()==4);
if(pretrim)assert(h.artifact_cc_window(0)==before);
}}
'''
  with tempfile.TemporaryDirectory(prefix='artifact-nack-')as d:
   exe=Path(d)/'check'
   p=subprocess.run(['g++','-std=c++11','-I'+str(BASE/'src'),'-x','c++','-','-o',str(exe)],input=source,text=True,capture_output=True)
   self.assertEqual(p.returncode,0,p.stderr)
   p=subprocess.run([str(exe)],capture_output=True,text=True)
   self.assertEqual(p.returncode,0,p.stderr)
 def test_official_function_oracle(self):
  with tempfile.TemporaryDirectory(prefix='artifact-oracle-') as d:
   exe=Path(d)/'check'
   subprocess.run(['g++','-std=c++11','-O2','-I'+str(BASE/'src'),str(BASE/'tests/test_artifact_oracle.cpp'),'-o',str(exe)],check=True)
   p=subprocess.run([str(exe)],capture_output=True,text=True)
   self.assertEqual(p.returncode,0,p.stderr+'\n'+p.stdout[-500:])
 def test_mechanisms(self):
  with tempfile.TemporaryDirectory(prefix='artifact-unit-') as d:
   exe=Path(d)/'check'
   subprocess.run(['g++','-std=c++11','-O2','-I'+str(BASE/'src'),str(BASE/'tests/test_artifact.cpp'),'-o',str(exe)],check=True)
   subprocess.run([str(exe)],check=True)
