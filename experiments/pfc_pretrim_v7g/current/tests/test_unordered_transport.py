import pathlib
import subprocess
import tempfile
import unittest

BASE = pathlib.Path(__file__).resolve().parents[1] / 'src'

class UnorderedTests(unittest.TestCase):
    def test_receive_sack_capacity_and_duplicate(self):
        self.compile_run(r'''
SharedRxBudget metadata(4096); RxPayloadCapacity payload(3*4192);
UnorderedRxWindow r(metadata);
assert(r.accept_data(130,&payload,4192)==RxResult::OUT_OF_ORDER);
assert(r.next_expected()==0 && r.sack(128)==4);
assert(r.accept_data(130,&payload,4192)==RxResult::DUPLICATE);
assert(payload.used()==4192 && metadata.used()==1);
assert(r.accept_data(65,&payload,4192)==RxResult::OUT_OF_ORDER);
assert(r.sack(64)==2 && r.sack(128)==4);
assert(r.accept_data(16384,&payload,4192)==RxResult::DISTANCE_OVERFLOW);
assert(r.accept_data(0,&payload,4192)==RxResult::CONTIGUOUS);
assert(r.accept_data(1,&payload,4192)==RxResult::CAPACITY_OVERFLOW);
payload.release(4192); assert(r.accept_data(1,&payload,4192)==RxResult::CONTIGUOUS);
assert(r.accept_data(0,&payload,4192)==RxResult::OLD);
assert(payload.used()==3*4192);
''')

    def test_fifo_wrap_and_gap_fill(self):
        self.compile_run(r'''
SharedRxBudget b(4096); UnorderedRxWindow r(b);
for(unsigned i=1;i<200;i++) assert(r.accept_data(i)==RxResult::OUT_OF_ORDER);
assert(r.next_expected()==0 && b.used()==199);
assert(r.accept_data(0)==RxResult::CONTIGUOUS);
assert(r.next_expected()==200 && !b.used());
for(unsigned i=200;i<33000;i++) assert(r.accept_data(i)==RxResult::CONTIGUOUS);
assert(r.next_expected()==33000 && !r.occupancy());
''')

    def test_sender_sack_and_qp_timer(self):
        self.compile_run(r'''
assert(sack_confirms(130,0,128,4));
assert(!sack_confirms(129,0,128,4));
assert(sack_confirms(1,2,128,0));
QpRetransmissionTimer t;
t.sent(1,0,10,1010); t.sent(2,0,12,1012);
assert(t.deadline()==1010);
t.sent(1,1,20,1020); assert(t.deadline()==1012);
assert(t.expired(1011).empty());
auto due=t.expired(1012); assert(due.size()==1 && due[0].psn==2);
t.ack(1); assert(t.empty());
t.sent(1,3,100,1100); t.sent(1,4,200,1200);
t.sent(1,2,250,1250);
assert(t.expired(1100).empty());
auto newest=t.expired(1200); assert(newest.size()==1 && newest[0].attempt==4);
NativeAckEchoLedger echoes;
assert(!echoes.first_payload_echo(7,false));
assert(echoes.first_payload_echo(7,true));
assert(!echoes.first_payload_echo(7,true));
UecDctcpState cc(50,16384);
cc.ack(1,false); assert(cc.cwnd()>50);
double old=cc.cwnd(); cc.ack(1,true); assert(cc.cwnd()<old);
old=cc.cwnd(); cc.loss(); assert(cc.cwnd()==old-1);
for(int i=0;i<100;i++) cc.loss(); assert(cc.cwnd()==1);
''')

    def test_duplicate_echo_advances_native_mp_cumulative_without_credit(self):
        self.compile_run(r'''
MpRdmaState mp(128,8,32);
for(unsigned i=0;i<128;i++)mp.note_sent(i);
mp.note_data_ack(127,false,3,false,0);
mp.enter_recovery(0); double old=mp.cwnd(); uint64_t credits=mp.ack_credit();
assert(mp.recovery_active() && mp.snd_una()==0);
mp.note_cumulative_progress(128);
assert(mp.snd_una()==128 && !mp.recovery_active());
assert(mp.cwnd()==old && mp.ack_credit()==credits);
assert(mp.new_data_allowed(0,~uint64_t(0)));
mp.note_cumulative_progress(64);
assert(mp.snd_una()==128 && mp.cwnd()==old && mp.ack_credit()==credits);
''')

    def test_source_queue_skips_only_stale_unordered_retransmissions(self):
        self.compile_run(r'''
assert(unordered_retransmission_needed(false,false,0,0));
assert(unordered_retransmission_needed(true,true,2,2));
assert(!unordered_retransmission_needed(true,false,2,2));
assert(!unordered_retransmission_needed(true,true,1,2));
''')

    def compile_run(self, body):
        source='#include "unordered_transport.h"\n#include <cassert>\nusing namespace pfc_pretrim_v7;\nint main(){'+body+'}\n'
        with tempfile.TemporaryDirectory() as d:
            cmd=['g++','-std=c++11','-O0','-I'+str(BASE),'-x','c++','-','-o',d+'/test']
            p=subprocess.run(cmd,input=source,text=True,capture_output=True)
            self.assertEqual(p.returncode,0,p.stderr)
            p=subprocess.run([d+'/test'],capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)

if __name__=='__main__': unittest.main()
