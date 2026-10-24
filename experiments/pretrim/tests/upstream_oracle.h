// Exact function bodies extracted from REPS_EuroSys_Artifact b7515887 uec.cpp.
// Only UecSrc -> ReferenceCc is renamed. Test scaffolding supplies its dependencies.
// Copyright (c) 2025 Ultra Ethernet Consortium. See ../src/upstream/LICENSE.
#ifndef PRETRIM_UPSTREAM_ORACLE_H
#define PRETRIM_UPSTREAM_ORACLE_H
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
using namespace std;
using mem_b=int64_t;using simtime_picosec=uint64_t;
inline double timeAsUs(uint64_t x){return double(x)/1000000;}
inline uint64_t timeFromUs(unsigned x){return uint64_t(x)*1000000;}
struct ReferenceCc {
 struct Clock{uint64_t value=0;uint64_t now()const{return value;}}clock;
 Clock&eventlist(){return clock;}
 struct Flow{unsigned flow_id()const{return 1;}string str()const{return "";}}_flow;
 struct Nic{uint64_t linkspeed()const{return 400000000000ULL;}}_nic;
 bool _debug_src=false,_debug=false,_receiver_based_cc=false,_enable_qa_gate=false;
 unsigned _debug_flowid=999;
 string _name="",_nodename="";
 mem_b _cwnd=204800,_mtu=4096,_mss=4096,_maxwnd=900000,_bdp=600000,_achieved_bytes=0,_received_bytes=0,_in_flight=0;
 uint32_t _fi_count=0,_bytes_to_ignore=0,_bytes_ignored=0,_inc_bytes=0;
 uint64_t _base_rtt=12000000,_raw_rtt=12000000,_avg_delay=0,_qa_endtime=0,_last_eta_time=0,_last_adjust_time=0,_last_dec_time=0;
 bool _trigger_qa=false,_increase=false;
 uint64_t _target_Qdelay=6000000;
 double _qa_threshold=24000000,_gamma=.8,_fi=20,_alpha=4.*4*.5*4096/6000000,_delay_alpha=.0125,_eta=.15*4096*4;
 uint64_t _adjust_period_threshold=12000000;uint32_t _adjust_bytes_threshold=8000;
 void updateCwndOnAck_DCTCP(bool,simtime_picosec,mem_b);
 bool quick_adapt(bool,simtime_picosec);void fair_increase(uint32_t);
 void proportional_increase(uint32_t,simtime_picosec);void fast_increase(uint32_t,simtime_picosec);
 void multiplicative_decrease(uint32_t);void fulfill_adjustment();
 void updateCwndOnAck_NSCC(bool,simtime_picosec,mem_b);void updateCwndOnNack_NSCC(bool,mem_b);
 void update_base_rtt(simtime_picosec,uint16_t);void update_delay(simtime_picosec,bool,bool);
 simtime_picosec get_avg_delay();
};
void ReferenceCc::updateCwndOnAck_DCTCP(bool skip, simtime_picosec rtt, mem_b newly_acked_bytes) {
    cout << timeAsUs(eventlist().now()) << " DCTCP start " << _name << " cwnd " << _cwnd
         << " with params skip " << skip << " acked bytes " << newly_acked_bytes << endl;

    if (skip == false)  // additive increase, 1 PKT /RTT
    {
        _cwnd += newly_acked_bytes * _mtu / _cwnd;

    } else {  // multiplicative decrease, done per mark, more aggressive than DCTCP (less smoothing)
              // but much simpler and more responsive since we don't need to track alpha.
        _cwnd -= newly_acked_bytes / 3;
        _cwnd = max((mem_b)_mtu, _cwnd);
    }
}

bool ReferenceCc::quick_adapt(bool is_loss, simtime_picosec avgqdelay) {
    if (_receiver_based_cc)
        return false;

    if (_debug_src){
        cout << "At " << timeAsUs(eventlist().now()) << " " << _flow.str() << " quickadapt called is loss "<< is_loss << " delay " << avgqdelay << " qa_endtime " << timeAsUs(_qa_endtime) << " trigger qa " << _trigger_qa << endl;
    }
    if (eventlist().now()>_qa_endtime){
        bool qa_gate = true;
        if (_enable_qa_gate ){
            qa_gate = (_achieved_bytes < _maxwnd/8);
        }
        if (_qa_endtime != 0 && (_trigger_qa || is_loss || (avgqdelay > _qa_threshold)) && qa_gate ){
            if (_debug_src) {
                cout << "At " << timeAsUs(eventlist().now()) << " " << _flow.str() << " running quickadapt, CWND is " << _cwnd << " setting it to " << _achieved_bytes <<  endl;
            }

            if (_cwnd < _achieved_bytes){
                if (_debug_src) {
                    cout << "This shouldn't happen: QUICK ADAPT MIGHT INCREASE THE CWND" << endl;
                }
            }
            else {
                _cwnd = max(_achieved_bytes, (mem_b)_mtu); //* _qa_scaling;
                if(_flow.flow_id() == _debug_flowid)
                    cout <<timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " quick_adapt  _cwnd " << _cwnd << " is_loss " << is_loss << endl;
                _bytes_to_ignore = _in_flight;
                _bytes_ignored = 0;
                _trigger_qa = false;
                return true;
            }
        }
        _achieved_bytes = 0;
        _qa_endtime = eventlist().now() + _base_rtt + _target_Qdelay;
    }
    return false;
}

void ReferenceCc::fair_increase(uint32_t newly_acked_bytes){
    _inc_bytes += _fi * _mtu * newly_acked_bytes; //increase by 16Million!
}

void ReferenceCc::proportional_increase(uint32_t newly_acked_bytes,simtime_picosec delay){
    fast_increase(newly_acked_bytes, delay);
    if (_increase)
        return;
    
    //make sure targetQdelay > delay;
    assert(_target_Qdelay > delay);

    _inc_bytes += (uint32_t)round(_alpha * (_target_Qdelay - delay) * (double)newly_acked_bytes);

    fair_increase(newly_acked_bytes);
}

void ReferenceCc::fast_increase(uint32_t newly_acked_bytes,simtime_picosec delay){
    if (delay < timeFromUs(1u)){
        _fi_count += newly_acked_bytes;
        if (_fi_count > _cwnd || _increase){
            _cwnd += newly_acked_bytes * 0.5;

            if (_cwnd > _maxwnd)
                _cwnd = _maxwnd;
            _increase = true;
            return;
        }
    }
    else  {
        _fi_count = 0;
    }
    _increase = false;
}

void ReferenceCc::multiplicative_decrease(uint32_t newly_acked_bytes){
    _increase = false;
    _fi_count = 0;
    simtime_picosec avg_delay = get_avg_delay();
    if (avg_delay > _target_Qdelay){
        if (eventlist().now() - _last_dec_time > _base_rtt){
            _cwnd *= max(1-_gamma*(avg_delay-_target_Qdelay)/avg_delay, 0.5);/*_max_md_jump instead of 1*/
            _last_dec_time = eventlist().now();
        }
        // fair_decrease(can_decrease, newly_acked_bytes);
    }
}

void ReferenceCc::fulfill_adjustment(){
    assert(_bdp > 0);
    if (_debug_src) {
        cout << "Running fulfill adjustment cwnd " << _cwnd << " inc " << _inc_bytes << " bdp " << _bdp << endl;
    }
    _cwnd += min((mem_b)_adjust_bytes_threshold, _inc_bytes / _cwnd); 

    _inc_bytes = 0;

    //unclear what received_bytes this is referring to. 
    _received_bytes = 0;
    _last_adjust_time = eventlist().now();
}

void ReferenceCc::updateCwndOnAck_NSCC(bool skip, simtime_picosec delay, mem_b newly_acked_bytes) {
    // bool can_decrease = _exp_avg_ecn > _ecn_thresh;

    if (_bytes_ignored < _bytes_to_ignore && skip)
        return;

    simtime_picosec avg_delay = get_avg_delay();

    if (quick_adapt(false,avg_delay))
        return;

    if (!skip && delay >= _target_Qdelay) {
        fair_increase(newly_acked_bytes);
        if (_flow.flow_id() == _debug_flowid || ReferenceCc::_debug) {
            cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " " << _flow.str() << " fair_increase  _nscc_cwnd " << _cwnd << endl;
        }
    } else if (!skip && delay < _target_Qdelay) {
        proportional_increase(newly_acked_bytes,delay);
        if (_flow.flow_id() == _debug_flowid || ReferenceCc::_debug) {
            cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " " << _flow.str() << " proportional_increase _nscc_cwnd " << _cwnd << endl;
        }
    } else if (skip && delay >= _target_Qdelay) {    
        multiplicative_decrease(newly_acked_bytes);
        if (_flow.flow_id() == _debug_flowid || ReferenceCc::_debug) {
            cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " " << _flow.str() << " multiplicative_decrease _nscc_cwnd " << _cwnd << endl;
        }
    } else if (skip && delay < _target_Qdelay) {
        // fair_decrease(can_decrease,newly_acked_bytes);
        // if (_flow.flow_id() == _debug_flowid || ReferenceCc::_debug) {
        //     cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " " << _flow.str() << " fair_decrease _nscc_cwnd " << _cwnd
        //         <<" mtu " << _mtu
        //         << "_maxwnd " << _maxwnd << endl;
        // }
        // FD is out.
        // NOOP, just switch path
    }

    // if ( _received_bytes > _adjust_bytes_threshold || eventlist().now() - _last_adjust_time > _adjust_period_threshold ) {
    if ( _received_bytes > _adjust_bytes_threshold || eventlist().now() - _last_adjust_time > _adjust_period_threshold ) {
        if (_flow.flow_id() == _debug_flowid || ReferenceCc::_debug) {
            cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<<  " " << _flow.str() << " fulfill_adjustmentx _nscc_cwnd " << _cwnd
                << " inc_bytes " << _inc_bytes
                << endl;
        }
        fulfill_adjustment();
        if (_flow.flow_id() == _debug_flowid || ReferenceCc::_debug) {
            cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " " << _flow.str() << " fulfill_adjustment _nscc_cwnd " << _cwnd << endl;
        }
    }

    if (eventlist().now() - _last_eta_time > _adjust_period_threshold ) {
        _cwnd += _eta;
        _last_eta_time = eventlist().now();
        if (_flow.flow_id() == _debug_flowid) {
            cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " " << _flow.str() << " eta _nscc_cwnd " << _cwnd << " target_q_delay " <<timeAsUs(_target_Qdelay) << endl;
        }
    }

    if (_cwnd < _mtu)
        _cwnd = _mtu;

    if (_cwnd > _maxwnd)
        _cwnd = _maxwnd;
    if (_flow.flow_id() == _debug_flowid)
        cout << timeAsUs(eventlist().now()) <<" flowid " << _flow.flow_id()<< " final _nscc_cwnd " << _cwnd << " _basertt " << timeAsUs(_base_rtt)<< endl;
}

void ReferenceCc::updateCwndOnNack_NSCC(bool skip, mem_b nacked_bytes) {
    _cwnd -= nacked_bytes;
    if (_cwnd < _mtu)
        _cwnd = _mtu;

    _trigger_qa = true;
    if (_bytes_ignored >= _bytes_to_ignore)
        quick_adapt(true, get_avg_delay());    
}

void ReferenceCc::update_base_rtt(simtime_picosec raw_rtt, uint16_t packet_size){
    if (_base_rtt > _raw_rtt && packet_size == _mtu) {
        _base_rtt = _raw_rtt;
        _bdp = timeAsUs(_raw_rtt) * _nic.linkspeed() / 8000000; 
        _maxwnd = 1.5 * _bdp;
        
        if (ReferenceCc::_debug)
            cout << "Reinit BDP and MAXWND to "  << _bdp << " " << _maxwnd << " in pkts " << _maxwnd/_mtu << endl;
    }
}

void ReferenceCc::update_delay(simtime_picosec raw_rtt, bool update_avg, bool skip){
    simtime_picosec delay = _raw_rtt - _base_rtt;
    if(update_avg){

        if(skip == false && delay > _target_Qdelay){
            _avg_delay = _delay_alpha * _base_rtt*0.25 + (1-_delay_alpha) * _avg_delay;
        }else{
            if (delay > 5*_base_rtt)
            {
                double r = 0.0125;
                _avg_delay = r * delay + (1 - r) * _avg_delay;
            }
            else
            {
                _avg_delay = _delay_alpha * delay + (1 - _delay_alpha) * _avg_delay;
            }
        }
    }
    if (_debug_src) {
        cout << "Update delay with sample " << timeAsUs(delay) << " avg is " << timeAsUs(_avg_delay) << " base rtt is " << _base_rtt << endl;
    }
}

simtime_picosec ReferenceCc::get_avg_delay(){
    return _avg_delay;
}
#endif
