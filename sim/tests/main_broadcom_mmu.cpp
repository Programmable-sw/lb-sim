#include <assert.h>
#include <stdint.h>

#include "../broadcom_mmu.h"

static void test_egress_guarantee_and_dynamic_threshold() {
    BroadcomMmu mmu(64 * 1024, 4096, 0.0625, 1.0, 16 * 1024);
    mmu.registerIngressPort(1);
    mmu.registerEgressPort(7);

    assert(mmu.admit(1, 3, 7, 3, 4096) == BroadcomMmu::ADMIT);
    assert(mmu.egressQueueMinBytes(7, 3) == 4096);
    assert(mmu.egressQueueSharedBytes(7, 3) == 0);
    assert(mmu.pauseThresholdBytes(3) > 2 * 4096);

    assert(mmu.admit(1, 3, 7, 3, 4096) == BroadcomMmu::ADMIT);
    assert(mmu.egressQueueSharedBytes(7, 3) == 4095);
    assert(mmu.ingressPgBytes(1, 3) == 8192);

    mmu.release(1, 3, 7, 3, 4096);
    mmu.release(1, 3, 7, 3, 4096);
    assert(mmu.totalIngressBytes() == 0);
    assert(mmu.egressQueueMinBytes(7, 3) == 0);
    assert(mmu.egressQueueSharedBytes(7, 3) == 0);
}

static void test_pg_pause_resume_and_headroom() {
    BroadcomMmu mmu(256 * 1024, 4096, 0.0625, 1.0, 8192);
    mmu.registerIngressPort(2);
    mmu.registerEgressPort(8);

    while (!mmu.shouldPause(2, 3))
        assert(mmu.admit(2, 3, 8, 3, 4096) == BroadcomMmu::ADMIT);
    assert(!mmu.shouldResume(2, 3));

    while (mmu.ingressPgBytes(2, 3) > 8192)
        mmu.release(2, 3, 8, 3, 4096);
    assert(mmu.shouldResume(2, 3));
}

static void test_ports_and_service_pools_are_independent() {
    BroadcomMmu mmu(512 * 1024, 4096, 0.0625, 1.0, 16 * 1024);
    mmu.registerIngressPort(1);
    mmu.registerIngressPort(2);
    mmu.registerEgressPort(5);
    mmu.registerEgressPort(6);
    assert(mmu.admit(1, 1, 5, 1, 4096) == BroadcomMmu::ADMIT);
    assert(mmu.admit(2, 3, 6, 3, 4096) == BroadcomMmu::ADMIT);
    assert(mmu.ingressServicePoolBytes(1) == 4096);
    assert(mmu.ingressServicePoolBytes(0) == 4096);
    assert(mmu.egressServicePoolBytes(1) == 0);
    assert(mmu.egressServicePoolBytes(0) == 0);
}

static void test_ingress_sp_accounting_is_symmetric() {
    BroadcomMmu mmu(512 * 1024, 1048, 0.0625, 1.0, 58288);
    mmu.registerIngressPort(1);
    mmu.registerEgressPort(1);
    assert(mmu.admit(1, 3, 1, 3, 4144) == BroadcomMmu::ADMIT);
    assert(mmu.totalIngressBytes() == 4144);
    assert(mmu.ingressServicePoolBytes(0) == 4144);
    mmu.release(1, 3, 1, 3, 4144);
    assert(mmu.totalIngressBytes() == 0);
    assert(mmu.ingressServicePoolBytes(0) == 0);
}

int main() {
    test_egress_guarantee_and_dynamic_threshold();
    test_pg_pause_resume_and_headroom();
    test_ports_and_service_pools_are_independent();
    test_ingress_sp_accounting_is_symmetric();
    return 0;
}
