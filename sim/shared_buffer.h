#ifndef SHARED_BUFFER_H
#define SHARED_BUFFER_H

#include <stdint.h>

class SharedBufferSwitch {
public:
    enum Admission { ADMIT, DROP_EGRESS, DROP_INGRESS };
    virtual ~SharedBufferSwitch() {}
    virtual void shared_register_ingress(const void* queue) = 0;
    virtual void shared_register_egress(const void* queue) = 0;
    virtual Admission shared_admit(const void* ingress, uint32_t pg,
                                    const void* egress, uint32_t queue,
                                    uint64_t bytes) = 0;
    virtual void shared_release(const void* ingress, uint32_t pg,
                                const void* egress, uint32_t queue,
                                uint64_t bytes) = 0;
    virtual bool shared_should_pause(const void* ingress, uint32_t pg) const = 0;
    virtual bool shared_should_resume(const void* ingress, uint32_t pg) const = 0;
    virtual uint64_t shared_pause_threshold(uint32_t pg) const = 0;
    virtual bool shared_should_mark_ecn(const void* egress, uint32_t queue,
                                        uint64_t kmin, uint64_t kmax,
                                        double sample01) const = 0;
    virtual uint64_t shared_buffer_used() const = 0;
};

#endif
