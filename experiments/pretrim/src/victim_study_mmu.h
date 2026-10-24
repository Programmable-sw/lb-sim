#ifndef V7G_VICTIM_STUDY_MMU_H
#define V7G_VICTIM_STUDY_MMU_H
#include <cstdint>
inline uint64_t study_xon(uint64_t xoff, uint64_t hysteresis) {
    const uint64_t floor = 8192;
    return xoff > floor && xoff-floor > hysteresis ? xoff-hysteresis : floor;
}
#endif
