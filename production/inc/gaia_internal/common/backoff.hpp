////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <cstddef>

namespace gaia
{
namespace common
{
namespace backoff
{

#if defined(__x86_64__)
static void pause() { asm volatile("pause"); }
#elif defined(__aarch64__)
static void pause() { asm volatile("yield"); }
#else
static void pause() { asm volatile("" ::: "memory"); }
#endif

// On pre-Skylake Intel CPUs, PAUSE has latency of ~10 cycles. We assume the
// same (for now) for AMD x86_64 CPUs.
constexpr size_t c_pause_cycles_x86_default{10};
// On Skylake, PAUSE has latency of ~140 cycles (!).
constexpr size_t c_pause_cycles_skylake{140};
// On Cascade Lake and later Intel uarchs, PAUSE has latency of ~40 cycles.
constexpr size_t c_pause_cycles_cascadelake{40};

// TODO: document YIELD latency for AArch64. So far it seems to have latency of
// only 1 cycle, but I'm not sure. Also could avoid hardcoding PAUSE latencies
// by using RDTSC (or ARM equivalent) to count cycles in spin loop.

// REVIEW: We assume for now that our CPU runs at 3GHz. Later we could get the
// clock speed at compile- or runtime.
constexpr size_t c_cycles_per_ns{3};
constexpr size_t c_ns_per_us{1000};

// REVIEW: Assume for now we are always running on x86_64 (Cascade Lake). Again,
// we could make this configurable at compile- or runtime.
constexpr size_t c_pause_cycles{c_pause_cycles_cascadelake};
constexpr size_t c_pause_iterations_per_us{(c_cycles_per_ns * c_ns_per_us) / c_pause_cycles};

void spin_wait(size_t iterations)
{
    for (size_t i = 0; i < iterations; ++i)
    {
        pause();
    }
}

} // namespace backoff
} // namespace common
} // namespace gaia
