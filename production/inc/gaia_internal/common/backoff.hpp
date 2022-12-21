////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <immintrin.h>

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

// The target latency for this spin loop is 5us (not configurable for now).

// On Skylake (~3GHz), PAUSE has latency of ~140 cycles (!), so we spin for 108 iterations.
constexpr size_t c_spin_iterations_skylake{108};
// On Cascade Lake (~3GHz), PAUSE has latency of ~40 cycles, so we spin for 375 iterations.
constexpr size_t c_spin_iterations_cascadelake{375};

constexpr size_t c_spin_iterations = c_spin_iterations_cascadelake;

void spin_wait_5us()
{
    for (size_t i = 0; i < c_spin_iterations; ++i)
    {
        pause();
    }
}

} // namespace backoff
} // namespace common
} // namespace gaia
