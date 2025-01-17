////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#pragma once

#include <stdexcept>

#include <libexplain/mmap.h>
#include <libexplain/munmap.h>
#include <sys/mman.h>

#include "gaia/exception.hpp"

#include "gaia_internal/common/fd_helpers.hpp"
#include "gaia_internal/common/system_error.hpp"

namespace gaia
{
namespace common
{

// We use a template to avoid a cast from void* in the caller.
template <typename T>
inline void map_fd_data(T*& addr, size_t length, int protection, int flags, int fd, size_t offset)
{
    void* mapping = ::mmap(nullptr, length, protection, flags, fd, static_cast<off_t>(offset));
    if (mapping == MAP_FAILED)
    {
        int err = errno;
        const char* reason = ::explain_mmap(nullptr, length, protection, flags, fd, static_cast<off_t>(offset));
        throw system_error(reason, err);
    }
    addr = static_cast<T*>(mapping);
}

// We use a template because the compiler won't convert T* to void*&.
template <typename T>
inline void unmap_fd_data(T*& addr, size_t length)
{
    if (addr)
    {
        void* tmp = addr;
        addr = nullptr;
        if (-1 == ::munmap(tmp, length))
        {
            int err = errno;
            const char* reason = ::explain_munmap(tmp, length);
            throw system_error(reason, err);
        }
    }
}

} // namespace common
} // namespace gaia
