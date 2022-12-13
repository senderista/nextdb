////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include "db_server.hpp"

#include <unistd.h>

#include <csignal>

#include <atomic>
#include <format>
#include <functional>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>

#include "gaia/exceptions.hpp"

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/bitmap.hpp"
#include "gaia_internal/common/scope_guard.hpp"
#include "gaia_internal/common/socket_helpers.hpp"
#include "gaia_internal/common/system_error.hpp"
#include "gaia_internal/db/db_object.hpp"

#include "db_helpers.hpp"
#include "memory_helpers.hpp"
#include "safe_ts.hpp"

using namespace flatbuffers;
using namespace gaia::db;
using namespace gaia::db::memory_manager;
using namespace gaia::db::messages;
using namespace gaia::db::transactions;
using namespace gaia::common;
using namespace gaia::common::bitmap;
using namespace gaia::common::iterators;
using namespace gaia::common::scope_guard;

using persistence_mode_t = server_config_t::persistence_mode_t;

static constexpr char c_message_unexpected_event_received[] = "Unexpected event received!";
static constexpr char c_message_current_event_is_inconsistent_with_state_transition[]
    = "Current event is inconsistent with state transition!";
static constexpr char c_message_thread_must_be_joinable[] = "Thread must be joinable!";
static constexpr char c_message_epoll_create1_failed[] = "epoll_create1() failed!";
static constexpr char c_message_epoll_wait_failed[] = "epoll_wait() failed!";
static constexpr char c_message_epoll_ctl_failed[] = "epoll_ctl() failed!";
static constexpr char c_message_unexpected_event_type[] = "Unexpected event type!";
static constexpr char c_message_epollerr_flag_should_not_be_set[] = "EPOLLERR flag should not be set!";
static constexpr char c_message_unexpected_fd[] = "Unexpected fd!";
static constexpr char c_message_unexpected_errno_value[] = "Unexpected errno value!";
static constexpr char c_message_unexpected_stream_type[] = "Unexpected stream type!";

void server_t::handle_connect(
    session_event_t event, const void*, session_state_t old_state, session_state_t new_state)
{
    ASSERT_PRECONDITION(event == session_event_t::CONNECT, c_message_unexpected_event_received);

    // This message should only be received after the client thread was first initialized.
    ASSERT_PRECONDITION(
        old_state == session_state_t::DISCONNECTED && new_state == session_state_t::CONNECTED,
        c_message_current_event_is_inconsistent_with_state_transition);

    // We need to reply to the client with the fds for the data/locator segments.
    FlatBufferBuilder builder;
    build_server_reply_info(builder, session_event_t::CONNECT, old_state, new_state);

    // Collect fds.
    int fd_list[static_cast<size_t>(data_mapping_t::index_t::count_mappings)];
    data_mapping_t::collect_fds(c_data_mappings, fd_list);

    send_msg_with_fds(
        session_socket(),
        &fd_list[0],
        static_cast<size_t>(data_mapping_t::index_t::count_mappings),
        builder.GetBufferPointer(),
        builder.GetSize());
}

void server_t::handle_client_shutdown(
    session_event_t event, const void*, session_state_t, session_state_t new_state)
{
    ASSERT_PRECONDITION(
        event == session_event_t::CLIENT_SHUTDOWN,
        c_message_unexpected_event_received);

    ASSERT_PRECONDITION(
        new_state == session_state_t::DISCONNECTED,
        c_message_current_event_is_inconsistent_with_state_transition);

    // If this event is received, the client must have closed the write end of the socket
    // (equivalent of sending a FIN), so we need to do the same. Closing the socket
    // will send a FIN to the client, so they will read EOF and can close the socket
    // as well. We can just set the shutdown flag here, which will break out of the poll
    // loop and immediately close the socket. (If we received EOF from the client after
    // we closed our write end, then we would be calling shutdown(SHUT_WR) twice, which
    // is another reason to just close the socket.)
    s_session_context->session_shutdown = true;

    // TODO: If the session had an active txn, clean up all its resources.
}

void server_t::handle_server_shutdown(
    session_event_t event, const void*, session_state_t, session_state_t new_state)
{
    ASSERT_PRECONDITION(
        event == session_event_t::SERVER_SHUTDOWN,
        c_message_unexpected_event_received);

    ASSERT_PRECONDITION(
        new_state == session_state_t::DISCONNECTED,
        c_message_current_event_is_inconsistent_with_state_transition);

    // This transition should only be triggered on notification of the server shutdown event.
    // Because we are about to shut down, we can't wait for acknowledgment from the client and
    // should just close the session socket. As noted above, setting the shutdown flag will
    // immediately break out of the poll loop and close the session socket.
    s_session_context->session_shutdown = true;
}

std::pair<int, int> server_t::get_stream_socket_pair()
{
    // Create a connected pair of datagram sockets, one of which we will keep
    // and the other we will send to the client.
    // We use SOCK_SEQPACKET because it supports both datagram and connection
    // semantics: datagrams allow buffering without framing, and a connection
    // ensures that client returns EOF after server has called shutdown(SHUT_WR).
    int socket_pair[2] = {-1};
    if (-1 == ::socketpair(PF_UNIX, SOCK_SEQPACKET, 0, socket_pair))
    {
        throw_system_error("socketpair() failed!");
    }

    auto [client_socket, server_socket] = socket_pair;
    // We need to use the initializer + mutable hack to capture structured bindings in a lambda.
    auto socket_cleanup = make_scope_guard(
        [client_socket = client_socket, server_socket = server_socket]() mutable {
            close_fd(client_socket);
            close_fd(server_socket);
        });

    // Set server socket to be nonblocking, because we use it within an epoll loop.
    set_non_blocking(server_socket);

    socket_cleanup.dismiss();
    return std::pair{client_socket, server_socket};
}

void server_t::handle_request_stream(
    session_event_t event, const void* event_data, session_state_t old_state, session_state_t new_state)
{
    ASSERT_PRECONDITION(
        event == session_event_t::REQUEST_STREAM,
        c_message_unexpected_event_received);

    // This event never changes session state.
    ASSERT_PRECONDITION(
        old_state == new_state,
        c_message_current_event_is_inconsistent_with_state_transition);

    // We can't use structured binding names in a lambda capture list.
    int client_socket, server_socket;
    std::tie(client_socket, server_socket) = get_stream_socket_pair();

    // The client socket should unconditionally be closed on exit because it's
    // duplicated when passed to the client and we no longer need it on the
    // server.
    auto client_socket_cleanup = make_scope_guard([&client_socket] { close_fd(client_socket); });
    auto server_socket_cleanup = make_scope_guard([&server_socket] { close_fd(server_socket); });

    auto request = static_cast<const client_request_t*>(event_data);

    switch (request->data_type())
    {
        // TODO: stream type-specific code goes here
        default:
            ASSERT_UNREACHABLE(c_message_unexpected_stream_type);
    }

    // Transfer ownership of the server socket to the stream producer thread.
    server_socket_cleanup.dismiss();

    // Any exceptions after this point will close the server socket, ensuring the producer thread terminates.
    // However, its destructor will not run until the session thread exits and joins the producer thread.
    FlatBufferBuilder builder;
    build_server_reply_info(builder, event, old_state, new_state);
    send_msg_with_fds(session_socket(), &client_socket, 1, builder.GetBufferPointer(), builder.GetSize());
}

void server_t::apply_transition(session_event_t event, const void* event_data)
{
    if (event == session_event_t::NOP)
    {
        return;
    }

    messages::session_state_t& session_state = s_session_context->session_state;

    for (auto t : c_valid_transitions)
    {
        if (t.event == event && (t.state == session_state || t.state == session_state_t::ANY))
        {
            session_state_t new_state = t.transition.new_state;

            // If the transition's new state is ANY, then keep the state the same.
            if (new_state == session_state_t::ANY)
            {
                new_state = session_state;
            }

            session_state_t old_state = session_state;
            session_state = new_state;

            if (t.transition.handler)
            {
                t.transition.handler(event, event_data, old_state, session_state);
            }

            return;
        }
    }

    // If we get here, we haven't found any compatible transition.
    // TODO: consider propagating exception back to client?
    throw invalid_session_transition(
        "No allowed state transition from state '"
        + std::string(EnumNamesession_state_t(session_state))
        + "' with event '"
        + std::string(EnumNamesession_event_t(event))
        + "'.");
}

void server_t::build_server_reply_info(
    FlatBufferBuilder& builder,
    session_event_t event,
    session_state_t old_state,
    session_state_t new_state,
    gaia_txn_id_t txn_id,
    log_offset_t txn_log_offset,
    const std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_logs_to_apply)
{
    builder.ForceDefaults(true);
    const auto txn_logs_to_apply_vec = builder.CreateVectorOfStructs<transaction_log_info_t>(
        txn_logs_to_apply.size(),
        [&](size_t i, transaction_log_info_t* t) -> void {
            const auto& [txn_id, log_offset] = txn_logs_to_apply[i];
            gaia_txn_id_t commit_ts = get_txn_metadata()->get_commit_ts_from_begin_ts(txn_id);
            *t = {txn_id, commit_ts, log_offset};
        });
    const auto transaction_info = Createtransaction_info_t(builder, txn_id, txn_log_offset, txn_logs_to_apply_vec);
    const auto server_reply = Createserver_reply_t(
        builder, event, old_state, new_state,
        reply_data_t::transaction_info, transaction_info.Union());
    const auto message = Createmessage_t(builder, any_message_t::reply, server_reply.Union());
    builder.Finish(message);
}

void server_t::clear_server_state()
{
    data_mapping_t::close(c_data_mappings);
}

// To avoid synchronization, we assume that this method is only called when
// no sessions exist and the server is not accepting any new connections.
void server_t::init_shared_memory()
{
    ASSERT_PRECONDITION(s_listening_socket == -1, "Listening socket should not be open!");
    ASSERT_PRECONDITION(!s_session_context, "init_shared_memory() should not be called within a database session!");

    // Just in case this is invoked from a reinitialization path.
    clear_server_state();

    // Clear server state if an exception is thrown.
    auto cleanup_memory = make_scope_guard([] { clear_server_state(); });

    // Validate shared memory mapping definitions and assert that mappings are not made yet.
    data_mapping_t::validate(c_data_mappings, std::size(c_data_mappings));
    for (auto data_mapping : c_data_mappings)
    {
        ASSERT_INVARIANT(!data_mapping.is_set(), "Memory should be unmapped");
    }

    // s_shared_locators uses sizeof(gaia_offset_t) * c_max_locators = 16GB of virtual address space.
    //
    // s_shared_data uses (64B) * c_max_locators = 256GB of virtual address space.
    //
    // s_shared_logs uses (16B) * c_max_locators = 64GB of virtual address space.
    //
    // s_shared_id_index uses (32B) * c_max_locators = 128GB of virtual address space
    // (assuming 4-byte alignment). We could eventually shrink this to
    // 4B/locator (assuming 4-byte locators), or 16GB, if we can assume that
    // gaia_ids are sequentially allocated and seldom deleted, so we can just
    // use an array of locators indexed by gaia_id.
    //
    // s_shared_type_index uses (8B) * c_max_locators = 32GB of virtual address space.
    //
    // s_shared_txn_metadata uses (8B) * get_max_ts_count() = 32TB of virtual address space.
    data_mapping_t::create(c_data_mappings, s_server_conf.instance_name().c_str());

    // The server currently doesn't use the memory manager or chunk manager, but
    // we need to perform one-time initialization of the memory manager metadata
    // somewhere.
    memory_manager_t memory_manager;
    memory_manager.initialize(
        reinterpret_cast<uint8_t*>(s_shared_data.data()->objects),
        sizeof(s_shared_data.data()->objects));

    // REVIEW: The data mapping code should ideally call the default constructor
    // for the templated type using placement new, but this zero-initializes
    // giant arrays that are zeroed already, so we have to use ad-hoc
    // initialization where it's necessary.
    get_logs()->initialize();

    cleanup_memory.dismiss();
}

void server_t::init_memory_manager(bool initializing)
{
    if (initializing)
    {
        // This is only called by the main thread, to prepare for recovery.
        s_session_context->memory_manager.initialize(
            reinterpret_cast<uint8_t*>(s_shared_data.data()->objects),
            sizeof(s_shared_data.data()->objects));

        chunk_offset_t chunk_offset = s_session_context->memory_manager.allocate_chunk();
        if (!chunk_offset.is_valid())
        {
            throw memory_allocation_error_internal();
        }
        s_session_context->chunk_manager.initialize(chunk_offset);
    }
    else
    {
        // This is called by server-side session threads, to use in GC.
        // These threads perform no allocations, so they do not need to
        // initialize their chunk manager with an allocated chunk.
        s_session_context->memory_manager.load(
            reinterpret_cast<uint8_t*>(s_shared_data.data()->objects),
            sizeof(s_shared_data.data()->objects));
    }
}

sigset_t server_t::get_masked_signals()
{
    sigset_t sigset;
    ::sigemptyset(&sigset);

    ::sigaddset(&sigset, SIGHUP);
    ::sigaddset(&sigset, SIGINT);
    ::sigaddset(&sigset, SIGTERM);
    ::sigaddset(&sigset, SIGQUIT);

    return sigset;
}

void server_t::signal_handler(sigset_t sigset, int& signum)
{
    // Wait until a signal is delivered.
    // REVIEW: do we have any use for sigwaitinfo()?
    ::sigwait(&sigset, &signum);

    std::cerr << "Caught signal '" << ::strsignal(signum) << "'." << std::endl;

    signal_eventfd_multiple_threads(s_server_shutdown_eventfd);
}

void server_t::init_listening_socket(const std::string& socket_name)
{
    // Launch a connection-based listening Unix socket on a well-known address.
    // We use SOCK_SEQPACKET to get connection-oriented *and* datagram semantics.
    // This socket needs to be nonblocking so we can use epoll to wait on the
    // shutdown eventfd as well (the connected sockets it spawns will inherit
    // nonblocking mode). Actually, nonblocking mode may not have any effect in
    // level-triggered epoll mode, but it's good to ensure we can never block,
    // in case of bugs or surprising semantics in epoll.
    int listening_socket = ::socket(PF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK, 0);
    if (listening_socket == -1)
    {
        throw_system_error("Socket creation failed!");
    }
    auto socket_cleanup = make_scope_guard([&listening_socket] { close_fd(listening_socket); });

    // Initialize the socket address structure.
    sockaddr_un server_addr{};
    server_addr.sun_family = AF_UNIX;

    // The socket name (minus its null terminator) needs to fit into the space
    // in the server address structure after the prefix null byte.
    ASSERT_INVARIANT(
        socket_name.size() <= sizeof(server_addr.sun_path) - 1,
        std::format("Socket name '{}' is too long!", socket_name).c_str());

    // We prepend a null byte to the socket name so the address is in the
    // (Linux-exclusive) "abstract namespace", i.e., not bound to the
    // filesystem.
    ::strncpy(&server_addr.sun_path[1], socket_name.c_str(), sizeof(server_addr.sun_path) - 1);

    // Bind the socket to the address and start listening for connections.
    // The socket name is not null-terminated in the address structure, but
    // we need to add an extra byte for the null byte prefix.
    socklen_t server_addr_size = sizeof(server_addr.sun_family) + 1 + ::strlen(&server_addr.sun_path[1]);
    if (-1 == ::bind(listening_socket, reinterpret_cast<struct sockaddr*>(&server_addr), server_addr_size))
    {
        // REVIEW: Identify other common errors that should have user-friendly error messages.
        if (errno == EADDRINUSE)
        {
            std::cerr << "ERROR: bind() failed! - " << (::strerror(errno)) << std::endl;
            std::cerr
                << "The " << c_db_server_name
                << " cannot start because another instance is already running."
                << std::endl
                << "Stop any instances of the server and try again."
                << std::endl;
            exit(1);
        }

        throw_system_error("bind() failed!");
    }
    if (-1 == ::listen(listening_socket, 0))
    {
        throw_system_error("listen() failed!");
    }

    socket_cleanup.dismiss();
    s_listening_socket = listening_socket;
}

bool server_t::authenticate_client_socket(int socket)
{
    struct ucred cred;
    socklen_t cred_len = sizeof(cred);
    if (-1 == ::getsockopt(socket, SOL_SOCKET, SO_PEERCRED, &cred, &cred_len))
    {
        throw_system_error("getsockopt(SO_PEERCRED) failed!");
    }

    // Client must have same effective user ID as server.
    return (cred.uid == ::geteuid());
}

bool server_t::can_start_session(int socket_fd)
{
    if (s_session_threads.size() >= c_session_limit)
    {
        std::cerr << "Disconnecting new session because session limit has been exceeded." << std::endl;
        return false;
    }

    if (!authenticate_client_socket(socket_fd))
    {
        std::cerr << "Disconnecting new session because authentication failed" << std::endl;
        return false;
    }

    return true;
}

// We adopt a lazy GC approach to freeing thread resources, rather than having
// each thread clean up after itself on exit. This approach allows us to avoid
// any synchronization between the exiting thread and its owning thread, as well
// as the awkwardness of passing each thread a reference to its owning object.
// In general, any code which creates a new thread is expected to call this
// function to compensate for the "garbage" it is adding to the system.
//
// Removing a thread entry is O(1) (because we swap it with the last element and
// truncate the last element), so the whole scan with removals is O(n).
static void reap_exited_threads(std::vector<std::thread>& threads)
{
    for (auto it = threads.begin(); it != threads.end();)
    {
        // Test if the thread has already exited (this is possible with the
        // pthreads API but not with the std::thread API).
        auto handle = it->native_handle();

        // pthread_kill(0) returns 0 if the thread is still running, and ESRCH
        // otherwise (unless it has already been detached or joined, in which
        // case the thread ID may be invalid or reused, possibly causing a
        // segfault). We never use a thread ID after the thread has been joined
        // (and we never detach threads), so we should be OK.
        //
        // https://man7.org/linux/man-pages/man3/pthread_kill.3.html
        // "POSIX.1-2008 recommends that if an implementation detects the use of
        // a thread ID after the end of its lifetime, pthread_kill() should
        // return the error ESRCH. The glibc implementation returns this error
        // in the cases where an invalid thread ID can be detected. But note
        // also that POSIX says that an attempt to use a thread ID whose
        // lifetime has ended produces undefined behavior, and an attempt to use
        // an invalid thread ID in a call to pthread_kill() can, for example,
        // cause a segmentation fault."
        //
        // https://man7.org/linux/man-pages/man3/pthread_self.3.html
        // "A thread ID may be reused after a terminated thread has been joined,
        // or a detached thread has terminated."
        int error = ::pthread_kill(handle, 0);

        if (error == 0)
        {
            // The thread is still running, so do nothing.
            ++it;
        }
        else if (error == ESRCH)
        {
            // If this thread has already exited, then join it and deallocate
            // its object to release both memory and thread-related system
            // resources.
            ASSERT_INVARIANT(it->joinable(), c_message_thread_must_be_joinable);
            it->join();

            // Move the last element into the current entry.
            *it = std::move(threads.back());
            threads.pop_back();
        }
        else
        {
            // Throw on all other errors (e.g., if the thread has been detached
            // or joined).
            throw_system_error("pthread_kill(0) failed!", error);
        }
    }
}

void server_t::client_dispatch_handler(const std::string& socket_name)
{
    // Register session cleanup handler first, so we can execute it last.
    auto session_cleanup = make_scope_guard([] {
        for (auto& thread : s_session_threads)
        {
            ASSERT_INVARIANT(thread.joinable(), c_message_thread_must_be_joinable);
            thread.join();
        }
        // All session threads have been joined, so they can be destroyed.
        s_session_threads.clear();
    });

    // Start listening for incoming client connections.
    init_listening_socket(socket_name);
    // We close the listening socket before waiting for session threads to exit,
    // so no new sessions can be established while we wait for all session
    // threads to exit (we assume they received the same server shutdown
    // notification that we did).
    auto listener_cleanup = make_scope_guard([&] { close_fd(s_listening_socket); });

    // Set up the epoll loop.
    int epoll_fd = ::epoll_create1(0);
    if (epoll_fd == -1)
    {
        throw_system_error(c_message_epoll_create1_failed);
    }

    // We close the epoll descriptor before closing the listening socket, so any
    // connections that arrive before the listening socket is closed will
    // receive ECONNRESET rather than ECONNREFUSED. This is perhaps unfortunate
    // but shouldn't really matter in practice.
    auto epoll_cleanup = make_scope_guard([&epoll_fd] { close_fd(epoll_fd); });
    int registered_fds[] = {s_listening_socket, s_server_shutdown_eventfd};
    for (int registered_fd : registered_fds)
    {
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = registered_fd;
        if (-1 == ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, registered_fd, &ev))
        {
            throw_system_error(c_message_epoll_ctl_failed);
        }
    }
    epoll_event events[std::size(registered_fds)];

    // Enter the epoll loop.
    while (true)
    {
        // Block forever (we will be notified of shutdown).
        int ready_fd_count = ::epoll_wait(epoll_fd, events, std::size(events), -1);
        if (ready_fd_count == -1)
        {
            // Attaching the debugger will send a SIGSTOP which we can't block.
            // Any signal which we block will set the shutdown eventfd and will
            // alert the epoll fd, so we don't have to worry about getting EINTR
            // from a signal intended to terminate the process.
            if (errno == EINTR)
            {
                continue;
            }
            throw_system_error(c_message_epoll_wait_failed);
        }

        for (int i = 0; i < ready_fd_count; ++i)
        {
            epoll_event ev = events[i];
            // We never register for anything but EPOLLIN,
            // but EPOLLERR will always be delivered.
            if (ev.events & EPOLLERR)
            {
                if (ev.data.fd == s_listening_socket)
                {
                    int error = 0;
                    socklen_t err_len = sizeof(error);
                    // Ignore errors getting error message and default to generic error message.
                    ::getsockopt(s_listening_socket, SOL_SOCKET, SO_ERROR, static_cast<void*>(&error), &err_len);
                    throw_system_error("Client socket error!", error);
                }
                else if (ev.data.fd == s_server_shutdown_eventfd)
                {
                    throw_system_error("Shutdown eventfd error!");
                }
            }

            // At this point, we should only get EPOLLIN.
            ASSERT_INVARIANT(ev.events == EPOLLIN, c_message_unexpected_event_type);

            if (ev.data.fd == s_listening_socket)
            {
                int session_socket = ::accept(s_listening_socket, nullptr, nullptr);
                if (session_socket == -1)
                {
                    throw_system_error("accept() failed!");
                }

                // The connecting client will get ECONNRESET on their first
                // read from this socket.
                if (!can_start_session(session_socket))
                {
                    close_fd(session_socket);
                    continue;
                }

                // First reap any session threads that have terminated (to
                // avoid memory and system resource leaks).
                reap_exited_threads(s_session_threads);

                // Create session thread.
                s_session_threads.emplace_back(session_handler, session_socket);
            }
            else if (ev.data.fd == s_server_shutdown_eventfd)
            {
                consume_eventfd(s_server_shutdown_eventfd);
                return;
            }
            else
            {
                // We don't monitor any other fds.
                ASSERT_UNREACHABLE(c_message_unexpected_fd);
            }
        }
    }
}

void server_t::session_handler(int socket)
{
    s_session_context = new server_session_context_t();
    auto cleanup_session_context = make_scope_guard([&] {
        delete s_session_context;
        s_session_context = nullptr;
    });

    // Set up session socket.
    s_session_context->session_socket = socket;

    // Initialize this thread's memory manager.
    bool initializing = false;
    init_memory_manager(initializing);

    // Initialize thread-local data for safe timestamp mechanism.
    safe_ts_t::initialize(get_safe_ts_entries(), get_watermarks());

    // Set up epoll loop.
    int epoll_fd = ::epoll_create1(0);
    if (epoll_fd == -1)
    {
        throw_system_error(c_message_epoll_create1_failed);
    }
    auto epoll_cleanup = make_scope_guard([&epoll_fd] { close_fd(epoll_fd); });

    int fds[] = {session_socket(), s_server_shutdown_eventfd};
    for (int fd : fds)
    {
        // We should only get EPOLLRDHUP from the client socket, but oh well.
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = fd;
        if (-1 == ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev))
        {
            throw_system_error(c_message_epoll_ctl_failed);
        }
    }
    epoll_event events[std::size(fds)];

    // Event to signal session-owned threads to terminate.
    s_session_context->session_shutdown_eventfd = make_eventfd();

    // Enter epoll loop.
    while (!s_session_context->session_shutdown)
    {
        // Block forever (we will be notified of shutdown).
        int ready_fd_count = ::epoll_wait(epoll_fd, events, std::size(events), -1);
        if (ready_fd_count == -1)
        {
            // Attaching the debugger will send a SIGSTOP which we can't block.
            // Any signal which we block will set the shutdown eventfd and will
            // alert the epoll fd, so we don't have to worry about getting EINTR
            // from a signal intended to terminate the process.
            if (errno == EINTR)
            {
                continue;
            }
            throw_system_error(c_message_epoll_wait_failed);
        }

        session_event_t event = session_event_t::NOP;
        const void* event_data = nullptr;

        // Buffer used to send and receive all message data.
        uint8_t msg_buf[c_max_msg_size_in_bytes]{0};

        // Buffer used to receive file descriptors.
        int fd_buf[c_max_fd_count] = {-1};
        size_t fd_buf_size = std::size(fd_buf);

        // If the shutdown flag is set, we need to exit immediately before
        // processing the next ready fd.
        for (int i = 0; i < ready_fd_count && !s_session_context->session_shutdown; ++i)
        {
            epoll_event ev = events[i];
            if (ev.data.fd == session_socket())
            {
                // NB: Because many event flags are set in combination with others, the
                // order we test them in matters! E.g., EPOLLIN seems to always be set
                // whenever EPOLLRDHUP is set, so we need to test EPOLLRDHUP before
                // testing EPOLLIN.
                if (ev.events & EPOLLERR)
                {
                    // This flag is unmaskable, so we don't need to register for it.
                    int error = 0;
                    socklen_t err_len = sizeof(error);
                    // Ignore errors getting error message and default to generic error message.
                    ::getsockopt(session_socket(), SOL_SOCKET, SO_ERROR, static_cast<void*>(&error), &err_len);
                    std::cerr << "Client socket error: " << ::strerror(error) << std::endl;
                    event = session_event_t::CLIENT_SHUTDOWN;
                }
                else if (ev.events & EPOLLHUP)
                {
                    // This flag is unmaskable, so we don't need to register for it.
                    // Both ends of the socket have issued a shutdown(SHUT_WR) or equivalent.
                    ASSERT_INVARIANT(!(ev.events & EPOLLERR), c_message_epollerr_flag_should_not_be_set);
                    event = session_event_t::CLIENT_SHUTDOWN;
                }
                else if (ev.events & EPOLLRDHUP)
                {
                    // The client has called shutdown(SHUT_WR) to signal their intention to
                    // disconnect. We do the same by closing the session socket.
                    // REVIEW: Can we get both EPOLLHUP and EPOLLRDHUP when the client half-closes
                    // the socket after we half-close it?
                    ASSERT_INVARIANT(!(ev.events & EPOLLHUP), "EPOLLHUP flag should not be set!");
                    event = session_event_t::CLIENT_SHUTDOWN;
                }
                else if (ev.events & EPOLLIN)
                {
                    ASSERT_INVARIANT(
                        !(ev.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)),
                        "EPOLLERR, EPOLLHUP, EPOLLRDHUP flags should not be set!");

                    // Read client message with possible file descriptors.
                    size_t bytes_read = recv_msg_with_fds(
                        session_socket(), fd_buf, &fd_buf_size, msg_buf, sizeof(msg_buf));
                    // We shouldn't get EOF unless EPOLLRDHUP is set.
                    // REVIEW: it might be possible for the client to call shutdown(SHUT_WR)
                    // after we have already woken up on EPOLLIN, in which case we would
                    // legitimately read 0 bytes and this assert would be invalid.
                    ASSERT_INVARIANT(bytes_read > 0, "Failed to read message!");

                    const message_t* msg = Getmessage_t(msg_buf);
                    const client_request_t* request = msg->msg_as_request();
                    event = request->event();
                    event_data = static_cast<const void*>(request);
                }
                else
                {
                    // We don't register for any other events.
                    ASSERT_UNREACHABLE(c_message_unexpected_event_type);
                }
            }
            else if (ev.data.fd == s_server_shutdown_eventfd)
            {
                ASSERT_INVARIANT(ev.events == EPOLLIN, "Expected EPOLLIN event type!");
                consume_eventfd(s_server_shutdown_eventfd);
                event = session_event_t::SERVER_SHUTDOWN;
            }
            else
            {
                // We don't monitor any other fds.
                ASSERT_UNREACHABLE(c_message_unexpected_fd);
            }

            ASSERT_INVARIANT(event != session_event_t::NOP, c_message_unexpected_event_type);

            // The transition handlers are the only places we currently call
            // send_msg_with_fds(). We need to handle a peer_disconnected
            // exception thrown from that method (translated from EPIPE).
            try
            {
                apply_transition(event, event_data);
            }
            catch (const peer_disconnected& e)
            {
                std::cerr << "Client socket error: " << e.what() << std::endl;
                s_session_context->session_shutdown = true;
            }
        }
    }
}

template <typename T_element>
void server_t::stream_producer_handler(
    int stream_socket, int cancel_eventfd, std::shared_ptr<generator_t<T_element>> generator_fn)
{
    // We can rely on close_fd() to perform the equivalent of shutdown(SHUT_RDWR), because we
    // hold the only fd pointing to this socket.
    auto socket_cleanup = make_scope_guard([&stream_socket] { close_fd(stream_socket); });

    // Verify that the socket is the correct type for the semantics we assume.
    check_socket_type(stream_socket, SOCK_SEQPACKET);

    // Check that our stream socket is non-blocking (so we don't accidentally block in write()).
    ASSERT_PRECONDITION(is_non_blocking(stream_socket), "Stream socket is in blocking mode!");

    int epoll_fd = ::epoll_create1(0);
    if (epoll_fd == -1)
    {
        throw_system_error(c_message_epoll_create1_failed);
    }
    auto epoll_cleanup = make_scope_guard([&epoll_fd] { close_fd(epoll_fd); });

    // We poll for write availability of the stream socket in level-triggered mode,
    // and only write at most one buffer of data before polling again, to avoid read
    // starvation of the cancellation eventfd.
    epoll_event sock_ev{};
    sock_ev.events = EPOLLOUT;
    sock_ev.data.fd = stream_socket;
    if (-1 == ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, stream_socket, &sock_ev))
    {
        throw_system_error(c_message_epoll_ctl_failed);
    }

    epoll_event cancel_ev{};
    cancel_ev.events = EPOLLIN;
    cancel_ev.data.fd = cancel_eventfd;
    if (-1 == ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cancel_eventfd, &cancel_ev))
    {
        throw_system_error(c_message_epoll_ctl_failed);
    }

    epoll_event events[2];
    bool producer_shutdown = false;
    bool disabled_writable_notification = false;

    // The userspace buffer that we use to construct a batch datagram message.
    std::vector<T_element> batch_buffer;

    // We need to call reserve() rather than the "sized" constructor to avoid changing size().
    batch_buffer.reserve(c_stream_batch_size);

    auto gen_it = generator_iterator_t<T_element>(generator_fn);

    while (!producer_shutdown)
    {
        // Block forever (we will be notified of shutdown).
        int ready_fd_count = ::epoll_wait(epoll_fd, events, std::size(events), -1);
        if (ready_fd_count == -1)
        {
            // Attaching the debugger will send a SIGSTOP which we can't block.
            // Any signal which we block will set the shutdown eventfd and will
            // alert the epoll fd, so we don't have to worry about getting EINTR
            // from a signal intended to terminate the process.
            if (errno == EINTR)
            {
                continue;
            }
            throw_system_error(c_message_epoll_wait_failed);
        }

        // If the shutdown flag is set, we need to exit immediately before
        // processing the next ready fd.
        for (int i = 0; i < ready_fd_count && !producer_shutdown; ++i)
        {
            epoll_event ev = events[i];
            if (ev.data.fd == stream_socket)
            {
                // NB: Because many event flags are set in combination with others, the
                // order we test them in matters! E.g., EPOLLIN seems to always be set
                // whenever EPOLLRDHUP is set, so we need to test EPOLLRDHUP before
                // testing EPOLLIN.
                if (ev.events & EPOLLERR)
                {
                    // This flag is unmaskable, so we don't need to register for it.
                    int error = 0;
                    socklen_t err_len = sizeof(error);

                    // Ignore errors getting error message and default to generic error message.
                    ::getsockopt(stream_socket, SOL_SOCKET, SO_ERROR, static_cast<void*>(&error), &err_len);
                    std::cerr << "Stream socket error: '" << ::strerror(error) << "'." << std::endl;
                    producer_shutdown = true;
                }
                else if (ev.events & EPOLLHUP)
                {
                    // This flag is unmaskable, so we don't need to register for it.
                    // We should get this when the client has closed its end of the socket.
                    ASSERT_INVARIANT(!(ev.events & EPOLLERR), c_message_epollerr_flag_should_not_be_set);
                    producer_shutdown = true;
                }
                else if (ev.events & EPOLLOUT)
                {
                    ASSERT_INVARIANT(
                        !disabled_writable_notification,
                        "Received write readiness notification on socket after deregistering from EPOLLOUT!");

                    ASSERT_INVARIANT(
                        !(ev.events & (EPOLLERR | EPOLLHUP)),
                        "EPOLLERR and EPOLLHUP flags should not be set!");

                    // Write to the send buffer until we exhaust either the iterator or the buffer free space.
                    while (gen_it && (batch_buffer.size() < c_stream_batch_size))
                    {
                        T_element next_val = *gen_it;
                        batch_buffer.push_back(next_val);
                        ++gen_it;
                    }

                    // We need to send any pending data in the buffer, followed by EOF if we
                    // exhausted the iterator. We let the client decide when to close the socket,
                    // because their next read may be arbitrarily delayed (and they may still have
                    // pending data).

                    // First send any remaining data in the buffer.
                    if (batch_buffer.size() > 0)
                    {
                        // To simplify client state management by allowing the client to dequeue
                        // entries in FIFO order using std::vector.pop_back(), we reverse the order
                        // of entries in the buffer.
                        std::reverse(std::begin(batch_buffer), std::end(batch_buffer));

                        // We don't want to handle signals, so set MSG_NOSIGNAL to convert SIGPIPE
                        // to EPIPE.
                        if (-1 == ::send(stream_socket, batch_buffer.data(), batch_buffer.size() * sizeof(T_element), MSG_NOSIGNAL))
                        {
                            // Break out of the poll loop on any write error.
                            producer_shutdown = true;

                            // It should never happen that the socket is no longer writable after we
                            // receive EPOLLOUT, because we are the only writer and the receive
                            // buffer is always large enough for a batch.
                            ASSERT_INVARIANT(errno != EAGAIN && errno != EWOULDBLOCK, c_message_unexpected_errno_value);

                            // There is a race between the client reading its pending datagram and
                            // triggering an EPOLLOUT notification, and then closing its socket and
                            // triggering an EPOLLHUP notification. We might receive EPIPE or
                            // ECONNRESET from the preceding write if we haven't yet processed the
                            // EPOLLHUP notification. This doesn't indicate an error condition on
                            // the client side, so we don't log socket errors in this case. Even if
                            // the write failed because the client-side session thread crashed, we
                            // will detect and handle that condition in the server-side session
                            // thread.
                            //
                            // REVIEW: We could avoid setting the producer_shutdown flag in this
                            // case and wait for an EPOLLHUP notification in the next loop
                            // iteration. We exit immediately for now because 1) we aren't doing
                            // nontrivial cleanup on receiving EPOLLHUP (just setting the
                            // producer_shutdown flag), and 2) expecting an EPOLLHUP notification to
                            // always be delivered even after receiving EPIPE or ECONNRESET seems
                            // like a fragile assumption (we could validate this assumption with a
                            // test program, but again this is undocumented behavior and therefore
                            // subject to change).
                            if (errno != EPIPE && errno != ECONNRESET)
                            {
                                std::cerr << "Stream socket error: '" << ::strerror(errno) << "'." << std::endl;
                            }
                        }
                        else
                        {
                            // We successfully wrote to the socket, so clear the buffer. (Partial
                            // writes are impossible with datagram sockets.) The standard is
                            // somewhat unclear, but apparently clear() will not change the capacity
                            // in any recent implementation of the standard library
                            // (https://cplusplus.github.io/LWG/issue1102).
                            batch_buffer.clear();
                        }
                    }

                    // If we exhausted the iterator, send EOF to client. (We still need to wait for
                    // the client to close their socket, because they may still have unread data, so
                    // we don't set the producer_shutdown flag.)
                    if (!gen_it)
                    {
                        ::shutdown(stream_socket, SHUT_WR);
                        // Unintuitively, after we call shutdown(SHUT_WR), the socket is always
                        // writable, because a write will never block, but any write will return
                        // EPIPE. Therefore, we unregister the socket for writable notifications
                        // after we call shutdown(SHUT_WR). We should now only be notified (with
                        // EPOLLHUP/EPOLLERR) when the client closes the socket, so we can close
                        // our end of the socket and terminate the thread.
                        epoll_event ev{};
                        // We're only interested in EPOLLHUP/EPOLLERR notifications, and we
                        // don't need to register for those.
                        ev.events = 0;
                        ev.data.fd = stream_socket;
                        if (-1 == ::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, stream_socket, &ev))
                        {
                            throw_system_error(c_message_epoll_ctl_failed);
                        }
                        disabled_writable_notification = true;
                    }
                }
                else
                {
                    // We don't register for any other events.
                    ASSERT_UNREACHABLE(c_message_unexpected_event_type);
                }
            }
            else if (ev.data.fd == cancel_eventfd)
            {
                ASSERT_INVARIANT(ev.events == EPOLLIN, c_message_unexpected_event_type);
                consume_eventfd(cancel_eventfd);
                producer_shutdown = true;
            }
            else
            {
                // We don't monitor any other fds.
                ASSERT_UNREACHABLE(c_message_unexpected_fd);
            }
        }
    }
}

template <typename T_element>
void server_t::start_stream_producer(int stream_socket, std::shared_ptr<generator_t<T_element>> generator_fn)
{
    // First reap any owned threads that have terminated (to avoid memory and
    // system resource leaks).
    reap_exited_threads(session_owned_threads());

    // Create stream producer thread.
    session_owned_threads().emplace_back(
        stream_producer_handler<T_element>, stream_socket, s_session_context->session_shutdown_eventfd, generator_fn);
}

// This method must be run on the main thread
// (https://thomastrapp.com/blog/signal-handler-for-multithreaded-c++/).
void server_t::run(server_config_t server_conf)
{
    // There can only be one thread running at this point, so this doesn't need synchronization.
    s_server_conf = server_conf;

    // Create eventfd shutdown event.
    s_server_shutdown_eventfd = make_eventfd();
    auto cleanup_shutdown_eventfd = make_scope_guard([] {
        // We can't close this fd until all readers and writers have exited.
        // The only readers are the client dispatch thread and the session
        // threads, and the only writer is the signal handler thread. All
        // these threads must have exited before we exit this scope and this
        // handler executes.
        close_fd(s_server_shutdown_eventfd);
    });

    // Block handled signals in this thread and subsequently spawned threads, so
    // they can be handled by the dedicated signal handler thread.
    sigset_t handled_signals = get_masked_signals();

    // Per POSIX, we must use pthread_sigmask() rather than sigprocmask()
    // in a multithreaded program.
    // REVIEW: should this be SIG_SETMASK?
    ::pthread_sigmask(SIG_BLOCK, &handled_signals, nullptr);

    // Launch signal handler thread.
    int caught_signal = 0;
    std::thread signal_handler_thread(signal_handler, handled_signals, std::ref(caught_signal));

    // Initialize all shared memory structures.
    init_shared_memory();

    // Launch thread to listen for client connections and create session threads.
    std::thread client_dispatch_thread(client_dispatch_handler, server_conf.instance_name());

    // The client dispatch thread will only return after all sessions have been disconnected
    // and the listening socket has been closed.
    client_dispatch_thread.join();

    // The signal handler thread will only return after a blocked signal is pending.
    signal_handler_thread.join();

    // We shouldn't get here unless the signal handler thread has caught a signal.
    ASSERT_INVARIANT(caught_signal != 0, "A signal should have been caught!");

    // To exit with the correct status (reflecting a caught signal),
    // we need to unblock blocked signals and re-raise the signal.
    // We may have already received other pending signals by the time
    // we unblock signals, in which case they will be delivered and
    // terminate the process before we can re-raise the caught signal.
    // That is benign, because we've already performed cleanup actions
    // and the exit status will still be valid.
    ::pthread_sigmask(SIG_UNBLOCK, &handled_signals, nullptr);
    ::raise(caught_signal);
}
