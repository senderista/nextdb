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
#include <thread>

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
#include "txn_metadata.hpp"

using namespace gaia::db;
using namespace gaia::db::memory_manager;
using namespace gaia::db::transactions;
using namespace gaia::common;
using namespace gaia::common::bitmap;
using namespace gaia::common::iterators;
using namespace gaia::common::scope_guard;

using persistence_mode_t = server_config_t::persistence_mode_t;

static constexpr char c_message_epoll_create1_failed[] = "epoll_create1() failed!";
static constexpr char c_message_epoll_wait_failed[] = "epoll_wait() failed!";
static constexpr char c_message_epoll_ctl_failed[] = "epoll_ctl() failed!";
static constexpr char c_message_unexpected_event_type[] = "Unexpected event type!";
static constexpr char c_message_unexpected_fd[] = "Unexpected fd!";

void server_t::clear_server_state()
{
    data_mapping_t::close(c_data_mappings);
}

// To avoid synchronization, we assume that this method is only called when
// no sessions exist and the server is not accepting any new connections.
void server_t::init_shared_memory()
{
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
    // s_shared_txn_metadata uses (8B) * txn_metadata_t::c_num_entries = 32TB of virtual address space.
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

    // TODO: Recovery (from a user-specified checkpoint) should execute here.

    // Create a dummy txn representing the initial state of the DB (either empty or recovered).
    init_txn_history();

    cleanup_memory.dismiss();
}

void server_t::init_txn_history()
{
    // Acquire a begin_ts and create a txn metadata entry for it.
    gaia_txn_id_t initial_begin_ts = register_begin_ts();
    // Set begin_ts entry status to submitted.
    get_txn_metadata()->set_active_txn_submitted(initial_begin_ts);
    // Acquire a commit_ts for this begin_ts and create a txn metadata entry for it.
    gaia_txn_id_t initial_commit_ts = register_commit_ts(initial_begin_ts, c_invalid_log_offset);
    // Set linked commit_ts in begin_ts entry.
    get_txn_metadata()->set_submitted_txn_commit_ts(initial_begin_ts, initial_commit_ts);
    // Set commit_ts entry status to committed.
    get_txn_metadata()->update_txn_decision(initial_commit_ts, true);
    // We can unconditionally mark the initial txn durable since it is either
    // empty or recovered from durable data.
    get_txn_metadata()->set_txn_durable(initial_commit_ts);
    // There was no log associated with the initial txn, so assume GC is complete.
    get_txn_metadata()->set_txn_gc_complete(initial_commit_ts);
    // Finally, advance all watermarks (except the pre-reclaim watermark) to
    // the initial commit_ts.
    get_watermarks()->advance_watermark(watermark_type_t::pre_apply, initial_commit_ts);
    get_watermarks()->advance_watermark(watermark_type_t::post_apply, initial_commit_ts);
    get_watermarks()->advance_watermark(watermark_type_t::post_gc, initial_commit_ts);

    // Assert desired state.
    ASSERT_POSTCONDITION(get_txn_metadata()->is_txn_submitted(initial_begin_ts),
        "Initial txn's begin_ts should be in SUBMITTED state!");
    ASSERT_POSTCONDITION(get_txn_metadata()->get_commit_ts_from_begin_ts(initial_begin_ts) == initial_commit_ts,
        "Initial txn's begin_ts is not linked to its commit_ts!");
    ASSERT_POSTCONDITION(get_txn_metadata()->get_begin_ts_from_commit_ts(initial_commit_ts) == initial_begin_ts,
        "Initial txn's commit_ts is not linked to its begin_ts!");
    ASSERT_POSTCONDITION(get_txn_metadata()->is_txn_committed(initial_commit_ts),
        "Initial txn's commit_ts should be in COMMITTED state!");
    ASSERT_POSTCONDITION(get_txn_metadata()->is_txn_durable(initial_commit_ts),
        "Initial txn's commit_ts should be durable!");
    ASSERT_POSTCONDITION(get_txn_metadata()->is_txn_gc_complete(initial_commit_ts),
        "Initial txn's commit_ts should be GC-complete!");
    ASSERT_POSTCONDITION(get_watermarks()->get_watermark(watermark_type_t::pre_apply) == initial_commit_ts,
        "Pre-apply watermark should be equal to initial txn's commit_ts!");
    ASSERT_POSTCONDITION(get_watermarks()->get_watermark(watermark_type_t::post_apply) == initial_commit_ts,
        "Post-apply watermark should be equal to initial txn's commit_ts!");
    ASSERT_POSTCONDITION(get_watermarks()->get_watermark(watermark_type_t::post_gc) == initial_commit_ts,
        "Post-GC watermark should be equal to initial txn's commit_ts!");
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

int server_t::get_listening_socket(const std::string& socket_name)
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
    auto server_addr_size = static_cast<socklen_t>(sizeof(server_addr.sun_family) + 1 + ::strlen(&server_addr.sun_path[1]));
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
    return listening_socket;
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
    if (s_session_count >= c_session_limit)
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

void server_t::session_listener_handler(int session_listener_epoll_fd)
{
    // Loop forever on blocking calls to epoll_wait().
    while (true)
    {
        // We need to accommodate the maximum number of session sockets, plus
        // the server shutdown eventfd.
        epoll_event events[c_session_limit + 1];
        // Block forever (we will be notified of shutdown).
        int ready_fd_count = ::epoll_wait(session_listener_epoll_fd, events, std::size(events), -1);
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
            // We only register for EPOLLIN and EPOLLRDHUP,
            // but EPOLLERR and EPOLLHUP will always be delivered.
            if (ev.events & EPOLLERR)
            {
                if (ev.data.fd == s_server_shutdown_eventfd)
                {
                    throw_system_error("Server shutdown eventfd error!");
                }
                else
                {
                    // Assume this is a session socket fd.
                    int error = 0;
                    socklen_t err_len = sizeof(error);
                    // Ignore errors getting error message and default to generic error message.
                    ::getsockopt(ev.data.fd, SOL_SOCKET, SO_ERROR, static_cast<void*>(&error), &err_len);
                    throw_system_error("Client socket error!", error);
                }
            }

            if (ev.data.fd == s_server_shutdown_eventfd)
            {
                // We can only get EPOLLIN from an eventfd.
                ASSERT_INVARIANT(ev.events == EPOLLIN, c_message_unexpected_event_type);
                consume_eventfd(s_server_shutdown_eventfd);
                return;
            }

            // At this point we assume the event must be from a client session
            // socket. If we kept all session socket fds in a data structure, we
            // could verify that the fd is expected. (Eventually we will need to
            // map session socket fds to shared session data to properly clean
            // up crashed sessions.)

            // We should only get EPOLLHUP or EPOLLRDHUP, because the only
            // operation the client performs on its session socket is close() or
            // shutdown(SHUT_WR).
            // We should get EPOLLHUP if both sides have called
            // shutdown(SHUT_WR), or the other side has called close() on their
            // socket with the last open file descriptor for that socket (if we
            // previously called close() then any future use of that socket
            // would just return EBADF).
            // We should get EPOLLRDHUP if either we have called
            // shutdown(SHUT_RD) or the other side has called shutdown(SHUT_WR).
            // The ideal shutdown sequence is for both sides to call
            // shutdown(SHUT_WR), with the second call after receiving
            // EPOLLRDHUP or EOF from read(), but we have to be resilient to
            // crashed session threads.
            ASSERT_INVARIANT(ev.events & (EPOLLHUP | EPOLLRDHUP), c_message_unexpected_event_type);
            // According to epoll documentation, closing the socket fd will
            // automatically unregister it from the epoll set.
            close_fd(ev.data.fd);
            // Decrement the global session count so this closed session does
            // not count toward the session limit.
            --s_session_count;
        }
    }
}

void server_t::client_dispatch_handler(const std::string& socket_name, int session_listener_epoll_fd)
{
    // Start listening for incoming client connections.
    int listening_socket = get_listening_socket(socket_name);
    // We close the listening socket before waiting for session threads to exit,
    // so no new sessions can be established while we wait for all session
    // threads to exit (we assume they received the same server shutdown
    // notification that we did).
    auto listener_cleanup = make_scope_guard([&] { close_fd(listening_socket); });

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
    int registered_fds[] = {listening_socket, s_server_shutdown_eventfd};
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
            epoll_event listen_ev = events[i];
            // We never register for anything but EPOLLIN,
            // but EPOLLERR will always be delivered.
            if (listen_ev.events & EPOLLERR)
            {
                if (listen_ev.data.fd == listening_socket)
                {
                    int error = 0;
                    socklen_t err_len = sizeof(error);
                    // Ignore errors getting error message and default to generic error message.
                    ::getsockopt(listening_socket, SOL_SOCKET, SO_ERROR, static_cast<void*>(&error), &err_len);
                    throw_system_error("Client socket error!", error);
                }
                else if (listen_ev.data.fd == s_server_shutdown_eventfd)
                {
                    throw_system_error("Shutdown eventfd error!");
                }
            }

            // At this point, we should only get EPOLLIN.
            ASSERT_INVARIANT(listen_ev.events == EPOLLIN, c_message_unexpected_event_type);

            if (listen_ev.data.fd == listening_socket)
            {
                int session_socket = ::accept(listening_socket, nullptr, nullptr);
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

                // Register this socket with a listener thread that (for now)
                // just waits for the client session socket to close, decrements
                // s_session_count, and closes the server session socket (later
                // iterations will clean up session state).
                epoll_event session_ev{};
                // We don't need to register for EPOLLERR or EPOLLHUP.
                session_ev.events = EPOLLIN | EPOLLRDHUP;
                session_ev.data.fd = session_socket;
                if (-1 == ::epoll_ctl(session_listener_epoll_fd, EPOLL_CTL_ADD, session_socket, &session_ev))
                {
                    throw_system_error(c_message_epoll_ctl_failed);
                }

                // Now that the socket is registered with a close handler, we
                // can safely increment the session count.
                ++s_session_count;

                // Reply with fds for the shared-memory mappings.
                int sent_fds[static_cast<size_t>(data_mapping_t::index_t::count_mappings)];
                for (const auto& data_mapping : c_data_mappings)
                {
                    sent_fds[static_cast<size_t>(data_mapping.mapping_index)] = data_mapping.fd();
                }
                // We need to send at least one byte of data to distinguish a datagram from EOF,
                // so we just send a well-known magic cookie.
                uint64_t magic = c_session_magic;

                // We assume that since the session socket is already registered
                // with the session listener thread, it will be closed by that
                // thread if the sendmsg() call fails.
                try
                {
                    send_msg_with_fds(
                        session_socket, sent_fds, std::size(sent_fds), &magic, sizeof(magic));
                }
                catch (const peer_disconnected& e)
                {
                    std::cerr << "The connecting client is now disconnected." << std::endl;
                }
                catch (const system_error& e)
                {
                    std::cerr << "Write to session socket failed: " << e.what() << std::endl;
                }
            }
            else if (listen_ev.data.fd == s_server_shutdown_eventfd)
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

    // Create session listener epoll fd.
    int session_listener_epoll_fd = ::epoll_create1(0);
    if (session_listener_epoll_fd == -1)
    {
        throw_system_error(c_message_epoll_create1_failed);
    }
    auto session_epoll_cleanup = make_scope_guard(
        [&session_listener_epoll_fd] { close_fd(session_listener_epoll_fd); });

    // Block handled signals in this thread and subsequently spawned threads, so
    // they can be handled by the dedicated signal handler thread.
    sigset_t handled_signals = get_masked_signals();

    // Per POSIX, we must use pthread_sigmask() rather than sigprocmask()
    // in a multithreaded program.
    // REVIEW: should this be SIG_SETMASK?
    ::pthread_sigmask(SIG_BLOCK, &handled_signals, nullptr);

    // Launch signal handler thread.
    // NB: This should be launched before performing any nontrivial work, to
    // ensure proper cleanup on shutdown.
    int caught_signal = 0;
    std::thread signal_handler_thread(signal_handler, handled_signals, std::ref(caught_signal));

    // Initialize all shared memory structures.
    // NB: This must be called before accepting any client connections!
    init_shared_memory();

    // Launch thread to handle session shutdown.
    // NB: This must be launched before the client dispatch thread!
    std::thread session_listener_thread(session_listener_handler, session_listener_epoll_fd);

    // Launch thread to listen for client connections.
    std::thread client_dispatch_thread(
        client_dispatch_handler, server_conf.instance_name(), session_listener_epoll_fd);

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
