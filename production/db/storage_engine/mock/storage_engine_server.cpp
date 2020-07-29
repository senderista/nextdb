/////////////////////////////////////////////
// Copyright (c) Gaia Platform LLC
// All rights reserved.
/////////////////////////////////////////////

#include "storage_engine_server.hpp"

using namespace gaia::db;
using namespace gaia::db::messages;

// from https://www.man7.org/linux/man-pages/man2/eventfd.2.html
const uint64_t server::MAX_SEMAPHORE_COUNT = 0xfffffffffffffffe;
int server::s_server_shutdown_event_fd = -1;
int server::s_connect_socket = -1;
std::mutex server::s_commit_lock;
int server::s_fd_data = -1;
se_base::offsets* server::s_shared_offsets = nullptr;
thread_local session_state_t server::s_session_state = session_state_t::DISCONNECTED;
thread_local bool server::s_session_shutdown = false;
thread_local gaia_xid_t server::s_transaction_id = -1;
constexpr server::valid_transition_t server::s_valid_transitions[];

void server::handle_connect(int*, size_t, session_event_t event, session_state_t old_state, session_state_t new_state) {
    retail_assert(event == session_event_t::CONNECT);
    // This message should only be received after the client thread was first initialized.
    retail_assert(old_state == session_state_t::DISCONNECTED && new_state == session_state_t::CONNECTED);
    // We need to reply to the client with the fds for the data/locator segments.
    FlatBufferBuilder builder;
    build_server_reply(builder, session_event_t::CONNECT, old_state, new_state, s_transaction_id);
    const int send_fds[] = {s_fd_data, s_fd_offsets};
    send_msg_with_fds(s_session_socket, send_fds, array_size(send_fds), builder.GetBufferPointer(), builder.GetSize());
}

void server::handle_begin_txn(int*, size_t, session_event_t event, session_state_t old_state, session_state_t new_state) {
    retail_assert(event == session_event_t::BEGIN_TXN);
    // This message should only be received while a transaction is in progress.
    retail_assert(old_state == session_state_t::CONNECTED && new_state == session_state_t::TXN_IN_PROGRESS);
    // Currently we don't need to alter any server-side state for opening a transaction.
    FlatBufferBuilder builder;
    s_transaction_id = allocate_transaction_id();
    build_server_reply(builder, session_event_t::CONNECT, old_state, new_state, s_transaction_id);
    send_msg_with_fds(s_session_socket, nullptr, 0, builder.GetBufferPointer(), builder.GetSize());
}

void server::handle_rollback_txn(int*, size_t, session_event_t event, session_state_t old_state, session_state_t new_state) {
    retail_assert(event == session_event_t::ROLLBACK_TXN);
    // This message should only be received while a transaction is in progress.
    retail_assert(old_state == session_state_t::TXN_IN_PROGRESS && new_state == session_state_t::CONNECTED);
    // Currently we don't need to alter any server-side state for rolling back a transaction.
}

void server::handle_commit_txn(int* fds, size_t fd_count, session_event_t event, session_state_t old_state, session_state_t new_state) {
    retail_assert(event == session_event_t::COMMIT_TXN);
    // This message should only be received while a transaction is in progress.
    retail_assert(old_state == session_state_t::TXN_IN_PROGRESS && new_state == session_state_t::TXN_COMMITTING);
    // Get the log fd and mmap it.
    retail_assert(fds && fd_count == 1);
    int fd_log = *fds;
    // Check that the log memfd was sealed for writes.
    int seals = fcntl(fd_log, F_GET_SEALS);
    if (seals == -1) {
        throw_system_error("fcntl(F_GET_SEALS) failed");
    }
    retail_assert(seals & F_SEAL_WRITE);
    // Linux won't let us create a shared read-only mapping if F_SEAL_WRITE is set,
    // which seems contrary to the manpage for fcntl(2).
    s_log = static_cast<log*>(map_fd(sizeof(log), PROT_READ, MAP_PRIVATE, fd_log, 0));
    // Close our log fd so the shared memory will be released when the client closes it.
    close(fd_log);
    // Actually commit the transaction.
    bool success = tx_commit();
    session_event_t decision = success ? session_event_t::DECIDE_TXN_COMMIT : session_event_t::DECIDE_TXN_ABORT;
    // Server-initiated state transition! (Any issues with reentrant handlers?)
    apply_transition(decision, nullptr, 0);
}

void server::handle_decide_txn(int*, size_t, session_event_t event, session_state_t old_state, session_state_t new_state) {
    retail_assert(event == session_event_t::DECIDE_TXN_COMMIT || event == session_event_t::DECIDE_TXN_ABORT);
    retail_assert(old_state == session_state_t::TXN_COMMITTING && new_state == session_state_t::CONNECTED);
    FlatBufferBuilder builder;
    build_server_reply(builder, event, old_state, new_state, s_transaction_id);
    send_msg_with_fds(s_session_socket, nullptr, 0, builder.GetBufferPointer(), builder.GetSize());
}

void server::handle_client_shutdown(int*, size_t, session_event_t event, session_state_t, session_state_t new_state) {
    retail_assert(event == session_event_t::CLIENT_SHUTDOWN);
    retail_assert(new_state == session_state_t::DISCONNECTED);
    // If this event is received, the client must have closed the write end of the socket
    // (equivalent of sending a FIN), so we need to do the same. Closing the socket
    // will send a FIN to the client, so they will read EOF and can close the socket
    // as well. We can just set the shutdown flag here, which will break out of the poll
    // loop and immediately close the socket. (If we received EOF from the client after
    // we closed our write end, then we would be calling shutdown(SHUT_WR) twice, which
    // is another reason to just close the socket.)
    s_session_shutdown = true;
}

void server::handle_server_shutdown(int*, size_t, session_event_t event, session_state_t, session_state_t new_state) {
    retail_assert(event == session_event_t::SERVER_SHUTDOWN);
    retail_assert(new_state == session_state_t::DISCONNECTING);
    // This transition should only be triggered on notification of the server shutdown event.
    // We notify the client of our intention to shut down by closing our write end of the
    // socket, and wait for them to do the same before closing the socket.
    shutdown(s_session_socket, SHUT_WR);
}

// this must be run on main thread
// see https://thomastrapp.com/blog/signal-handler-for-multithreaded-c++/
void server::run() {
    // Create eventfd shutdown event.
    // Linux is non-POSIX-compliant and sometimes marks an fd as readable
    // from select/poll/epoll even when a subsequent read would block.
    // Therefore it's safest to always set an fd to nonblocking when it's
    // used with select/poll/epoll. However, a datagram socket will return
    // EAGAIN/EWOULDBLOCK on write if there's not enough space in the send
    // buffer to write the whole message. This shouldn't be an issue as long
    // as our send buffer is larger than any message, but we should assert
    // that writes never block when the socket is writable, just to be sure.
    // We really just want the semantics of a broadcast, level-triggered
    // "waitable flag", but the closest thing to that is semaphore mode, which
    // has the unwanted semantics of a decrement on each read (the eventfd stops
    // alerting when it is decremented to zero). So as a workaround, we write
    // the largest possible value to the eventfd to ensure that it is never
    // decremented to zero, no matter how many threads read (and decrement) the
    // value.
    s_server_shutdown_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
    // Block handled signals in this thread and subsequently spawned threads.
    sigset_t handled_signals = mask_signals();
    // Launch signal handler thread.
    int caught_signal = 0;
    std::thread signal_handler_thread(signal_handler, handled_signals, std::ref(caught_signal));
    init_shared_memory();
    std::thread client_dispatch_thread(client_dispatch_handler);
    client_dispatch_thread.join();
    signal_handler_thread.join();
    // To exit with the correct status (reflecting a caught signal),
    // we need to unblock blocked signals and re-raise the signal.
    if (caught_signal != 0) {
        pthread_sigmask(SIG_UNBLOCK, &handled_signals, nullptr);
        raise(caught_signal);
    }
}