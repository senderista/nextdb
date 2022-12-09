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
static constexpr char c_message_validating_txn_should_have_been_validated_before_log_invalidation[]
    = "A committing transaction can only have its log invalidated if the transaction was concurrently validated!";
static constexpr char c_message_validating_txn_should_have_been_validated_before_conflicting_log_invalidation[]
    = "A possibly conflicting txn can only have its log invalidated if the committing transaction was concurrently validated!";
static constexpr char c_message_preceding_txn_should_have_been_validated[]
    = "A transaction with commit timestamp preceding this transaction's begin timestamp is undecided!";
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

void server_t::handle_begin_txn(
    session_event_t event, const void*, session_state_t old_state, session_state_t new_state)
{
    ASSERT_PRECONDITION(event == session_event_t::BEGIN_TXN, c_message_unexpected_event_received);

    // This message should only be received while a transaction is in progress.
    ASSERT_PRECONDITION(
        old_state == session_state_t::CONNECTED && new_state == session_state_t::TXN_IN_PROGRESS,
        c_message_current_event_is_inconsistent_with_state_transition);

    txn_begin();

    ASSERT_POSTCONDITION(
        txn_log_offset().is_valid(),
        "Transaction log offset should be valid!");

    FlatBufferBuilder builder;
    build_server_reply_info(
        builder,
        session_event_t::BEGIN_TXN,
        old_state,
        new_state,
        txn_id(),
        txn_log_offset(),
        txn_logs_for_snapshot());
    send_msg_with_fds(session_socket(), nullptr, 0, builder.GetBufferPointer(), builder.GetSize());
}

void server_t::txn_begin()
{
    s_session_context->txn_context = std::make_shared<server_transaction_context_t>();
    auto cleanup_txn_context = make_scope_guard([&] {
        s_session_context->txn_context.reset();
    });

    // Allocate a new begin_ts for this txn and initialize its metadata in the txn table.
    s_session_context->txn_context->txn_id = get_txn_metadata()->register_begin_ts();

    // The begin_ts returned by register_begin_ts() should always be valid because it
    // retries if it is concurrently sealed.
    ASSERT_INVARIANT(txn_id().is_valid(), "Begin timestamp is invalid!");

    // Ensure that there are no undecided txns in our snapshot window.
    safe_watermark_t pre_apply_watermark(watermark_type_t::pre_apply);
    validate_txns_in_range(static_cast<gaia_txn_id_t>(pre_apply_watermark) + 1, txn_id());

    get_txn_log_offsets_for_snapshot(txn_id(), txn_logs_for_snapshot());

    // Allocate the txn log offset on the server, for rollback-safety if the client session crashes.
    s_session_context->txn_context->txn_log_offset = get_logs()->allocate_log_offset(txn_id());

    // REVIEW: This exception needs to be thrown on the client!
    if (!txn_log_offset().is_valid())
    {
        throw transaction_log_allocation_failure_internal();
    }

    cleanup_txn_context.dismiss();
}

void server_t::get_txn_log_offsets_for_snapshot(
    gaia_txn_id_t begin_ts,
    std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_ids_with_log_offsets_for_snapshot)
{
    ASSERT_PRECONDITION(
        txn_ids_with_log_offsets_for_snapshot.empty(),
        "Vector passed in to get_txn_log_offsets_for_snapshot() should be empty!");

    // Take a snapshot of the post-apply watermark and scan backward from
    // begin_ts, stopping either just before the saved watermark or at the first
    // commit_ts whose log offset has been invalidated. This avoids having our
    // scan race the concurrently advancing watermark.
    safe_watermark_t post_apply_watermark(watermark_type_t::post_apply);
    for (gaia_txn_id_t ts = begin_ts - 1; ts > static_cast<gaia_txn_id_t>(post_apply_watermark); --ts)
    {
        if (get_txn_metadata()->is_commit_ts(ts))
        {
            ASSERT_INVARIANT(
                get_txn_metadata()->is_txn_decided(ts),
                "Undecided commit_ts found in snapshot window!");
            if (get_txn_metadata()->is_txn_committed(ts))
            {
                gaia_txn_id_t txn_id = get_txn_metadata()->get_begin_ts_from_commit_ts(ts);

                // Because the watermark could advance past its saved value, we
                // need to be sure that we don't send a commit_ts with a
                // possibly-reused log offset, so we increment the reference
                // count in the txn log header before sending it to the client.
                // The reference count is collocated in the same word as the
                // begin timestamp, and we verify the timestamp hasn't changed
                // when we increment the reference count, so we will never try
                // to apply a reused txn log.
                log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(ts);
                txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);
                if (txn_log->acquire_reference(txn_id))
                {
                    txn_ids_with_log_offsets_for_snapshot.emplace_back(txn_id, log_offset);
                }
                else
                {
                    // We ignore a commit_ts with an invalidated log offset
                    // because its log has already been applied to the shared
                    // locator view, so we don't need to send it to the client
                    // anyway. This means all preceding committed txns have
                    // already been applied to the shared locator view, so we
                    // can terminate the scan early.
                    break;
                }
            }
        }
    }

    // Because we scan the snapshot window backward and append log offsets to
    // the buffer, they are in reverse order.
    std::reverse(std::begin(txn_ids_with_log_offsets_for_snapshot), std::end(txn_ids_with_log_offsets_for_snapshot));
}

// Release any shared references to txn logs applied to our snapshot.
//
// REVIEW: If the client notifies us when log application has completed (e.g.,
// via a session message or a cross-process eventfd), we can release these
// references before begin_transaction() returns, to avoid delaying log GC.
// However, we cannot do this with the current implementation of index scan and
// pre-commit index update, which relies on being able to apply the same logs to
// the shared locator view that were used to construct the current txn's
// snapshot on the client.
void server_t::release_txn_log_offsets_for_snapshot()
{
    for (const auto& [txn_id, log_offset] : txn_logs_for_snapshot())
    {
        get_logs()->get_log_from_offset(log_offset)->release_reference(txn_id);
    }

    txn_logs_for_snapshot().clear();
}

void server_t::release_transaction_resources()
{
    // Ensure the cleaning of the txn context.
    auto cleanup_txn_context = make_scope_guard([&] {
        s_session_context->txn_context.reset();
    });

    // Release all references to txn logs applied to our snapshot.
    //
    // NB: this must be called before calling perform_maintenance(), because
    // otherwise we cannot GC the referenced txn logs!
    if (s_session_context->txn_context)
    {
        release_txn_log_offsets_for_snapshot();
    }

    // Update watermarks and perform associated maintenance tasks. This will
    // block new transactions on this session thread, but that is a feature, not
    // a bug, because it provides natural backpressure on clients who submit
    // long-running transactions that prevent old versions and logs from being
    // freed. This approach helps keep the system from accumulating more
    // deferred work than it can ever retire, which is a problem with performing
    // all maintenance asynchronously in the background. Allowing this work to
    // delay beginning new transactions but not delay committing the current
    // transaction seems like a good compromise.
    perform_maintenance();
}

void server_t::handle_rollback_txn(
    session_event_t event, const void*, session_state_t old_state, session_state_t new_state)
{
    ASSERT_PRECONDITION(event == session_event_t::ROLLBACK_TXN, c_message_unexpected_event_received);

    // This message should only be received while a transaction is in progress.
    ASSERT_PRECONDITION(
        old_state == session_state_t::TXN_IN_PROGRESS && new_state == session_state_t::CONNECTED,
        c_message_current_event_is_inconsistent_with_state_transition);

    ASSERT_PRECONDITION(
        txn_id().is_valid(),
        "No active transaction to roll back!");

    // Release all txn resources and mark the txn's begin_ts metadata as terminated.
    txn_rollback();
}

void server_t::handle_commit_txn(
    session_event_t event, const void*, session_state_t old_state, session_state_t new_state)
{
    ASSERT_PRECONDITION(event == session_event_t::COMMIT_TXN, c_message_unexpected_event_received);

    // This message should only be received while a transaction is in progress.
    ASSERT_PRECONDITION(
        old_state == session_state_t::TXN_IN_PROGRESS && new_state == session_state_t::TXN_COMMITTING,
        c_message_current_event_is_inconsistent_with_state_transition);

    // Actually commit the transaction.
    session_event_t decision = session_event_t::NOP;
    bool success = txn_commit();
    decision = success ? session_event_t::DECIDE_TXN_COMMIT : session_event_t::DECIDE_TXN_ABORT;

    // REVIEW: This is the only reentrant transition handler, and the only server-side state transition.
    apply_transition(decision, nullptr);
}

void server_t::handle_decide_txn(
    session_event_t event, const void*, session_state_t old_state, session_state_t new_state)
{
    ASSERT_PRECONDITION(
        (event == session_event_t::DECIDE_TXN_COMMIT
            || event == session_event_t::DECIDE_TXN_ABORT),
        c_message_unexpected_event_received);

    ASSERT_PRECONDITION(
        old_state == session_state_t::TXN_COMMITTING && new_state == session_state_t::CONNECTED,
        c_message_current_event_is_inconsistent_with_state_transition);

    auto cleanup = make_scope_guard([&] { release_transaction_resources(); });

    FlatBufferBuilder builder;
    build_server_reply_info(
        builder, event, old_state, new_state,
        txn_id(), txn_log_offset());
    send_msg_with_fds(session_socket(), nullptr, 0, builder.GetBufferPointer(), builder.GetSize());
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

    // If the session had an active txn, clean up all its resources.
    if (s_session_context->txn_context && txn_id().is_valid())
    {
        bool client_disconnected = true;
        txn_rollback(client_disconnected);
    }
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

    // REVIEW: The data mapping code should ideally call the default constructor
    // for the templated type using placement new, but this zero-initializes
    // giant arrays that are zeroed already, so we have to use ad-hoc
    // initialization where it's necessary.
    get_logs()->initialize();

    // We don't execute within a session, so we need to create our own session context.
    s_session_context = new server_session_context_t();
    auto cleanup_session_context = make_scope_guard([&] {
        delete s_session_context;
        s_session_context = nullptr;
    });

    bool initializing = true;
    init_memory_manager(initializing);

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

void server_t::deallocate_object(gaia_offset_t offset)
{
    // First extract the chunk ID from the offset, so we know which chunks are
    // candidates for deallocation.
    chunk_offset_t chunk_offset = chunk_from_offset(offset);

    chunk_manager_t chunk_manager;
    chunk_manager.load(chunk_offset);

    // We need to read the chunk version *before* we deallocate the object, to
    // ensure that the chunk hasn't already been deallocated and reused before
    // we read the version!
    chunk_version_t version = chunk_manager.get_version();

    // Cache this chunk and its current version for later deallocation.
    // REVIEW: This could be changed to use contains() after C++20.
    if (map_gc_chunks_to_versions().count(chunk_offset) == 0)
    {
        map_gc_chunks_to_versions().insert({chunk_offset, version});
    }
    else
    {
        // If this GC task already cached this chunk, then the versions must match!
        chunk_version_t cached_version = map_gc_chunks_to_versions()[chunk_offset];
        ASSERT_INVARIANT(version == cached_version, "Chunk version must match cached chunk version!");
    }

    // Delegate deallocation of the object to the chunk manager.
    chunk_manager.deallocate(offset);
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

void server_t::validate_txns_in_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts)
{
    // Scan txn table entries from start_ts to end_ts.
    // Seal any uninitialized entries and validate any undecided txns.
    for (gaia_txn_id_t ts = start_ts; ts < end_ts; ++ts)
    {
        // Fence off any txns that have allocated a commit_ts between start_ts
        // and end_ts but have not yet registered a commit_ts metadata entry in
        // the txn table.
        if (get_txn_metadata()->seal_uninitialized_ts(ts))
        {
            continue;
        }

        // Validate any undecided submitted txns.
        if (get_txn_metadata()->is_commit_ts(ts) && get_txn_metadata()->is_txn_validating(ts))
        {
            bool is_committed = validate_txn(ts);

            // Update the current txn's decided status.
            get_txn_metadata()->update_txn_decision(ts, is_committed);
        }
    }
}

gaia_txn_id_t server_t::submit_txn(gaia_txn_id_t begin_ts, log_offset_t log_offset)
{
    ASSERT_PRECONDITION(get_txn_metadata()->is_txn_active(begin_ts), "Not an active transaction!");

    ASSERT_PRECONDITION(get_logs()->is_log_offset_allocated(log_offset), "Invalid log offset!");

    // Allocate a new commit_ts and initialize its metadata with our begin_ts and log offset.
    gaia_txn_id_t commit_ts = get_txn_metadata()->register_commit_ts(begin_ts, log_offset);

    // Now update the active txn metadata.
    get_txn_metadata()->set_active_txn_submitted(begin_ts, commit_ts);

    return commit_ts;
}

// This helper method takes 2 txn logs (by offset), and determines if they have
// a non-empty intersection. We could just use std::set_intersection, but that
// outputs all elements of the intersection in a third container, while we just
// need to test for non-empty intersection (and terminate as soon as the first
// common element is found), so we write our own simple merge intersection code.
// If we ever need to return the IDs of all conflicting objects to clients (or
// just log them for diagnostics), we could use std::set_intersection.
bool server_t::txn_logs_conflict(log_offset_t offset1, log_offset_t offset2)
{
    txn_log_t* log1 = get_logs()->get_log_from_offset(offset1);
    txn_log_t* log2 = get_logs()->get_log_from_offset(offset2);

    // Perform standard merge intersection and terminate on the first conflict found.
    size_t log1_idx = 0, log2_idx = 0;
    while (log1_idx < log1->record_count && log2_idx < log2->record_count)
    {
        log_record_t* lr1 = log1->log_records + log1_idx;
        log_record_t* lr2 = log2->log_records + log2_idx;

        if (lr1->locator == lr2->locator)
        {
            return true;
        }
        else if (lr1->locator < lr2->locator)
        {
            ++log1_idx;
        }
        else
        {
            ++log2_idx;
        }
    }
    return false;
}

bool server_t::validate_txn(gaia_txn_id_t commit_ts)
{
    // Validation algorithm:

    // NB: We make two passes over the conflict window, even though only one
    // pass would suffice, because we want to avoid unnecessary validation of
    // undecided txns by skipping over them on the first pass while we check
    // committed txns for conflicts, hoping that some undecided txns will be
    // decided before the second pass. This adds some complexity (e.g., tracking
    // committed txns already tested for conflicts), but avoids unnecessary
    // duplicated work, by deferring helping other txns validate until all
    // necessary work is complete and we would be forced to block otherwise.
    //
    // 1. Find all committed transactions in conflict window, i.e., between our
    //    begin and commit timestamps, traversing from oldest to newest txns and
    //    testing for conflicts as we go, to allow as much time as possible for
    //    undecided txns to be validated, and for commit timestamps allocated
    //    within our conflict window to be registered. Seal all uninitialized
    //    timestamps we encounter, so no active txns can be submitted within our
    //    conflict window. Any submitted txns which have allocated a commit
    //    timestamp within our conflict window but have not yet registered their
    //    commit timestamp must now retry with a new timestamp. (This allows us
    //    to treat our conflict window as an immutable snapshot of submitted
    //    txns, without needing to acquire any locks.) Skip over all sealed
    //    timestamps, active txns, undecided txns, and aborted txns. Repeat this
    //    phase until no newly committed txns are discovered.
    //
    // 2. Recursively validate all undecided txns in the conflict window, from
    //    oldest to newest. Note that we cannot recurse indefinitely, because by
    //    hypothesis no undecided txns can precede our begin timestamp (because
    //    a new transaction must validate any undecided transactions with commit
    //    timestamps preceding its begin timestamp before it can proceed).
    //    However, we could duplicate work with other session threads validating
    //    their committing txns. We mitigate this by 1) deferring any recursive
    //    validation until no more committed txns have been found during a
    //    repeated scan of the conflict window, 2) tracking all txns' validation
    //    status in their commit timestamp entries, and 3) rechecking validation
    //    status for the current txn after scanning each possibly conflicting
    //    txn log (and aborting validation if it has already been validated).
    //
    // 3. Test any committed txns validated in the preceding step for conflicts.
    //    (This also includes any txns which were undecided in the first pass
    //    but committed before the second pass.)
    //
    // 4. If any of these steps finds that our write set conflicts with the
    //    write set of any committed txn, we must abort. Otherwise, we commit.
    //    In either case, we set the decided state in our commit timestamp metadata
    //    and return the commit decision to the client.
    //
    // REVIEW: Possible optimization (in the original version but removed in the
    // current code for simplicity): find the latest undecided txn which
    // conflicts with our write set. Any undecided txns later than this don't
    // need to be validated (but earlier, non-conflicting undecided txns still
    // need to be validated, because they could conflict with a later undecided
    // txn with a conflicting write set, which would abort and hence not cause
    // us to abort).

    // Because we make multiple passes over the conflict window, we need to track
    // committed txns that have already been tested for conflicts.
    // REVIEW: This codepath is latency-sensitive enough that we may want to avoid
    // dynamic allocation (we could probably just use linear search in an array).
    std::unordered_set<gaia_txn_id_t> committed_txns_tested_for_conflicts;

    // Iterate over all txns in conflict window, and test all committed txns for
    // conflicts. Repeat until no new committed txns are discovered. This gives
    // undecided txns as much time as possible to be validated by their
    // committing txn.
    bool has_found_new_committed_txn;
    do
    {
        has_found_new_committed_txn = false;
        for (gaia_txn_id_t ts = get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts) + 1; ts < commit_ts; ++ts)
        {
            // Seal all uninitialized timestamps. This marks a "fence" after which
            // any submitted txns with commit timestamps in our conflict window must
            // already be registered under their commit_ts (they must retry with a
            // new timestamp otherwise). (The sealing is necessary only on the
            // first pass, but the "uninitialized txn metadata" check is cheap enough
            // that repeating it on subsequent passes shouldn't matter.)
            if (get_txn_metadata()->seal_uninitialized_ts(ts))
            {
                continue;
            }

            if (get_txn_metadata()->is_commit_ts(ts) && get_txn_metadata()->is_txn_committed(ts))
            {
                // Remember each committed txn commit_ts so we don't test it again.
                const auto& [iter, is_new_committed_ts] = committed_txns_tested_for_conflicts.insert(ts);
                if (is_new_committed_ts)
                {
                    has_found_new_committed_txn = true;

                    // Eagerly test committed txns for conflicts to give undecided
                    // txns more time for validation (and submitted txns more time
                    // to register their commit timestamps before they're sealed).

                    // We need to acquire references on both txn logs being
                    // tested for conflicts, in case either txn log is
                    // invalidated by another thread concurrently advancing the
                    // watermark. If either log is invalidated, it must be that
                    // another thread has validated our txn, so we can exit
                    // early.

                    if (!acquire_txn_log_reference_from_commit_ts(commit_ts))
                    {
                        // If the committing txn has already had its log invalidated,
                        // then it must have already been (recursively) validated, so
                        // we can just return the commit decision.
                        ASSERT_INVARIANT(
                            get_txn_metadata()->is_txn_decided(commit_ts),
                            c_message_validating_txn_should_have_been_validated_before_log_invalidation);
                        return get_txn_metadata()->is_txn_committed(commit_ts);
                    }
                    auto release_committing_log_ref = make_scope_guard([&commit_ts] {
                        release_txn_log_reference_from_commit_ts(commit_ts);
                    });

                    if (!acquire_txn_log_reference_from_commit_ts(ts))
                    {
                        // If this committed txn already had its log
                        // invalidated, then it must be eligible for GC. But any
                        // commit_ts within the conflict window is ineligible
                        // for GC, so the committing txn must have already been
                        // (recursively) validated, and we can just return the
                        // commit decision.
                        ASSERT_INVARIANT(
                            get_txn_metadata()->is_txn_decided(commit_ts),
                            c_message_validating_txn_should_have_been_validated_before_conflicting_log_invalidation);
                        return get_txn_metadata()->is_txn_committed(commit_ts);
                    }
                    auto release_committed_log_ref = make_scope_guard([&ts] {
                        release_txn_log_reference_from_commit_ts(ts);
                    });

                    if (txn_logs_conflict(get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts), get_txn_metadata()->get_txn_log_offset_from_ts(ts)))
                    {
                        return false;
                    }
                }
            }

            // Check if another thread has already validated this txn.
            if (get_txn_metadata()->is_txn_decided(commit_ts))
            {
                return get_txn_metadata()->is_txn_committed(commit_ts);
            }
        }
    } while (has_found_new_committed_txn);

    // Validate all undecided txns, from oldest to newest. If any validated txn
    // commits, test it immediately for conflicts. Also test any committed txns
    // for conflicts if they weren't tested in the first pass.
    for (gaia_txn_id_t ts = get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts) + 1; ts < commit_ts; ++ts)
    {
        if (get_txn_metadata()->is_commit_ts(ts))
        {
            // Validate any currently undecided txn.
            if (get_txn_metadata()->is_txn_validating(ts))
            {
                // By hypothesis, there are no undecided txns with commit timestamps
                // preceding the committing txn's begin timestamp.
                ASSERT_INVARIANT(
                    ts > txn_id(),
                    c_message_preceding_txn_should_have_been_validated);

                // Recursively validate the current undecided txn.
                bool is_committed = validate_txn(ts);

                // Update the current txn's decided status.
                get_txn_metadata()->update_txn_decision(ts, is_committed);
            }

            // If a previously undecided txn has now committed, test it for conflicts.
            if (get_txn_metadata()->is_txn_committed(ts) && committed_txns_tested_for_conflicts.count(ts) == 0)
            {
                // We need to acquire references on both txn logs being
                // tested for conflicts, in case either txn log is
                // invalidated by another thread concurrently advancing the
                // watermark. If either log is invalidated, it must be that
                // another thread has validated our txn, so we can exit
                // early.

                if (!acquire_txn_log_reference_from_commit_ts(commit_ts))
                {
                    // If the committing txn has already had its log invalidated,
                    // then it must have already been (recursively) validated, so
                    // we can just return the commit decision.
                    ASSERT_INVARIANT(
                        get_txn_metadata()->is_txn_decided(commit_ts),
                        c_message_validating_txn_should_have_been_validated_before_log_invalidation);
                    return get_txn_metadata()->is_txn_committed(commit_ts);
                }
                auto release_committing_log_ref = make_scope_guard([&commit_ts] {
                    release_txn_log_reference_from_commit_ts(commit_ts);
                });

                if (!acquire_txn_log_reference_from_commit_ts(ts))
                {
                    // If this committed txn already had its log
                    // invalidated, then it must be eligible for GC. But any
                    // commit_ts within the conflict window is ineligible
                    // for GC, so the committing txn must have already been
                    // (recursively) validated, and we can just return the
                    // commit decision.
                    ASSERT_INVARIANT(
                        get_txn_metadata()->is_txn_decided(commit_ts),
                        c_message_validating_txn_should_have_been_validated_before_conflicting_log_invalidation);
                    return get_txn_metadata()->is_txn_committed(commit_ts);
                }
                auto release_committed_log_ref = make_scope_guard([&ts] {
                    release_txn_log_reference_from_commit_ts(ts);
                });

                if (txn_logs_conflict(get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts), get_txn_metadata()->get_txn_log_offset_from_ts(ts)))
                {
                    return false;
                }
            }
        }

        // Check if another thread has already validated this txn.
        if (get_txn_metadata()->is_txn_decided(commit_ts))
        {
            return get_txn_metadata()->is_txn_committed(commit_ts);
        }
    }

    // At this point, there are no undecided txns in the conflict window, and
    // all committed txns have been tested for conflicts, so we can commit.
    return true;
}

void server_t::apply_txn_log_from_ts(gaia_txn_id_t commit_ts)
{
    ASSERT_PRECONDITION(
        get_txn_metadata()->is_commit_ts(commit_ts) && get_txn_metadata()->is_txn_committed(commit_ts),
        "apply_txn_log_from_ts() must be called on the commit_ts of a committed txn!");

    // Because txn logs are only eligible for GC after they fall behind the
    // post-apply watermark, we don't need to protect this txn log from GC.
    ASSERT_INVARIANT(
        commit_ts <= get_watermarks()->get_watermark(watermark_type_t::pre_apply) &&
        commit_ts > get_watermarks()->get_watermark(watermark_type_t::post_apply),
        "Cannot apply txn log unless it is at or behind the pre-apply watermark and ahead of the post-apply watermark!");
    log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts);
    txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);

    // Ensure that the begin_ts in this metadata entry matches the txn log header.
    ASSERT_INVARIANT(
        txn_log->begin_ts() == get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts),
        "txn log begin_ts must match begin_ts reference in commit_ts metadata!");

    // Update the shared locator view with each redo version (i.e., the
    // version created or updated by the txn). This is safe as long as the
    // committed txn being applied has commit_ts older than the oldest
    // active txn's begin_ts (so it can't overwrite any versions visible in
    // that txn's snapshot). This update is non-atomic because log application
    // is idempotent and therefore a txn log can be re-applied over the same
    // txn's partially-applied log during snapshot reconstruction.
    apply_log_to_locators(s_shared_locators.data(), txn_log);
}

void server_t::gc_txn_log_from_offset(log_offset_t log_offset, bool is_committed)
{
    txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);

    // If the txn committed, we deallocate only undo versions, because the
    // redo versions may still be visible after the txn has fallen
    // behind the watermark. If the txn aborted, then we deallocate only
    // redo versions, because the undo versions may still be visible. Note
    // that we could deallocate intermediate versions (i.e., those
    // superseded within the same txn) immediately, but we do it here
    // for simplicity.
    deallocate_txn_log(txn_log, is_committed);
}

void server_t::deallocate_txn_log(txn_log_t* txn_log, bool is_committed)
{
    ASSERT_PRECONDITION(txn_log, "txn_log must be a valid address!");
    ASSERT_PRECONDITION(
        !txn_log->begin_ts().is_valid(),
        "A deallocated txn_log cannot have an owning txn!");

    for (auto log_record = txn_log->log_records; log_record < txn_log->log_records + txn_log->record_count; ++log_record)
    {
        // For committed txns, free each undo version (i.e., the version
        // superseded by an update or delete operation), using the registered
        // object deallocator (if it exists), because the redo versions may still
        // be visible but the undo versions cannot be. For aborted or
        // rolled-back txns, free only the redo versions (because the undo
        // versions may still be visible).
        // NB: we can't safely free the redo versions and txn logs of aborted
        // txns in the decide handler, because concurrently validating txns
        // might be testing the aborted txn for conflicts while they still think
        // it is undecided. The only safe place to free the redo versions and
        // txn log of an aborted txn is after it falls behind the watermark,
        // because at that point it cannot be in the conflict window of any
        // committing txn.
        gaia_offset_t offset_to_free = is_committed
            ? log_record->old_offset
            : log_record->new_offset;

        if (offset_to_free.is_valid())
        {
            deallocate_object(offset_to_free);
        }

        // For committed txns, we need to remove any deleted locators from the
        // type index. For aborted or rolled-back txns, we need to remove any
        // allocated locators from the type index.
        bool is_locator_removal_committed = is_committed && log_record->operation() == gaia_operation_t::remove;
        bool is_locator_creation_aborted = !is_committed && log_record->operation() == gaia_operation_t::create;
        if (is_locator_removal_committed || is_locator_creation_aborted)
        {
            type_index_t* type_index = get_type_index();
            bool has_succeeded = type_index->delete_locator(log_record->locator);
            ASSERT_INVARIANT(has_succeeded, "A locator cannot be deleted twice!");
        }
    }

    // We've deallocated all garbage versions, and we have no shared references,
    // so we can clear record_count, making existing log records unreachable.
    //
    // NB: We haven't yet cleared the allocation bit for this log offset, so it
    // can't be reused. Clients must call deallocate_log_offset() to make the
    // log offset available for reuse.
    //
    // REVIEW: For now, we treat existing records as garbage, overwriting them
    // when the log offset is reused. At some point we should consider
    // decommitting unused pages from the txn log segment, but
    // madvise(MADV_DONTNEED) is expensive, and so is faulting in new zeroed
    // pages on first write access, so deferring decommit for now. (A compromise
    // might be to unconditionally decommit all but the first k pages from a log
    // segment, assuming that those pages are unlikely to be reused.)

    txn_log->record_count = 0;
}

// This method is called by an active txn when it is terminated or by a
// submitted txn after it is validated. It performs a few system maintenance
// tasks, which can be deferred indefinitely with no effect on availability or
// correctness, but which are essential to maintain acceptable performance and
// resource usage.
//
// To enable reasoning about the safety of reclaiming resources which should no
// longer be needed by any present or future txns, and other invariant-based
// reasoning, we define a set of "watermarks": upper or lower bounds on the
// endpoint of a sequence of txns with some property. There are currently four
// watermarks defined: the "pre-apply" watermark, which serves as an upper bound
// on the last committed txn which was fully applied to the shared view; the
// "post-apply" watermark, which serves as a lower bound on the last committed
// txn which was fully applied to the shared view; the "post-GC" watermark,
// which serves as a lower bound on the last txn to have its resources fully
// reclaimed (i.e., its txn log and all its undo or redo versions deallocated,
// for a committed or aborted txn respectively), and the "pre-truncate"
// watermark, which serves as a boundary for safely truncating the txn table
// (i.e., decommitting physical pages corresponding to virtual address space
// that will never be read or written again).
//
// At a high level, the first pass applies all committed txn logs to the shared
// view, in order (not concurrently), and advances two watermarks marking an
// upper bound and lower bound respectively on the timestamp of the latest txn
// whose redo log has been completely applied to the shared view. The second
// pass executes GC operations concurrently on all txns which have either
// aborted or been fully applied to the shared view (and have been durably
// logged if persistence is enabled), and sets a flag on each txn when GC is
// complete. The third pass advances a watermark to the latest txn for which GC
// has completed for it and all its predecessors (marking a lower bound on the
// oldest txn whose metadata cannot yet be safely reclaimed), computes a safe
// truncation boundary using this watermark (as well as any "safe timestamps"
// reserved by threads scanning txn metadata that need to be protected from
// concurrent truncation of the txn table), and finally truncates the txn table
// at this boundary by decommitting all physical pages corresponding to virtual
// address space below the boundary.
//
// 1. We scan the interval from a snapshot of the pre-apply watermark to a
//    snapshot of the last allocated timestamp, and attempt to advance the
//    pre-apply watermark if it has the same value as the post-apply watermark,
//    and if the next txn metadata is either a sealed timestamp (we
//    seal all uninitialized entries during the scan), a commit_ts that is
//    decided, or a begin_ts that is terminated or submitted with a decided
//    commit_ts. If we successfully advanced the watermark and the current entry
//    is a committed commit_ts, then we can apply its redo log to the shared
//    view. After applying it (or immediately after advancing the pre-apply
//    watermark if the current timestamp is not a committed commit_ts), we
//    advance the post-apply watermark to the same timestamp. (Because we "own"
//    the current txn metadata after a successful CAS on the pre-apply
//    watermark, we can advance the post-apply watermark without a CAS.) Because
//    the pre-apply watermark can only move forward, updates to the shared view
//    are applied in timestamp order, and because the pre-apply watermark can only
//    be advanced if the post-apply watermark has caught up with it (which can
//    only be the case for a committed commit_ts if the redo log has been fully
//    applied), updates to the shared view are never applied concurrently.
//
// 2. We scan the interval from a snapshot of the post-GC watermark to a
//    snapshot of the post-apply watermark. If the current timestamp is not a
//    commit_ts, we continue the scan. Otherwise we check if its txn log is
//    invalidated. If so, then we know that GC is in progress or complete, so we
//    continue the scan. If persistence is enabled then we also check the
//    durable flag on the current txn metadata and abort the scan if it is
//    not set (to avoid freeing any redo versions that are being applied to the
//    write-ahead log). Otherwise we try to invalidate its txn log. If
//    invalidation fails, we abort the pass to avoid contention, otherwise we GC
//    this txn using the invalidated txn log, set the TXN_GC_COMPLETE flag, and
//    continue the scan.
//
// 3. Again, we scan the interval from a snapshot of the post-GC watermark to a
//    snapshot of the post-apply watermark. If the current entry is a begin_ts
//    or a commit_ts with TXN_GC_COMPLETE set, we try to advance the post-GC
//    watermark. If that fails (or the watermark cannot be advanced because the
//    commit_ts has TXN_GC_COMPLETE unset), we abort the pass. If we complete
//    this pass, then we calculate a "safe truncation timestamp" and attempt to
//    advance the pre-truncate watermark to that timestamp. If we successfully
//    advanced the pre-truncate watermark, then we calculate the number of pages
//    between the previous pre-truncate watermark value and its new value; if
//    this count exceeds zero then we decommit all such pages using madvise(2).
//    (It is possible for multiple GC tasks to concurrently or repeatedly
//    decommit the same pages, but madvise(2) is idempotent and
//    concurrency-safe.)
void server_t::perform_maintenance()
{
    // Attempt to apply all txn logs to the shared view, from the last value of
    // the post-apply watermark to the latest committed txn.
    apply_txn_logs_to_shared_view();

    // Attempt to reclaim the resources of all txns from the post-GC watermark
    // to the post-apply watermark.
    gc_applied_txn_logs();

    // Find a timestamp at which we can safely truncate the txn table, advance
    // the pre-truncate watermark to that timestamp, and truncate the txn table
    // at the highest page boundary less than the pre-truncate watermark.
    truncate_txn_table();
}

void server_t::apply_txn_logs_to_shared_view()
{
    // First get a snapshot of the timestamp counter for an upper bound on
    // the scan (we don't know yet if this is a begin_ts or commit_ts).
    gaia_txn_id_t last_allocated_ts = get_last_txn_id();

    // Now get a snapshot of the pre-apply watermark,
    // for a lower bound on the scan.
    safe_watermark_t pre_apply_watermark(watermark_type_t::pre_apply);

    // Scan from the saved pre-apply watermark to the last known timestamp,
    // and apply all committed txn logs from the longest prefix of decided
    // txns that does not overlap with the conflict window of any undecided
    // txn. Advance the pre-apply watermark before applying the txn log
    // of a committed txn, and advance the post-apply watermark after
    // applying the txn log.
    for (gaia_txn_id_t ts = static_cast<gaia_txn_id_t>(pre_apply_watermark) + 1; ts <= last_allocated_ts; ++ts)
    {
        // We need to seal uninitialized entries as we go along, so that we
        // don't miss any active begin_ts or committed commit_ts entries.
        //
        // We continue processing sealed timestamps
        // so that we can advance the pre-apply watermark over them.
        get_txn_metadata()->seal_uninitialized_ts(ts);

        // If this is a commit_ts, we cannot advance the watermark unless it's
        // decided.
        if (get_txn_metadata()->is_commit_ts(ts) && get_txn_metadata()->is_txn_validating(ts))
        {
            break;
        }

        // The watermark cannot be advanced past any begin_ts whose txn is not
        // either in the TXN_TERMINATED state or in the TXN_SUBMITTED state with
        // its commit_ts in the TXN_DECIDED state. This means that the watermark
        // can never advance into the conflict window of an undecided txn.
        if (get_txn_metadata()->is_begin_ts(ts))
        {
            if (get_txn_metadata()->is_txn_active(ts))
            {
                break;
            }

            if (get_txn_metadata()->is_txn_submitted(ts)
                && get_txn_metadata()->is_txn_validating(get_txn_metadata()->get_commit_ts_from_begin_ts(ts)))
            {
                break;
            }
        }

        // We can only advance the pre-apply watermark if the post-apply
        // watermark has caught up to it (this ensures that txn logs cannot be
        // applied concurrently to the shared view; they are already applied in
        // order because the pre-apply watermark advances in order). This is
        // exactly equivalent to a lock implemented by a CAS attempting to set a
        // boolean. When a thread successfully advances the pre-apply watermark,
        // it acquires the lock (because this makes the pre-apply and post-apply
        // watermarks unequal, so no other thread will attempt to advance the
        // pre-apply watermark, and a thread can only advance the post-apply
        // watermark after advancing the pre-apply watermark), and when the
        // owning thread subsequently advances the post-apply watermark, it
        // releases the lock (because all other threads observe that the
        // pre-apply and post-apply watermarks are equal again and can attempt
        // to advance the pre-apply watermark). This "inchworm" algorithm
        // (so-called because, like an inchworm, the "front" can only advance
        // after the "back" has caught up) is thus operationally equivalent to
        // locking on a reserved bit flag (call it TXN_GC_ELIGIBLE) in the
        // txn metadata, but allows us to avoid reserving another scarce bit
        // for this purpose.

        // The current timestamp in the scan is guaranteed to be positive due to
        // the loop precondition.
        gaia_txn_id_t prev_ts = ts - 1;

        // This thread must have observed both the pre- and post-apply
        // watermarks to be equal to the previous timestamp in the scan in order
        // to advance the pre-apply watermark to the current timestamp in the
        // scan. This means that any thread applying a txn log at the previous
        // timestamp must have finished applying the log, so we can safely apply
        // the log at the current timestamp.
        //
        // REVIEW: These loads could be relaxed, because a stale read could only
        // result in premature abort of the scan.
        if (get_watermarks()->get_watermark(watermark_type_t::pre_apply) != prev_ts
            || get_watermarks()->get_watermark(watermark_type_t::post_apply) != prev_ts)
        {
            break;
        }

        if (!get_watermarks()->advance_watermark(watermark_type_t::pre_apply, ts))
        {
            // If another thread has already advanced the watermark ahead of
            // this ts, we abort advancing it further.
            ASSERT_INVARIANT(
                get_watermarks()->get_watermark(watermark_type_t::pre_apply) > static_cast<gaia_txn_id_t>(pre_apply_watermark),
                "The watermark must have advanced if advance_watermark() failed!");

            break;
        }

        if (get_txn_metadata()->is_commit_ts(ts))
        {
            ASSERT_INVARIANT(
                get_txn_metadata()->is_txn_decided(ts),
                "The watermark should not be advanced to an undecided commit_ts!");

            if (get_txn_metadata()->is_txn_committed(ts))
            {
                // If a new txn starts after or while we apply this txn log to
                // the shared view, but before we advance the post-apply
                // watermark, it will re-apply some of our updates to its
                // snapshot of the shared view, but that is benign because log
                // replay is idempotent (as long as logs are applied in
                // timestamp order).
                apply_txn_log_from_ts(ts);
            }
        }

        // Now we advance the post-apply watermark to catch up with the pre-apply watermark.
        // REVIEW: Because no other thread can concurrently advance the post-apply watermark,
        // we don't need a full CAS here.
        bool has_advanced_watermark = get_watermarks()->advance_watermark(watermark_type_t::post_apply, ts);

        // No other thread should be able to advance the post-apply watermark,
        // because only one thread can advance the pre-apply watermark to this
        // timestamp.
        ASSERT_INVARIANT(has_advanced_watermark, "Couldn't advance the post-apply watermark!");
    }
}

void server_t::gc_applied_txn_logs()
{
    // Ensure we clean up our cached chunk IDs when we exit this task.
    auto cleanup_fd = make_scope_guard([&] { map_gc_chunks_to_versions().clear(); });

    // Get a snapshot of the post-apply watermark, for an upper bound on the scan.
    safe_watermark_t post_apply_watermark(watermark_type_t::post_apply);

    // Get a snapshot of the post-GC watermark, for a lower bound on the scan.
    safe_watermark_t post_gc_watermark(watermark_type_t::post_gc);

    // Scan from the post-GC watermark to the post-apply watermark, executing GC
    // on any commit_ts if the txn log is valid (and the durable flag is set if
    // persistence is enabled). (If we fail to invalidate the txn log, we abort
    // the scan to avoid contention.) When GC is complete, set the
    // TXN_GC_COMPLETE flag on the txn metadata and continue.
    for (
        gaia_txn_id_t ts = static_cast<gaia_txn_id_t>(post_gc_watermark) + 1;
        ts <= static_cast<gaia_txn_id_t>(post_apply_watermark);
        ++ts)
    {
        ASSERT_INVARIANT(
            !get_txn_metadata()->is_uninitialized_ts(ts),
            "All uninitialized txn table entries should be sealed!");

        ASSERT_INVARIANT(
            !(get_txn_metadata()->is_begin_ts(ts) && get_txn_metadata()->is_txn_active(ts)),
            "The watermark should not be advanced to an active begin_ts!");

        if (get_txn_metadata()->is_commit_ts(ts))
        {
            // If persistence is enabled, then we also need to check that
            // TXN_PERSISTENCE_COMPLETE is set (to avoid having redo versions
            // deallocated while they're being persisted).
            if (s_server_conf.is_persistence_enabled() && !get_txn_metadata()->is_txn_durable(ts))
            {
                break;
            }

            log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(ts);
            ASSERT_INVARIANT(log_offset.is_valid(), "A commit_ts txn metadata entry must have a valid log offset!");
            txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);
            gaia_txn_id_t begin_ts = get_txn_metadata()->get_begin_ts_from_commit_ts(ts);

            // If our begin_ts doesn't match the current begin_ts, the txn log
            // has already been invalidated (and possibly reused), so some other
            // thread has initiated GC of this txn log.
            if (txn_log->begin_ts() != begin_ts)
            {
                continue;
            }

            // Try to acquire ownership of this txn log by invalidating it.
            // Invalidation can fail only if it has already occurred or there
            // are outstanding shared references. In the first case, we should
            // abort the scan to avoid contention (because another thread must
            // have invalidated the txn log after the check we just performed).
            // In the second case, we should abort the scan because we cannot
            // make progress.
            if (!txn_log->invalidate(begin_ts))
            {
                break;
            }

            // Because we invalidated the log offset, we need to ensure it is
            // deallocated so it can be reused.
            auto cleanup_log_offset = make_scope_guard([&log_offset] { get_logs()->deallocate_log_offset(log_offset); });

            // Deallocate obsolete object versions and update index entries.
            gc_txn_log_from_offset(log_offset, get_txn_metadata()->is_txn_committed(ts));

            // We need to mark this txn metadata TXN_GC_COMPLETE to allow the
            // post-GC watermark to advance.
            bool has_set_metadata = get_txn_metadata()->set_txn_gc_complete(ts);

            // If persistence is enabled, then this commit_ts must have been
            // marked durable before we advanced the watermark, and no other
            // thread can set TXN_GC_COMPLETE after we invalidate the txn log, so
            // it should not be possible for this CAS to fail.
            ASSERT_INVARIANT(has_set_metadata, "Txn metadata cannot change after we invalidate the txn log!");
        }
    }

    // Now deallocate any unused chunks that have already been retired.
    // TODO: decommit unused pages in used chunks.
    for (auto& entry : map_gc_chunks_to_versions())
    {
        chunk_offset_t chunk_offset = entry.first;
        chunk_version_t chunk_version = entry.second;
        chunk_manager_t chunk_manager;
        chunk_manager.load(chunk_offset);
        chunk_manager.try_deallocate_chunk(chunk_version);
        chunk_manager.release();
    }

    // Finally, catch up the post-GC watermark.
    // Unlike log application, we don't try to perform GC and advance the
    // post-GC watermark in a single scan, because log application is strictly
    // sequential, while GC is sequentially initiated but concurrently executed.
    update_post_gc_watermark();
}

void server_t::update_post_gc_watermark()
{
    // Get a snapshot of the post-apply watermark, for an upper bound on the scan.
    safe_watermark_t post_apply_watermark(watermark_type_t::post_apply);

    // Get a snapshot of the post-GC watermark, for a lower bound on the scan.
    safe_watermark_t post_gc_watermark(watermark_type_t::post_gc);

    // Scan from the post-GC watermark to the post-apply watermark, advancing
    // the post-GC watermark to any commit_ts marked TXN_GC_COMPLETE, or any
    // begin_ts unless it is marked TXN_SUBMITTED and its commit_ts is not
    // marked TXN_GC_COMPLETE. (We need to preserve begin_ts entries for
    // commit_ts entries that have not completed GC, in order to allow index
    // entries to safely dereference a commit_ts entry from its begin_ts entry.)
    // If the post-GC watermark cannot be advanced to the current timestamp,
    // abort the scan.
    for (
        gaia_txn_id_t ts = static_cast<gaia_txn_id_t>(post_gc_watermark) + 1;
        ts <= static_cast<gaia_txn_id_t>(post_apply_watermark);
        ++ts)
    {
        ASSERT_INVARIANT(
            !get_txn_metadata()->is_uninitialized_ts(ts),
            "All uninitialized txn table entries should be sealed!");

        if (get_txn_metadata()->is_begin_ts(ts))
        {
            ASSERT_INVARIANT(
                !get_txn_metadata()->is_txn_active(ts),
                "The pre-apply watermark should not be advanced to an active begin_ts!");

            // We can only advance the post-GC watermark to a submitted begin_ts
            // if its commit_ts is marked TXN_GC_COMPLETE.
            if (get_txn_metadata()->is_txn_submitted(ts)
                && !get_txn_metadata()->is_txn_gc_complete(get_txn_metadata()->get_commit_ts_from_begin_ts(ts)))
            {
                break;
            }
        }

        if (get_txn_metadata()->is_commit_ts(ts))
        {
            ASSERT_INVARIANT(
                get_txn_metadata()->is_txn_decided(ts),
                "The pre-apply watermark should not be advanced to an undecided commit_ts!");

            // We can only advance the post-GC watermark to a commit_ts if it is
            // marked TXN_GC_COMPLETE.
            if (!get_txn_metadata()->is_txn_gc_complete(ts))
            {
                break;
            }
        }

        if (!get_watermarks()->advance_watermark(watermark_type_t::post_gc, ts))
        {
            // If another thread has already advanced the post-GC watermark
            // ahead of this ts, we abort advancing it further.
            ASSERT_INVARIANT(
                get_watermarks()->get_watermark(watermark_type_t::post_gc) > static_cast<gaia_txn_id_t>(post_gc_watermark),
                "The watermark must have advanced if advance_watermark() failed!");

            break;
        }
    }
}

void server_t::truncate_txn_table()
{
    // Get a snapshot of the pre-truncate watermark before advancing it.
    gaia_txn_id_t prev_pre_truncate_watermark = get_watermarks()->get_watermark(watermark_type_t::pre_truncate);

    // Compute a safe truncation timestamp.
    gaia_txn_id_t new_pre_truncate_watermark = safe_ts_t::get_safe_truncation_ts();

    // Abort if the safe truncation timestamp does not exceed the current
    // pre-truncate watermark.
    // NB: It is expected that the safe truncation timestamp can be behind
    // the pre-truncate watermark, because some published (but not yet
    // validated) timestamps may have been behind the pre-truncate watermark
    // when they were published (and will later fail validation).
    if (new_pre_truncate_watermark <= prev_pre_truncate_watermark)
    {
        return;
    }

    // Try to advance the pre-truncate watermark.
    if (!get_watermarks()->advance_watermark(watermark_type_t::pre_truncate, new_pre_truncate_watermark))
    {
        // Abort if another thread has concurrently advanced the
        // pre-truncate watermark, to avoid contention.
        ASSERT_INVARIANT(
            get_watermarks()->get_watermark(watermark_type_t::pre_truncate) > prev_pre_truncate_watermark,
            "The watermark must have advanced if advance_watermark() failed!");

        return;
    }

    // We advanced the pre-truncate watermark, so actually truncate the txn
    // table by decommitting its unused physical pages. Because this
    // operation is concurrency-safe and idempotent, it can be done without
    // mutual exclusion.
    // REVIEW: The previous logic could also be used to safely advance the
    // "head" pointer if the txn table were implemented as a circular
    // buffer.

    // Calculate the number of pages between the previously read pre-truncate
    // watermark and our safe truncation timestamp. If the result exceeds zero,
    // then decommit all such pages.
    char* prev_page_start_address = get_txn_metadata_page_address_from_ts(prev_pre_truncate_watermark);
    char* new_page_start_address = get_txn_metadata_page_address_from_ts(new_pre_truncate_watermark);

    // Check for overflow.
    ASSERT_INVARIANT(
        prev_page_start_address <= new_page_start_address,
        "The new pre-truncate watermark must start on the same or later page than the previous pre-truncate watermark!");

    size_t pages_to_decommit_count = (new_page_start_address - prev_page_start_address) / c_page_size_in_bytes;
    if (pages_to_decommit_count > 0)
    {
        // MADV_FREE seems like the best fit for our needs, since it allows
        // the OS to lazily reclaim decommitted pages. (If we move the txn
        // table to a shared mapping (e.g. memfd), then we'll need to switch
        // to MADV_REMOVE.)
        //
        // REVIEW: MADV_FREE makes it impossible to monitor RSS usage, so
        // use MADV_DONTNEED unless we actually need better performance (see
        // https://github.com/golang/go/issues/42330 for a discussion of
        // these issues). Moreover, we will never reuse the decommitted
        // virtual memory, so using MADV_FREE wouldn't save the cost of hard
        // page faults on first access to decommitted pages.
        if (-1 == ::madvise(prev_page_start_address, pages_to_decommit_count * c_page_size_in_bytes, MADV_DONTNEED))
        {
            throw_system_error("madvise(MADV_DONTNEED) failed!");
        }
    }
}

char* server_t::get_txn_metadata_page_address_from_ts(gaia_txn_id_t ts)
{
    char* txn_metadata_map_base_address = reinterpret_cast<char*>(get_txn_metadata());
    size_t ts_entry_byte_offset = ts * sizeof(txn_metadata_entry_t);
    size_t ts_entry_page_byte_offset = (ts_entry_byte_offset / c_page_size_in_bytes) * c_page_size_in_bytes;
    char* ts_entry_page_address = txn_metadata_map_base_address + ts_entry_page_byte_offset;
    return ts_entry_page_address;
}

void server_t::txn_rollback(bool client_disconnected)
{
    auto cleanup = make_scope_guard([&] { release_transaction_resources(); });

    // Directly free the final allocation recorded in chunk metadata if it is
    // absent from the txn log (due to a crashed session), and retire the chunk
    // owned by the client session when it crashed.
    if (client_disconnected)
    {
        // TODO: Implement this logic correctly by tracking the current chunk ID
        // in shared session state. For now, chunks owned by a crashed session
        // will never be retired (and therefore can never be reallocated).
        // Deallocation of object versions in these orphaned chunks will still
        // succeed, though, so GC correctness is unaffected.
    }

    // Free any deallocated objects.
    // TODO: ensure that we deallocate this log offset (GC will never see it)!
    txn_log_t* txn_log = get_txn_log();

    // Release ownership as precondition for GC.
    bool success = txn_log->invalidate(txn_id());
    ASSERT_POSTCONDITION(success, "Unsubmitted txn log cannot have any shared references!");

    // We don't need to go through the full GC path because this txn log was never submitted.
    bool is_committed = false;
    gc_txn_log_from_offset(txn_log_offset(), is_committed);

    // Make txn log offset available for reuse.
    get_logs()->deallocate_log_offset(txn_log_offset());

    // Set our txn status to TXN_TERMINATED.
    // This allows GC to proceed past this txn's begin_ts.
    get_txn_metadata()->set_active_txn_terminated(txn_id());
}

// Sort all txn log records by locator. This enables us to use fast binary
// search and merge intersection algorithms for conflict detection.
void server_t::sort_log()
{
    // We use `log_record_t.sequence` as a secondary sort key to preserve the
    // temporal order of multiple updates to the same locator.
    txn_log_t* txn_log = get_txn_log();
    std::sort(
        &txn_log->log_records[0],
        &txn_log->log_records[txn_log->record_count],
        [](const log_record_t& lhs, const log_record_t& rhs) {
            auto lhs_pair = std::pair{lhs.locator, lhs.sequence};
            auto rhs_pair = std::pair{rhs.locator, rhs.sequence};
            return lhs_pair < rhs_pair;
        });
}

// This method returns true for a commit decision and false for an abort decision.
bool server_t::txn_commit()
{
    // Before registering the log, sort by locator for fast conflict detection.
    sort_log();

    // Obtain a safe_ts for our begin_ts, to prevent recursive validation from
    // allowing the pre-truncate watermark to advance past our begin_ts into the
    // conflict window. This ensures that any thread (including recursive
    // validators) can safely read any txn metadata within the conflict window
    // of this txn, until the commit decision is returned to the client. NB:
    // This MUST be done before acquiring a commit_ts!
    //
    // We do not catch safe_ts_failure here because it should be impossible for
    // the post-GC watermark to advance past an active begin_ts (our begin_ts is
    // not marked SUBMITTED until we acquire a commit_ts). If a safe_ts_failure
    // exception is thrown here, that indicates a bug.
    //
    // BUGBUG: We have observed a safe_ts_failure exception thrown here! This is
    // a critical correctness issue: it indicates either a bug in the
    // implementation or in the algorithm itself. It is imperative to specify
    // and model-check the safe txn metadata truncation algorithm!
    safe_ts_t safe_begin_ts(txn_id());

    // Register the committing txn under a new commit timestamp.
    gaia_txn_id_t commit_ts = submit_txn(
        txn_id(), txn_log_offset());

    // Validate the txn against all other committed txns in the conflict window.
    bool is_committed = validate_txn(commit_ts);

    // Update the txn metadata with our commit decision.
    get_txn_metadata()->update_txn_decision(commit_ts, is_committed);

    // TODO: Persist the commit decision.
    // For now, just set the durable flag unconditionally after validation.
    // REVIEW: We can return a decision to the client asynchronously with the
    // decision being persisted (because the decision can be reconstructed from
    // the durable log itself, without the decision record).
    if (s_server_conf.is_persistence_enabled())
    {
        // Mark txn as durable in metadata so we can GC the txn log. We only
        // mark it durable after validation to simplify the state transitions:
        // TXN_VALIDATING -> TXN_DECIDED -> TXN_DURABLE.

        // TEST: We could inject a random sleep here to simulate persistence
        // latency and test our GC logic.
        get_txn_metadata()->set_txn_durable(commit_ts);
    }

    return is_committed;
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

bool server_t::acquire_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts)
{
    ASSERT_PRECONDITION(get_txn_metadata()->is_commit_ts(commit_ts), "Not a commit timestamp!");
    gaia_txn_id_t begin_ts = get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts);
    log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts);
    return acquire_txn_log_reference(log_offset, begin_ts);
}

void server_t::release_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts)
{
    ASSERT_PRECONDITION(get_txn_metadata()->is_commit_ts(commit_ts), "Not a commit timestamp!");
    gaia_txn_id_t begin_ts = get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts);
    log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts);
    release_txn_log_reference(log_offset, begin_ts);
}
