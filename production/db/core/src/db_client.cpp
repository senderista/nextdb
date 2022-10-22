////////////////////////////////////////////////////
// Copyright (c) Gaia Platform Authors
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include "db_client.hpp"

#include <format>
#include <functional>
#include <optional>
#include <thread>

#include <flatbuffers/flatbuffers.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/scope_guard.hpp"
#include "gaia_internal/common/socket_helpers.hpp"
#include "gaia_internal/common/system_error.hpp"
#include "gaia_internal/db/db_types.hpp"

#include "db_helpers.hpp"
#include "db_internal_types.hpp"

using namespace gaia::common;
using namespace gaia::db;
using namespace gaia::db::memory_manager;
using namespace gaia::db::messages;
using namespace flatbuffers;
using namespace scope_guard;

static void build_client_request(
    FlatBufferBuilder& builder,
    session_event_t event)
{
    builder.ForceDefaults(true);
    flatbuffers::Offset<client_request_t> client_request;
    client_request = Createclient_request_t(builder, event);
    auto message = Createmessage_t(builder, any_message_t::request, client_request.Union());
    builder.Finish(message);
}

void client_t::txn_cleanup()
{
    // Ensure the cleaning of the txn context.
    auto cleanup_txn_context = make_scope_guard([&] {
        s_session_context->txn_context->clear();
    });

    // Destroy the locator mapping.
    private_locators().close();
}

int client_t::get_session_socket(const std::string& socket_name)
{
    // Unlike the session socket on the server, this socket must be blocking,
    // because we don't read within a multiplexing poll loop.
    int session_socket = ::socket(PF_UNIX, SOCK_SEQPACKET, 0);
    if (session_socket == -1)
    {
        throw_system_error("Socket creation failed!");
    }

    auto cleanup_session_socket = make_scope_guard([&session_socket] { close_fd(session_socket); });

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

    // The socket name is not null-terminated in the address structure, but
    // we need to add an extra byte for the null byte prefix.
    socklen_t server_addr_size = sizeof(server_addr.sun_family) + 1 + strlen(&server_addr.sun_path[1]);
    if (-1 == ::connect(session_socket, reinterpret_cast<sockaddr*>(&server_addr), server_addr_size))
    {
        throw_system_error("Connect failed!");
    }

    cleanup_session_socket.dismiss();
    return session_socket;
}

// This method establishes a session with the server on the calling thread,
// which serves as a transaction context in the client and a container for
// client state on the server. The server always passes fds for the data and
// locator shared memory segments, but we only use them if this is the client
// process's first call to create_session(), because they are stored globally
// rather than per-session. (The connected socket is stored per-session.)
//
// REVIEW: There is currently no way for the client to be asynchronously notified
// when the server closes the session (e.g., if the server process shuts down).
// Throwing an asynchronous exception on the session thread may not be possible,
// and would be difficult to handle properly even if it were possible.
// In any case, send_msg_with_fds()/recv_msg_with_fds() already throws a
// peer_disconnected exception when the other end of the socket is closed.
void client_t::begin_session()
{
    // Fail if a session already exists on this thread.
    verify_no_session();

    ASSERT_PRECONDITION(
        !s_session_context,
        "Session context should not be initialized already at the start of a new session!");

    s_session_context = new client_session_context_t();
    auto cleanup_session_context = make_scope_guard([&] {
        delete s_session_context;
        s_session_context = nullptr;
    });

    // Validate shared memory mapping definitions and assert that mappings are not made yet.
    data_mapping_t::validate(data_mappings());
    for (const auto& data_mapping : data_mappings())
    {
        ASSERT_INVARIANT(!data_mapping.is_set(), "Segment is already mapped!");
    }

    // Connect to the server's well-known socket name, and ask it
    // for the data and locator shared memory segment fds.
    try
    {
        s_session_context->session_socket = get_session_socket(c_default_instance_name);
    }
    catch (const system_error& e)
    {
        throw server_connection_failed_internal(e.what(), e.get_errno());
    }

    // Send the server the connection request.
    FlatBufferBuilder builder;
    build_client_request(builder, session_event_t::CONNECT);

    client_messenger_t client_messenger;

    // If we receive ECONNRESET from the server, we assume that the session
    // limit was exceeded.
    // REVIEW: distinguish authentication failure from "session limit exceeded"
    // (authentication failure will also result in ECONNRESET, but
    // authentication is currently disabled in the server).
    try
    {
        client_messenger.send_and_receive(
            session_socket(), nullptr, 0, builder,
            static_cast<size_t>(data_mapping_t::index_t::count_mappings));
    }
    catch (const gaia::common::peer_disconnected&)
    {
        throw session_limit_exceeded_internal();
    }

    // Set up scope guards for the fds.
    // The locators fd needs to be kept around, so its scope guard will be dismissed at the end of this scope.
    // The other fds are not needed, so they'll get their own scope guard to clean them up.
    int fd_locators = client_messenger.received_fd(static_cast<size_t>(data_mapping_t::index_t::locators));
    auto cleanup_fd_locators = make_scope_guard([&fd_locators] { close_fd(fd_locators); });
    auto cleanup_fd_others = make_scope_guard([&] {
        for (const auto& data_mapping : data_mappings())
        {
            if (data_mapping.mapping_index != data_mapping_t::index_t::locators)
            {
                int fd = client_messenger.received_fd(static_cast<size_t>(data_mapping.mapping_index));
                close_fd(fd);
            }
        } });

    session_event_t event = client_messenger.server_reply()->event();
    ASSERT_INVARIANT(event == session_event_t::CONNECT, c_message_unexpected_event_received);

    // Set up the shared-memory mappings.
    // The locators mapping will be performed manually, so skip its information in the mapping table.
    for (const auto& data_mapping : data_mappings())
    {
        if (data_mapping.mapping_index != data_mapping_t::index_t::locators)
        {
            int fd = client_messenger.received_fd(static_cast<size_t>(data_mapping.mapping_index));
            data_mapping.open(fd);
        }
    }

    // Set up the private locator segment fd.
    s_session_context->fd_locators = fd_locators;

    init_memory_manager();

    cleanup_fd_locators.dismiss();
    cleanup_session_context.dismiss();
}

void client_t::end_session()
{
    verify_session_active();
    verify_no_txn();

    // Ensure the cleaning of the session context.
    auto cleanup_session_context = make_scope_guard([&] {
        delete s_session_context;
        s_session_context = nullptr;
    });
}

void client_t::begin_transaction()
{
    verify_session_active();
    verify_no_txn();

    ASSERT_PRECONDITION(
        s_session_context,
        "Session context should be initialized already at the start of a new transaction!");

    ASSERT_PRECONDITION(
        s_session_context->txn_context,
        "Transaction context should be allocated already at the start of a new transaction!");

    ASSERT_PRECONDITION(
        !(s_session_context->txn_context->initialized()),
        "Transaction context should not be initialized already at the start of a new transaction!");

    // Clean up all transaction-local session state.
    auto cleanup = make_scope_guard(txn_cleanup);

    // Map a private COW view of the locator shared memory segment.
    ASSERT_PRECONDITION(!private_locators().is_set(), "Locators segment is already mapped!");
    bool manage_fd = false;
    bool is_shared = false;
    private_locators().open(s_session_context->fd_locators, manage_fd, is_shared);

    // Send a TXN_BEGIN request to the server and receive a new txn ID, the
    // offset of a new txn log, and txn log offsets for all committed txns
    // within the snapshot window.
    FlatBufferBuilder builder;
    build_client_request(builder, session_event_t::BEGIN_TXN);

    client_messenger_t client_messenger;
    client_messenger.send_and_receive(session_socket(), nullptr, 0, builder);

    // Extract the transaction id and cache it; it needs to be reset for the next transaction.
    const transaction_info_t* txn_info = client_messenger.server_reply()->data_as_transaction_info();
    s_session_context->txn_context->txn_id = txn_info->transaction_id();
    s_session_context->txn_context->txn_log_offset = txn_info->transaction_log_offset();
    ASSERT_INVARIANT(
        txn_id().is_valid(),
        "Begin timestamp should not be invalid!");
    ASSERT_INVARIANT(
        txn_log_offset().is_valid(),
        "Txn log offset should not be invalid!");

    // Apply all txn logs received from the server to our snapshot, in order.
    const auto transaction_logs_to_apply = txn_info->transaction_logs_to_apply();
    for (const auto txn_log_info : *transaction_logs_to_apply)
    {
        // REVIEW: After snapshot reuse is enabled, skip applying logs with
        // txn_log_info.commit_timestamp <= s_latest_applied_commit_ts.
        apply_log_from_offset(private_locators().data(), txn_log_info->log_offset());
    }

    cleanup.dismiss();
}

void client_t::rollback_transaction()
{
    verify_txn_active();

    ASSERT_PRECONDITION(
        s_session_context->txn_context->initialized(),
        "Transaction context should be initialized already at the end of a transaction!");

    // Clean up all transaction-local session state.
    auto cleanup = make_scope_guard(txn_cleanup);

    FlatBufferBuilder builder;
    build_client_request(builder, session_event_t::ROLLBACK_TXN);
    send_msg_with_fds(session_socket(), nullptr, 0, builder.GetBufferPointer(), builder.GetSize());
}

// This method returns void on a commit decision and throws on an abort decision.
// It sends a message to the server containing the fd of this txn's log segment and
// will block waiting for a reply from the server.
void client_t::commit_transaction()
{
    verify_txn_active();

    ASSERT_PRECONDITION(
        s_session_context->txn_context->initialized(),
        "Transaction context should be initialized already at the end of a transaction!");

    // This optimization to treat committing a read-only txn as a rollback
    // allows us to avoid any special cases in the server for empty txn logs.
    if (get_txn_log()->record_count == 0)
    {
        rollback_transaction();
        return;
    }

    // Clean up all transaction-local session state when we exit.
    auto cleanup = make_scope_guard(txn_cleanup);

    // Send the server the commit message.
    FlatBufferBuilder builder;
    build_client_request(builder, session_event_t::COMMIT_TXN);

    client_messenger_t client_messenger;
    client_messenger.send_and_receive(session_socket(), nullptr, 0, builder);

    // Extract the commit decision from the server's reply and return it.
    session_event_t event = client_messenger.server_reply()->event();
    ASSERT_INVARIANT(
        event == session_event_t::DECIDE_TXN_COMMIT
            || event == session_event_t::DECIDE_TXN_ABORT,
        c_message_unexpected_event_received);

    const transaction_info_t* txn_info = client_messenger.server_reply()->data_as_transaction_info();
    ASSERT_INVARIANT(
        txn_info->transaction_id() == txn_id(), "Unexpected transaction id!");

    // Throw an exception on server-side abort.
    // REVIEW: We could include the gaia_ids of conflicting objects in
    // transaction_update_conflict_internal
    if (event == session_event_t::DECIDE_TXN_ABORT)
    {
        throw transaction_update_conflict_internal();
    }
}

void client_t::init_memory_manager()
{
    s_session_context->memory_manager.load(
        reinterpret_cast<uint8_t*>(shared_data().data()->objects),
        sizeof(shared_data().data()->objects));
}
