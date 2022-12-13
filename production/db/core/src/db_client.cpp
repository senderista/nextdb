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
#include <unordered_set>

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
#include "memory_helpers.hpp"
#include "safe_ts.hpp"

using namespace gaia::common;
using namespace gaia::db;
using namespace gaia::db::memory_manager;
using namespace gaia::db::messages;
using namespace gaia::db::transactions;
using namespace flatbuffers;
using namespace scope_guard;

static constexpr char c_message_validating_txn_should_have_been_validated_before_log_invalidation[]
    = "A committing transaction can only have its log invalidated if the transaction was concurrently validated!";
static constexpr char c_message_validating_txn_should_have_been_validated_before_conflicting_log_invalidation[]
    = "A possibly conflicting txn can only have its log invalidated if the committing transaction was concurrently validated!";
static constexpr char c_message_preceding_txn_should_have_been_validated[]
    = "A transaction with commit timestamp preceding this transaction's begin timestamp is undecided!";

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

    // Destroy the private locator mapping.
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

    // Allocate and initialize new session context.
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

    // Map a shared view of the locator shared memory segment.
    ASSERT_PRECONDITION(!shared_locators().is_set(), "Shared locators segment is already mapped!");
    bool manage_fd = false;
    bool is_shared = true;
    shared_locators().open(s_session_context->fd_locators, manage_fd, is_shared);

    // Initialize thread-local data for safe timestamp mechanism.
    // This MUST be done *after* initializing the shared-memory mappings!
    safe_ts_t::initialize(get_safe_ts_entries(), get_watermarks());
    auto cleanup_safe_ts_data = make_scope_guard([] {
        safe_ts_t::finalize();
    });

    init_memory_manager();

    cleanup_safe_ts_data.dismiss();
    cleanup_fd_locators.dismiss();
    cleanup_session_context.dismiss();
}

void client_t::end_session()
{
    verify_session_active();
    verify_no_txn();

    auto cleanup_session_context = make_scope_guard([&] {
        // Clean up thread-local data for safe timestamp mechanism.
        // This MUST be called *before* cleaning up session context!
        safe_ts_t::finalize();
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
    ASSERT_PRECONDITION(!private_locators().is_set(), "Private locators segment is already mapped!");
    bool manage_fd = false;
    bool is_shared = false;
    private_locators().open(s_session_context->fd_locators, manage_fd, is_shared);

    // Allocate a new begin_ts for this txn and initialize its metadata in the txn table.
    s_session_context->txn_context->txn_id = get_txn_metadata()->register_begin_ts();

    // The begin_ts returned by register_begin_ts() should always be valid because it
    // retries if it is concurrently sealed.
    ASSERT_INVARIANT(txn_id().is_valid(), "Begin timestamp is invalid!");

    // Ensure that there are no undecided txns in our snapshot window.
    safe_watermark_t pre_apply_watermark(watermark_type_t::pre_apply);
    validate_txns_in_range(static_cast<gaia_txn_id_t>(pre_apply_watermark) + 1, txn_id());

    // Allocate new txn log.
    s_session_context->txn_context->txn_log_offset = get_logs()->allocate_log_offset(txn_id());
    if (!txn_log_offset().is_valid())
    {
        throw transaction_log_allocation_failure_internal();
    }

    // Apply (in commit_ts order) all txn logs that were committed before our
    // begin_ts and may not have been applied to our snapshot.
    std::vector<std::pair<gaia_txn_id_t, log_offset_t>> txn_logs_for_snapshot;
    get_txn_log_offsets_for_snapshot(txn_id(), txn_logs_for_snapshot);
    for (const auto& [txn_id, log_offset] : txn_logs_for_snapshot)
    {
        // REVIEW: After snapshot reuse is enabled, skip applying logs with
        // commit_ts <= latest_applied_commit_ts.
        apply_log_from_offset(private_locators().data(), log_offset);
        get_logs()->get_log_from_offset(log_offset)->release_reference(txn_id);
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

    // Free any deallocated objects.
    // TODO: ensure that we deallocate this log offset on a crash.
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
    // allows us to avoid any special cases for empty txn logs.
    if (get_txn_log()->record_count == 0)
    {
        rollback_transaction();
        return;
    }

    // Clean up all transaction-local session state when we exit.
    auto cleanup = make_scope_guard(txn_cleanup);

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
    // and model-check the safe memory reclamation algorithm for txn metadata!
    safe_ts_t safe_begin_ts(txn_id());

    // Register the committing txn under a new commit timestamp.
    gaia_txn_id_t commit_ts = submit_txn(
        txn_id(), txn_log_offset());

    // Validate the txn against all other committed txns in the conflict window.
    bool is_committed = validate_txn(commit_ts);

    // Update the txn metadata with our commit decision.
    get_txn_metadata()->update_txn_decision(commit_ts, is_committed);

    // Throw an exception on server-side abort.
    // REVIEW: We could include the gaia_ids of conflicting objects in
    // transaction_update_conflict_internal (message or structured data).
    if (!is_committed)
    {
        throw transaction_update_conflict_internal();
    }

    // TODO: Persist the commit decision.
    // REVIEW: For now, just set the durable flag unconditionally after validation.
    // Later we can set the flag conditionally on persistence settings.
    //
    // REVIEW: We can return a decision asynchronously with persistence of the
    // decision (because the decision can be reconstructed from the durable log
    // itself, without the decision record).

    // Mark txn as durable in metadata so we can GC the txn log. We only
    // mark it durable after validation to simplify the state transitions:
    // TXN_VALIDATING -> TXN_DECIDED -> TXN_DURABLE.

    // TEST: We could inject a random sleep here to simulate persistence
    // latency and test our GC logic.
    get_txn_metadata()->set_txn_durable(commit_ts);

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

void client_t::init_memory_manager()
{
    s_session_context->memory_manager.load(
        reinterpret_cast<uint8_t*>(shared_data().data()->objects),
        sizeof(shared_data().data()->objects));
}

void client_t::validate_txns_in_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts)
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

void client_t::get_txn_log_offsets_for_snapshot(gaia_txn_id_t begin_ts,
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
    std::reverse(
        std::begin(txn_ids_with_log_offsets_for_snapshot),
        std::end(txn_ids_with_log_offsets_for_snapshot));
}

// Sort all txn log records by locator. This enables us to use fast binary
// search and merge intersection algorithms for conflict detection.
void client_t::sort_log()
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

gaia_txn_id_t client_t::submit_txn(gaia_txn_id_t begin_ts, log_offset_t log_offset)
{
    ASSERT_PRECONDITION(get_txn_metadata()->is_txn_active(begin_ts), "Not an active transaction!");

    ASSERT_PRECONDITION(get_logs()->is_log_offset_allocated(log_offset), "Invalid log offset!");

    // Allocate a new commit_ts and initialize its metadata with our begin_ts and log offset.
    gaia_txn_id_t commit_ts = get_txn_metadata()->register_commit_ts(begin_ts, log_offset);

    // Now update the active txn metadata.
    get_txn_metadata()->set_active_txn_submitted(begin_ts, commit_ts);

    return commit_ts;
}

bool client_t::validate_txn(gaia_txn_id_t commit_ts)
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

                    if (txn_logs_conflict(
                        get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts),
                        get_txn_metadata()->get_txn_log_offset_from_ts(ts)))
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

                if (txn_logs_conflict(
                    get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts),
                    get_txn_metadata()->get_txn_log_offset_from_ts(ts)))
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
void client_t::perform_maintenance()
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

void client_t::apply_txn_logs_to_shared_view()
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

void client_t::gc_applied_txn_logs()
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
            // REVIEW: For now, check the durable flag unconditionally.
            // Later we can check the flag conditionally on persistence settings.
            if (!get_txn_metadata()->is_txn_durable(ts))
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

void client_t::deallocate_object(gaia_offset_t offset)
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

void client_t::update_post_gc_watermark()
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

void client_t::truncate_txn_table()
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

char* client_t::get_txn_metadata_page_address_from_ts(gaia_txn_id_t ts)
{
    char* txn_metadata_map_base_address = reinterpret_cast<char*>(get_txn_metadata());
    size_t ts_entry_byte_offset = ts * sizeof(txn_metadata_entry_t);
    size_t ts_entry_page_byte_offset = (ts_entry_byte_offset / c_page_size_in_bytes) * c_page_size_in_bytes;
    char* ts_entry_page_address = txn_metadata_map_base_address + ts_entry_page_byte_offset;
    return ts_entry_page_address;
}

void client_t::apply_txn_log_from_ts(gaia_txn_id_t commit_ts)
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
    apply_log_to_locators(shared_locators().data(), txn_log);
}

void client_t::gc_txn_log_from_offset(log_offset_t log_offset, bool is_committed)
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

void client_t::deallocate_txn_log(txn_log_t* txn_log, bool is_committed)
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

// This helper method takes 2 txn logs (by offset), and determines if they have
// a non-empty intersection. We could just use std::set_intersection, but that
// outputs all elements of the intersection in a third container, while we just
// need to test for non-empty intersection (and terminate as soon as the first
// common element is found), so we write our own simple merge intersection code.
// If we ever need to return the IDs of all conflicting objects to clients (or
// just log them for diagnostics), we could use std::set_intersection.
bool client_t::txn_logs_conflict(log_offset_t offset1, log_offset_t offset2)
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

bool client_t::acquire_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts)
{
    ASSERT_PRECONDITION(get_txn_metadata()->is_commit_ts(commit_ts), "Not a commit timestamp!");
    gaia_txn_id_t begin_ts = get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts);
    log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts);
    return acquire_txn_log_reference(log_offset, begin_ts);
}

void client_t::release_txn_log_reference_from_commit_ts(gaia_txn_id_t commit_ts)
{
    ASSERT_PRECONDITION(get_txn_metadata()->is_commit_ts(commit_ts), "Not a commit timestamp!");
    gaia_txn_id_t begin_ts = get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts);
    log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(commit_ts);
    release_txn_log_reference(log_offset, begin_ts);
}
