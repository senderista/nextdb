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

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "gaia_internal/common/assert.hpp"
#include "gaia_internal/common/backoff.hpp"
#include "gaia_internal/common/scope_guard.hpp"
#include "gaia_internal/common/socket_helpers.hpp"
#include "gaia_internal/common/system_error.hpp"
#include "gaia_internal/db/db_types.hpp"

#include "db_helpers.hpp"
#include "db_internal_types.hpp"
#include "memory_helpers.hpp"
#include "safe_ts.hpp"

using namespace gaia::common;
using namespace gaia::common::backoff;
using namespace gaia::db;
using namespace gaia::db::memory_manager;
using namespace gaia::db::transactions;
using namespace scope_guard;

static constexpr char c_message_validating_txn_should_have_been_validated_before_log_invalidation[]
    = "A committing transaction can only have its log invalidated if the transaction was concurrently validated!";
static constexpr char c_message_validating_txn_should_have_been_validated_before_conflicting_log_invalidation[]
    = "A possibly conflicting txn can only have its log invalidated if the committing transaction was concurrently validated!";
static constexpr char c_message_preceding_txn_should_have_been_validated[]
    = "A transaction with commit timestamp preceding this transaction's begin timestamp is undecided!";

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
// The simplest asynchronous notification mechanism might be a flag in shared
// memory (either global or per-session), which the client could cheaply check
// before performing any transactional operation.
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

    // Connect to the server's well-known socket name, and receive fds for the
    // shared-memory mappings.
    int received_fds[static_cast<size_t>(data_mapping_t::index_t::count_mappings)];
    std::fill(received_fds, received_fds + std::size(received_fds), -1);
    size_t received_fd_count = std::size(received_fds);
    uint64_t magic{};
    try
    {
        s_session_context->session_socket = get_session_socket(c_default_instance_name);
        // A `peer_disconnected` exception implies that either authentication
        // failed or the session limit was exceeded.
        // REVIEW: distinguish authentication failure from "session limit exceeded"
        // (authentication failure will also result in ECONNRESET, but
        // authentication is currently disabled in the server).
        recv_msg_with_fds(
            session_socket(), received_fds, &received_fd_count, &magic, sizeof(magic));
        ASSERT_POSTCONDITION(magic == c_session_magic, "Session magic cookie has unexpected value!");
    }
    catch (const peer_disconnected& e)
    {
        throw server_connection_failed_internal(e.what());
    }
    catch (const system_error& e)
    {
        throw server_connection_failed_internal(e.what(), e.get_errno());
    }

    // Set up scope guards for the fds.
    // The locators fd needs to be kept around, so its scope guard will be dismissed at the end of this scope.
    // The other fds are not needed, so they'll get their own scope guard to clean them up.
    int fd_locators = received_fds[static_cast<size_t>(data_mapping_t::index_t::locators)];
    auto cleanup_fd_locators = make_scope_guard([&fd_locators] { close_fd(fd_locators); });
    auto cleanup_fd_others = make_scope_guard([&] {
        for (const auto& data_mapping : data_mappings())
        {
            if (data_mapping.mapping_index != data_mapping_t::index_t::locators)
            {
                int fd = received_fds[static_cast<size_t>(data_mapping.mapping_index)];
                close_fd(fd);
            }
        }
    });

    // Set up the shared-memory mappings.
    // The locators mapping will be performed manually, so skip its information in the mapping table.
    for (const auto& data_mapping : data_mappings())
    {
        if (data_mapping.mapping_index != data_mapping_t::index_t::locators)
        {
            int fd = received_fds[static_cast<size_t>(data_mapping.mapping_index)];
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

    // Initialize thread-local data for txn metadata protection.
    // This MUST be done *after* initializing the shared-memory mappings!

    // Reserve this thread's safe_ts entries index.
    ASSERT_PRECONDITION(safe_ts_entries_index() == safe_ts_entries_t::c_invalid_safe_ts_index,
        "safe_ts entries index should be invalid!");
    s_session_context->safe_ts_entries_index = get_safe_ts_entries()->reserve_safe_ts_index();
    if (safe_ts_entries_index() == safe_ts_entries_t::c_invalid_safe_ts_index)
    {
        throw safe_ts_failure_internal();
    }
    ASSERT_POSTCONDITION(safe_ts_entries_index() <= safe_ts_entries_t::c_max_safe_ts_index,
        "safe_ts entries index should be in valid range!");
    auto cleanup_safe_ts_entry = make_scope_guard([&] {
        // This automatically invalidates the entry at safe_ts_entries_index().
        get_safe_ts_entries()->release_safe_ts_index(s_session_context->safe_ts_entries_index);
    });

    init_memory_manager();

    cleanup_safe_ts_entry.dismiss();
    cleanup_fd_locators.dismiss();
    cleanup_session_context.dismiss();
}

void client_t::end_session()
{
    verify_session_active();
    verify_no_txn();

    auto cleanup_session_context = make_scope_guard([&] {
        // Release this thread's safe_ts entries index.
        // NB: This MUST be called *before* cleaning up session context!
        get_safe_ts_entries()->release_safe_ts_index(s_session_context->safe_ts_entries_index);
        // This automatically invalidates the entry at safe_ts_entries_index().
        ASSERT_POSTCONDITION(safe_ts_entries_index() == safe_ts_entries_t::c_invalid_safe_ts_index,
            "safe_ts entries index must be invalid!");

        // The session_context destructor will close the session socket.
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
    auto cleanup_txn_context = make_scope_guard([&] {
        s_session_context->txn_context->clear();
    });

    auto cleanup_snapshot_logs = make_scope_guard([&] {
        txn_logs_for_snapshot().clear();
    });

    // Protect all txn metadata accessed by this function. Reserving a timestamp
    // equal to the value of the post-GC watermark read before the begin_ts is
    // acquired is sufficient to ensure that all txn metadata scanned within the
    // snapshot window (i.e., post-apply watermark to begin_ts) and within the
    // conflict window of each undecided txn within the snapshot window is
    // protected from concurrent reclamation. The argument is as follows:
    //
    // 1. At any particular time, the pre-apply and post-apply watermarks are at
    //    least as recent as the post-GC watermark, and any value of the post-GC
    //    watermark after reserving its initially observed value is at least as
    //    recent as the reserved value, so the reserved value of the post-GC
    //    watermark protects all later values of the pre-apply, post-apply, and
    //    post-GC watermarks, and therefore all scans with those values as their
    //    left endpoint.
    //
    // 2. Any undecided txn with its commit_ts within the snapshot window must
    //    have its begin_ts preceded by the current value of the post-GC
    //    watermark, because the post-GC watermark cannot advance past the
    //    begin_ts of an undecided txn. Therefore, when the commit_ts status of
    //    an undecided txn was read as VALIDATING, the post-GC watermark must
    //    have been in the past of its begin_ts, and that value of the post-GC
    //    watermark must have been at least as recent as the value of the
    //    post-GC watermark that was reserved before the new txn's begin_ts was
    //    acquired. Therefore, the entire conflict window of any undecided txn
    //    with a commit_ts within the snapshot window of the new txn must be
    //    protected by the reserved value of the post-GC watermark.

    protect_txn_metadata();
    auto cleanup_txn_metadata_protection = make_scope_guard(unprotect_txn_metadata);

    // Allocate a new begin_ts for this txn and initialize its metadata in the txn table.
    s_session_context->txn_context->txn_id = get_txn_metadata()->register_begin_ts();

    // The begin_ts returned by register_begin_ts() should always be valid because it
    // retries if it is concurrently sealed.
    ASSERT_INVARIANT(txn_id().is_valid(), "Begin timestamp is invalid!");

    // We use this flag to apply various single-thread optimizations.
    bool is_snapshot_window_empty = false;
    if (latest_applied_commit_ts().is_valid())
    {
        is_snapshot_window_empty = (txn_id() == latest_applied_commit_ts() + 1);
    }

    // Ensure that there are no undecided txns in our snapshot window.
    if (!is_snapshot_window_empty)
    {
        auto pre_apply_watermark = get_safe_watermark(watermark_type_t::pre_apply);
        validate_txns_in_range(pre_apply_watermark + 1, txn_id());
    }

    // Allocate new txn log.
    s_session_context->txn_context->txn_log_offset = get_logs()->allocate_log_offset(txn_id());
    if (!txn_log_offset().is_valid())
    {
        throw transaction_log_allocation_failure_internal();
    }

    // If our snapshot is already mapped, and either the snapshot window is
    // empty or we can pin all committed txn logs between
    // latest_applied_commit_ts and our begin_ts, then we can reuse our
    // snapshot. Otherwise, we must remap our snapshot.
    bool can_reuse_snapshot = (
        private_locators().is_set() &&
        latest_applied_commit_ts().is_valid() &&
        (is_snapshot_window_empty || get_txn_log_offsets_in_range(
            latest_applied_commit_ts() + 1, txn_id(), txn_logs_for_snapshot())));

    ASSERT_POSTCONDITION(can_reuse_snapshot || txn_logs_for_snapshot().empty(),
        "get_txn_log_offsets_in_range() cannot fail and return a non-empty set of log offsets!");

    if (!can_reuse_snapshot)
    {
        // Map a new private COW view of the locator shared memory segment.
        private_locators().close();
        bool manage_fd = false;
        bool is_shared = false;
        private_locators().open(s_session_context->fd_locators, manage_fd, is_shared);

        // Get all txn logs that might need to be applied to the new snapshot.
        if (!is_snapshot_window_empty)
        {
            get_txn_log_offsets_for_snapshot(txn_id(), txn_logs_for_snapshot());
        }
    }

    // Apply (in commit_ts order) all txn logs that were committed before our
    // begin_ts and may not have been applied to our snapshot.
    if (!is_snapshot_window_empty)
    {
        for (const auto& [txn_id, log_offset] : txn_logs_for_snapshot())
        {
            apply_log_from_offset(private_locators().data(), log_offset);
            get_logs()->get_log_from_offset(log_offset)->release_reference(txn_id);
        }
    }

    cleanup_txn_context.dismiss();
}

void client_t::rollback_transaction()
{
    verify_txn_active();

    ASSERT_PRECONDITION(
        s_session_context->txn_context->initialized(),
        "Transaction context should be initialized already at the end of a transaction!");

    // Protect all txn metadata accessed by this function.
    protect_txn_metadata();
    auto cleanup_txn_metadata_protection = make_scope_guard(unprotect_txn_metadata);

    // Clean up all transaction-local session state.
    auto cleanup_txn_context = make_scope_guard([&] {
        s_session_context->txn_context->clear();
    });

    // Apply our undo log to roll back any changes to our private snapshot.
    bool apply_new_versions = false;
    apply_log_to_locators(private_locators().data(), txn_log(), apply_new_versions);

    // Free any deallocated objects.

    // Release ownership as precondition for GC.
    bool success = txn_log()->invalidate(txn_id());
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
void client_t::commit_transaction()
{
    verify_txn_active();

    ASSERT_PRECONDITION(
        s_session_context->txn_context->initialized(),
        "Transaction context should be initialized already at the end of a transaction!");

    // This optimization to treat committing a read-only txn as a rollback
    // allows us to avoid any special cases for empty txn logs.
    // REVIEW: Should read-only txns have to perform system maintenance tasks
    // (arguably yes because they force retaining obsolete versions)?
    if (txn_log()->record_count == 0)
    {
        rollback_transaction();
        return;
    }

    // Clean up all transaction-local session state when we exit.
    auto cleanup_txn_context = make_scope_guard([&] {
        s_session_context->txn_context->clear();
    });

    auto cleanup_snapshot_logs = make_scope_guard([&] {
        txn_logs_for_snapshot().clear();
    });

    auto cleanup_snapshot = make_scope_guard([&] {
        // If we failed to apply all committed logs in our conflict window to
        // our snapshot, then we cannot reuse it, so destroy the private
        // mapping.
        private_locators().close();

        // We must clear latest_applied_commit_ts if there are any "holes" in
        // the sequence of applied logs.
        s_session_context->latest_applied_commit_ts = c_invalid_gaia_txn_id;
    });

    // Protect all txn metadata accessed by this function. Reserving a timestamp
    // equal to the value of the post-GC watermark read before the commit_ts is
    // acquired is sufficient to ensure that all txn metadata scanned within the
    // conflict window of the committing txn (i.e., from its begin_ts to its
    // commit_ts) and within the conflict window of each undecided txn whose
    // commit_ts is within the committing txn's conflict window is protected
    // from concurrent reclamation. The argument is as follows:
    //
    // 1. The post-GC watermark cannot advance past the begin_ts of an undecided
    //    txn, and the committing txn was undecided when the post-GC watermark's
    //    value was reserved (it could not have been concurrently validated
    //    because its commit_ts had not yet been acquired), so the post-GC
    //    watermark preceded the committing txn's begin_ts when it was reserved,
    //    and therefore the entire conflict window of the committing txn is
    //    protected by the reserved post-GC watermark, until it is released by
    //    the committing session thread.
    //
    // 2. Any undecided txn with its commit_ts within the committing txn's
    //    conflict window must have its begin_ts preceded by the current value
    //    of the post-GC watermark, because the post-GC watermark cannot advance
    //    past the begin_ts of an undecided txn. Therefore, when the commit_ts
    //    status of an undecided txn was read as VALIDATING, the post-GC
    //    watermark must have been in the past of its begin_ts, and that value
    //    of the post-GC watermark must have been at least as recent as the
    //    value of the post-GC watermark that was reserved before the committing
    //    txn's begin_ts was acquired. Therefore, the entire conflict window of
    //    any undecided txn with a commit_ts within the conflict window of the
    //    committing txn must be protected by the reserved value of the post-GC
    //    watermark.

    // NB: The corresponding call to unprotect_txn_metadata() must be made after
    // txn log maintenance is complete and before txn metadata maintenance is
    // performed (so that advancing the pre-truncate watermark is not obstructed
    // by a reserved timestamp).

    protect_txn_metadata();
    auto cleanup_txn_metadata_protection = make_scope_guard(unprotect_txn_metadata);

    // Update watermarks and perform associated maintenance tasks. This will
    // block new transactions on this session thread, but that is a feature, not
    // a bug, because it provides natural backpressure on clients who submit
    // long-running transactions that prevent resources from being reclaimed.
    // This approach helps keep the system from accumulating more deferred work
    // than it can ever retire (which is a problem with performing all
    // maintenance asynchronously in the background).
    auto do_gc = make_scope_guard([&] {
        bool contention_detected = do_txn_log_maintenance();
        // We must release our reserved timestamp in order to advance the
        // pre-truncate watermark as far as possible.
        unprotect_txn_metadata();
        // We need to avoid executing unprotect_txn_metadata() twice because it
        // is not idempotent (we assert that it must be called directly after
        // calling protect_txn_metadata()).
        cleanup_txn_metadata_protection.dismiss();
        contention_detected |= do_txn_metadata_maintenance();
        // If we detect contention, then back off for about 5us (empirically
        // determined to be the best interval).
        if (contention_detected)
        {
            spin_wait(c_contention_backoff_us * c_pause_iterations_per_us);
        }
    });

    // Before registering the log, sort by locator for fast conflict detection.
    sort_log(txn_log());

    // Register the committing txn under a new commit timestamp.
    gaia_txn_id_t commit_ts = submit_txn(txn_id(), txn_log_offset());

    // We use this flag to apply various single-thread optimizations.
    bool is_conflict_window_empty = (commit_ts == txn_id() + 1);

    // Validate the txn against all other committed txns in the conflict window.
    bool is_committed = true;
    if (!is_conflict_window_empty)
    {
        is_committed = validate_txn(commit_ts);
    }
    else
    {
        // This is normally done within validate_txn().
        get_txn_metadata()->set_submitted_txn_commit_ts(txn_id(), commit_ts);
    }

    // Update this txn's commit_ts entry with the commit decision.
    get_txn_metadata()->update_txn_decision(commit_ts, is_committed);

    // Throw an exception on an abort decision.
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

    // Try to apply all committed logs in our conflict window to our snapshot,
    // to enable later reuse.
    // NB: Since we have already applied our own txn log to our snapshot, we are
    // applying our own log twice. But applying txn logs to snapshots is
    // idempotent, so this is benign, and cheaper than applying our undo log
    // first to avoid the duplicate log application.

    // If the conflict window is empty, then we already applied our own log,
    // which is the only unapplied committed log since our begin_ts.
    bool can_apply_logs = is_conflict_window_empty;
    if (!is_conflict_window_empty)
    {
        can_apply_logs = get_txn_log_offsets_in_range(
            txn_id() + 1, commit_ts, txn_logs_for_snapshot());

        ASSERT_POSTCONDITION(can_apply_logs || txn_logs_for_snapshot().empty(),
            "get_txn_log_offsets_in_range() cannot fail and return a non-empty set of log offsets!");

        if (can_apply_logs)
        {
            // Apply all logs in commit_ts order.
            for (const auto& [txn_id, log_offset] : txn_logs_for_snapshot())
            {
                apply_log_from_offset(private_locators().data(), log_offset);
                get_logs()->get_log_from_offset(log_offset)->release_reference(txn_id);
            }
        }
    }

    if (can_apply_logs)
    {
        // Remember the latest log we applied so we can reuse this snapshot.
        s_session_context->latest_applied_commit_ts = commit_ts;

        // We can possibly reuse this snapshot, so keep it mapped.
        cleanup_snapshot.dismiss();
    }
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

bool client_t::get_txn_log_offsets_in_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts,
    std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_ids_with_log_offsets)
{
    ASSERT_PRECONDITION(
        txn_ids_with_log_offsets.empty(),
        "Vector passed in to get_txn_log_offsets_in_range() should be empty!");

    // This is just for asserting an invariant.
    bool acquired_first_log_ref = false;

    for (gaia_txn_id_t ts = start_ts; ts < end_ts; ++ts)
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
                // count in the txn log header before applying the log.
                // The reference count is collocated in the same word as the
                // begin timestamp, and we verify the timestamp hasn't changed
                // when we increment the reference count, so we will never try
                // to apply a reused txn log.
                log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(ts);
                txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);
                bool acquire_ref_succeeded = txn_log->acquire_reference(txn_id);

                // If we can acquire a reference to the first txn log in the range, then
                // it should be impossible for any GC task to advance past that log's
                // commit_ts and invalidate any subsequent logs.
                ASSERT_INVARIANT(acquire_ref_succeeded || !acquired_first_log_ref,
                    "Any txn log after a pinned txn log cannot be invalidated!");

                if (acquire_ref_succeeded)
                {
                    acquired_first_log_ref = true;
                    txn_ids_with_log_offsets.emplace_back(txn_id, log_offset);
                }
                else
                {
                    // We must return either all or none of the txn logs in the range.
                    return false;
                }
            }
        }
    }

    return true;
}

void client_t::get_txn_log_offsets_for_snapshot(gaia_txn_id_t begin_ts,
    std::vector<std::pair<gaia_txn_id_t, log_offset_t>>& txn_ids_with_log_offsets)
{
    ASSERT_PRECONDITION(
        txn_ids_with_log_offsets.empty(),
        "Vector passed in to get_txn_log_offsets_for_snapshot() should be empty!");

    // Take a snapshot of the post-apply watermark and scan backward from
    // begin_ts, stopping either just before the saved watermark or at the first
    // commit_ts whose log offset has been invalidated. This avoids having our
    // scan race the concurrently advancing watermark.
    auto post_apply_watermark = get_safe_watermark(watermark_type_t::post_apply);
    for (gaia_txn_id_t ts = begin_ts - 1; ts > post_apply_watermark; --ts)
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
                // count in the txn log header before applying the log.
                // The reference count is collocated in the same word as the
                // begin timestamp, and we verify the timestamp hasn't changed
                // when we increment the reference count, so we will never try
                // to apply a reused txn log.
                log_offset_t log_offset = get_txn_metadata()->get_txn_log_offset_from_ts(ts);
                txn_log_t* txn_log = get_logs()->get_log_from_offset(log_offset);
                if (txn_log->acquire_reference(txn_id))
                {
                    txn_ids_with_log_offsets.emplace_back(txn_id, log_offset);
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
        std::begin(txn_ids_with_log_offsets),
        std::end(txn_ids_with_log_offsets));
}

// Sort all txn log records by locator. This enables us to use fast binary
// search and merge intersection algorithms for conflict detection.
void client_t::sort_log(txn_log_t* txn_log)
{
    // We use `log_record_t.sequence` as a secondary sort key to preserve the
    // temporal order of multiple updates to the same locator.
    std::sort(
        &txn_log->log_records[0],
        &txn_log->log_records[txn_log->record_count],
        [](const log_record_t& lhs, const log_record_t& rhs) {
            auto lhs_pair = std::pair{lhs.locator, lhs.sequence};
            auto rhs_pair = std::pair{rhs.locator, rhs.sequence};
            return lhs_pair < rhs_pair;
        });
}

// NB: The comparison logic here and in sort_log() must be identical.
bool client_t::is_log_sorted(txn_log_t* txn_log)
{
    return std::is_sorted(
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

    // Transition the begin_ts entry from ACTIVE to SUBMITTED state.
    get_txn_metadata()->set_active_txn_submitted(begin_ts);

    // Allocate a new commit_ts and initialize its metadata with our begin_ts and log offset.
    gaia_txn_id_t commit_ts = get_txn_metadata()->register_commit_ts(begin_ts, log_offset);

    return commit_ts;
}

bool client_t::validate_txn(gaia_txn_id_t commit_ts)
{
    gaia_txn_id_t begin_ts = get_txn_metadata()->get_begin_ts_from_commit_ts(commit_ts);

    // We defer setting the commit_ts in the begin_ts entry until validation, so
    // that validating threads don't have to wait for the committing session
    // thread to set it, and to ensure that any validated txn has it set.
    //
    // This is an idempotent operation.
    get_txn_metadata()->set_submitted_txn_commit_ts(begin_ts, commit_ts);

    // Optimization for single-threaded case: return success if the conflict window is empty.
    if (commit_ts == begin_ts + 1)
    {
        return true;
    }

    // Acquire a single reference to the committing txn's log for the duration
    // of validation to minimize contention.
    // This also prevents the pre-truncate watermark from advancing into the
    // conflict window, because the post-GC watermark cannot advance past a
    // submitted begin_ts with a commit_ts that is not marked TXN_GC_COMPLETE,
    // GC cannot occur while the reference is held, and the pre-truncate
    // watermark always lags the post-GC watermark.
    // If we fail to acquire the reference, the conflict window is unprotected,
    // but we don't need to scan the conflict window because the txn must have
    // already been validated.
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

    // Seal all uninitialized entries, validate all undecided txns,
    // and test all committed txns for conflicts with the committing txn.
    for (gaia_txn_id_t ts = begin_ts + 1; ts < commit_ts; ++ts)
    {
        // Sealing all uninitialized entries within the conflict window marks a
        // "fence" after which any submitted txns which have acquired commit
        // timestamps in the conflict window must already have stored commit_ts
        // entries at that timestamp (they must retry with a new timestamp
        // otherwise).
        if (get_txn_metadata()->seal_uninitialized_ts(ts))
        {
            continue;
        }

        // Validate each undecided txn, and then test committed txns for conflicts.
        if (get_txn_metadata()->is_commit_ts(ts))
        {
            // If this txn is undecided, validate it.
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

            // If the validated txn is committed, test it for conflicts.
            if (get_txn_metadata()->is_txn_committed(ts))
            {
                // We need to acquire references on both txn logs being tested for
                // conflicts, in case either txn log is invalidated by another
                // thread concurrently advancing the watermark. If either log is
                // invalidated, it must be that another thread has validated our
                // txn, so we can exit early.
                if (!acquire_txn_log_reference_from_commit_ts(ts))
                {
                    // If this submitted txn already had its log invalidated, then
                    // it must be eligible for GC. But any commit_ts within the
                    // conflict window is ineligible for GC, so the committing txn
                    // must have already been (recursively) validated, and we can
                    // just return the commit decision.
                    ASSERT_INVARIANT(
                        get_txn_metadata()->is_txn_decided(commit_ts),
                        c_message_validating_txn_should_have_been_validated_before_conflicting_log_invalidation);
                    return get_txn_metadata()->is_txn_committed(commit_ts);
                }
                auto release_submitted_log_ref = make_scope_guard([&ts] {
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

// The following two methods are called by an active txn when it is terminated
// or by a submitted txn after it is validated. They perform a few system
// maintenance tasks, which can be deferred indefinitely with no effect on
// availability or correctness, but which are essential to maintain acceptable
// performance and resource usage.
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
// oldest txn whose metadata cannot yet be safely reclaimed).
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
//    commit_ts has TXN_GC_COMPLETE unset), we abort the pass.
//
// Returns true if contention was detected, false otherwise.
bool client_t::do_txn_log_maintenance()
{
    bool contention_detected = false;

    // Attempt to apply all txn logs to the shared view, from the last value of
    // the post-apply watermark to the latest committed txn.
    contention_detected |= apply_txn_logs_to_shared_view();

    // Attempt to reclaim the resources of all txns from the post-GC watermark
    // to the post-apply watermark.
    contention_detected |= gc_applied_txn_logs();

    // Finally, catch up the post-GC watermark.
    // Unlike log application, we don't try to perform GC and advance the
    // post-GC watermark in a single scan, because log application is strictly
    // sequential, while GC is sequentially initiated but concurrently executed.
    contention_detected |= update_post_gc_watermark();

    return contention_detected;
}

// Calculate a "safe truncation timestamp", below which all virtual memory
// containing txn metadata can be safely reclaimed, and attempt to advance the
// pre-truncate watermark to that timestamp. If we successfully advanced the
// pre-truncate watermark, then we calculate the number of pages between the
// previous pre-truncate watermark value and its new value; if this count
// exceeds a threshold then we decommit all such pages using madvise(2). (It is
// possible for multiple GC tasks to concurrently or repeatedly decommit the
// same pages, but madvise(2) is idempotent and concurrency-safe.)
//
// Returns true if contention was detected, false otherwise.
bool client_t::do_txn_metadata_maintenance()
{
    // Find a timestamp at which we can safely truncate the txn table and
    // advance the pre-truncate watermark to that timestamp.
    gaia_txn_id_t old_pre_truncate_watermark, new_pre_truncate_watermark;
    bool contention_detected = update_pre_truncate_watermark(
        old_pre_truncate_watermark, new_pre_truncate_watermark);

    // If we advanced the pre-truncate watermark, truncate the txn table at the
    // highest page boundary less than the pre-truncate watermark.
    if (new_pre_truncate_watermark.is_valid())
    {
        truncate_txn_table(old_pre_truncate_watermark, new_pre_truncate_watermark);
    }

    return contention_detected;
}

bool client_t::apply_txn_logs_to_shared_view()
{
    bool contention_detected = false;

    // First get a snapshot of the timestamp counter for an upper bound on
    // the scan (we don't know yet if this is a begin_ts or commit_ts).
    gaia_txn_id_t last_allocated_ts = get_last_txn_id();

    // Now get a snapshot of the pre-apply watermark,
    // for a lower bound on the scan.
    auto pre_apply_watermark = get_safe_watermark(watermark_type_t::pre_apply);

    // Scan from the saved pre-apply watermark to the last known timestamp,
    // and apply all committed txn logs from the longest prefix of decided
    // txns that does not overlap with the conflict window of any undecided
    // txn. Advance the pre-apply watermark before applying the txn log
    // of a committed txn, and advance the post-apply watermark after
    // applying the txn log.
    for (gaia_txn_id_t ts = pre_apply_watermark + 1; ts <= last_allocated_ts; ++ts)
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
        // can never advance into the conflict window of an undecided txn,
        // ensuring that all logs of committed txns within the conflict window
        // remain available for conflict testing.
        if (get_txn_metadata()->is_begin_ts(ts))
        {
            if (get_txn_metadata()->is_txn_active(ts))
            {
                break;
            }

            if (get_txn_metadata()->is_txn_submitted(ts))
            {
                auto commit_ts = get_txn_metadata()->get_commit_ts_from_begin_ts(ts);
                // NB: Because transitioning a begin_ts entry from ACTIVE to
                // SUBMITTED and setting its linked commit_ts are not a single
                // atomic operation, it is possible for a begin_ts entry in
                // SUBMITTED state to have an invalid linked commit_ts. In that
                // case, we know the linked commit_ts has not been validated
                // (because validation sets the linked commit_ts before
                // recording a decision).
                if (!commit_ts.is_valid() || get_txn_metadata()->is_txn_validating(commit_ts))
                {
                    break;
                }
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
        if (get_safe_watermark(watermark_type_t::pre_apply) != prev_ts
            || get_safe_watermark(watermark_type_t::post_apply) != prev_ts)
        {
            // Either the pre-apply watermark has been advanced since our
            // previous read, or the post-apply watermark has not caught up with
            // it, so we know another thread is active.
            contention_detected = true;
            break;
        }

        if (!get_watermarks()->advance_watermark(watermark_type_t::pre_apply, ts))
        {
            // If another thread has already advanced the watermark ahead of
            // this ts, we abort advancing it further.
            ASSERT_INVARIANT(
                get_safe_watermark(watermark_type_t::pre_apply) > pre_apply_watermark,
                "The watermark must have advanced if advance_watermark() failed!");

            // Another thread concurrently advanced the watermark.
            contention_detected = true;
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

    return contention_detected;
}

bool client_t::gc_applied_txn_logs()
{
    bool contention_detected = false;

    // Ensure we clean up our cached chunk IDs when we exit this task.
    auto cleanup_fd = make_scope_guard([&] { map_gc_chunks_to_versions().clear(); });

    // Get a snapshot of the post-apply watermark, for an upper bound on the scan.
    auto post_apply_watermark = get_safe_watermark(watermark_type_t::post_apply);

    // Get a snapshot of the post-GC watermark, for a lower bound on the scan.
    auto post_gc_watermark = get_safe_watermark(watermark_type_t::post_gc);

    // Scan from the post-GC watermark to the post-apply watermark, executing GC
    // on any commit_ts if the txn log is valid (and the durable flag is set if
    // persistence is enabled). (If we fail to invalidate the txn log, we abort
    // the scan to avoid contention.) When GC is complete, set the
    // TXN_GC_COMPLETE flag on the txn metadata and continue.
    for (gaia_txn_id_t ts = post_gc_watermark + 1; ts <= post_apply_watermark; ++ts)
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
                // NB: We don't set contention_detected = true here because we can still
                // make progress (GC is concurrent, unlike log application).
                continue;
            }

            // Try to acquire ownership of this txn log by invalidating it.
            // Invalidation can fail only if it has already occurred or there
            // are outstanding shared references. In the first case, we should
            // abort the scan to avoid contention (because another thread must
            // have invalidated the txn log after the check we just performed).
            // In the second case, we should abort the scan because we cannot
            // make progress.
            // REVIEW: Should we change the signature of invalidate() so we can
            // distinguish between these two cases in order to reliably detect
            // contention?
            if (!txn_log->invalidate(begin_ts))
            {
                contention_detected = true;
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
    for (const auto& entry : map_gc_chunks_to_versions())
    {
        chunk_offset_t chunk_offset = entry.first;
        chunk_version_t chunk_version = entry.second;
        // Don't try to deallocate a chunk that this session owns; it will be
        // deallocated when it is retired.
        if (chunk_offset != client_t::chunk_offset())
        {
            chunk_manager_t chunk_manager;
            chunk_manager.load(chunk_offset);
            chunk_manager.try_deallocate_chunk(chunk_version);
            chunk_manager.release();
        }
    }

    return contention_detected;
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
    bool found_chunk = false;
    for (const auto& entry : map_gc_chunks_to_versions())
    {
        if (entry.first == chunk_offset)
        {
            found_chunk = true;
            // If this GC task already cached this chunk, then the versions must match!
            ASSERT_INVARIANT(entry.second == version, "Chunk version must match cached chunk version!");
        }
    }
    if (!found_chunk)
    {
        map_gc_chunks_to_versions().emplace_back(chunk_offset, version);
    }

    // Delegate deallocation of the object to the chunk manager.
    chunk_manager.deallocate(offset);
}

bool client_t::update_post_gc_watermark()
{
    bool contention_detected = false;

    // Get a snapshot of the post-apply watermark, for an upper bound on the scan.
    auto post_apply_watermark = get_safe_watermark(watermark_type_t::post_apply);

    // Get a snapshot of the post-GC watermark, for a lower bound on the scan.
    auto post_gc_watermark = get_safe_watermark(watermark_type_t::post_gc);

    // Scan from the post-GC watermark to the post-apply watermark, advancing
    // the post-GC watermark to any commit_ts marked TXN_GC_COMPLETE, or to any
    // begin_ts unless it is marked TXN_SUBMITTED and its commit_ts is not
    // marked TXN_GC_COMPLETE. (The latter condition prevents the pre-truncate
    // watermark from advancing into the conflict window of any commit_ts entry
    // that has not completed GC, in order to allow validating threads to safely
    // scan the conflict window while they hold a reference to the txn log
    // referenced by the commit_ts entry.)
    //
    // If the post-GC watermark cannot be advanced to the current timestamp,
    // abort the scan.
    for (gaia_txn_id_t ts = post_gc_watermark + 1; ts <= post_apply_watermark; ++ts)
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
            // if its commit_ts is marked TXN_GC_COMPLETE. This ensures that
            // acquiring a reference to a txn log referenced by a commit_ts
            // entry protects the entire conflict window of the commit_ts.
            if (get_txn_metadata()->is_txn_submitted(ts))
            {
                auto commit_ts = get_txn_metadata()->get_commit_ts_from_begin_ts(ts);
                // The pre-apply watermark can only advance to a submitted
                // begin_ts if its commit_ts is validated, and the commit_ts
                // cannot be validated without setting the begin_ts entry's
                // commit_ts, so this commit_ts must be valid.
                ASSERT_INVARIANT(commit_ts.is_valid() && get_txn_metadata()->is_txn_decided(commit_ts),
                    "The pre-apply watermark should not be advanced to a submitted begin_ts with an undecided commit_ts!");
                if (!get_txn_metadata()->is_txn_gc_complete(commit_ts))
                {
                    break;
                }
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
                get_safe_watermark(watermark_type_t::post_gc) > post_gc_watermark,
                "The watermark must have advanced if advance_watermark() failed!");

            // Another thread concurrently advanced the watermark.
            contention_detected = true;
            break;
        }
    }

    return contention_detected;
}

bool client_t::update_pre_truncate_watermark(
    gaia_txn_id_t& old_pre_truncate_watermark, gaia_txn_id_t& new_pre_truncate_watermark)
{
    ASSERT_PRECONDITION(safe_ts_entries_index() <= safe_ts_entries_t::c_max_safe_ts_index,
        "safe_ts entries index should be valid!");
    ASSERT_PRECONDITION(get_safe_ts_entries(), "Expected safe_ts_entries structure to be mapped!");
    ASSERT_PRECONDITION(get_watermarks(), "Expected watermarks structure to be mapped!");

    // The calling thread should have already released its reserved safe_ts, to
    // ensure the pre-truncate watermark can advance as far as possible.
    auto reserved_ts = get_safe_ts_entries()->get_reserved_ts(safe_ts_entries_index());
    ASSERT_PRECONDITION(!reserved_ts.is_valid(), "Expected any reserved safe_ts to be released!");

    // new_pre_truncate_watermark is only valid if we advance the pre-truncate watermark.
    new_pre_truncate_watermark = c_invalid_gaia_txn_id;

    // Get a snapshot of the pre-truncate watermark before advancing it.
    old_pre_truncate_watermark = get_watermarks()->get_watermark(watermark_type_t::pre_truncate);

    // Abort if the largest possible range of metadata that can be freed (from
    // the last truncation boundary to the current post-GC watermark) does not
    // exceed the minimum page threshold.
    // We add 1 because in general the truncation boundaries will not fall on a
    // page boundary.
    size_t pages_to_decommit_upper_bound = 1 + get_txn_metadata_page_count_from_ts_range(
        old_pre_truncate_watermark, get_watermarks()->get_watermark(watermark_type_t::post_gc));

    if (pages_to_decommit_upper_bound < transactions::c_min_pages_to_free)
    {
        return false;
    }

    // Compute a safe truncation timestamp.
    new_pre_truncate_watermark = get_safe_ts_entries()->get_safe_truncation_ts(get_watermarks());

    // Abort if the safe truncation timestamp does not exceed the current
    // pre-truncate watermark.
    // NB: It is expected that the safe truncation timestamp can be behind
    // the pre-truncate watermark, because some published (but not yet
    // validated) timestamps may have been behind the pre-truncate watermark
    // when they were published (and will later fail validation).
    if (new_pre_truncate_watermark <= old_pre_truncate_watermark)
    {
        new_pre_truncate_watermark = c_invalid_gaia_txn_id;
        return false;
    }

    // Try to advance the pre-truncate watermark.
    if (!get_watermarks()->advance_watermark(watermark_type_t::pre_truncate, new_pre_truncate_watermark))
    {
        new_pre_truncate_watermark = c_invalid_gaia_txn_id;

        // Abort if another thread has concurrently advanced the
        // pre-truncate watermark, to avoid contention.
        ASSERT_INVARIANT(
            get_watermarks()->get_watermark(watermark_type_t::pre_truncate) > old_pre_truncate_watermark,
            "The watermark must have advanced if advance_watermark() failed!");

        // Contention was detected.
        return true;
    }

    return false;
}

bool client_t::truncate_txn_table(
    gaia_txn_id_t old_pre_truncate_watermark, gaia_txn_id_t new_pre_truncate_watermark)
{
    // Truncate the txn table by decommitting its unused physical pages. Because
    // this operation is concurrency-safe and idempotent, it can be done without
    // mutual exclusion.
    // REVIEW: This method could also be used to safely advance the "head"
    // pointer if the txn table were implemented as a circular buffer.

    // Calculate the number of pages between the old and new pre-truncate
    // watermark values. If the result exceeds a threshold, then decommit all
    // such pages.
    size_t pages_to_decommit_count = get_txn_metadata_page_count_from_ts_range(
        old_pre_truncate_watermark, new_pre_truncate_watermark);
    if (pages_to_decommit_count > transactions::c_min_pages_to_free)
    {
        char* prev_page_start_address = get_txn_metadata_page_address_from_ts(old_pre_truncate_watermark);

        // MADV_FREE seems like the best fit for our needs, since it allows the OS
        // to lazily reclaim decommitted pages. However, it returns EINVAL when used
        // with MAP_SHARED, so we need to use MADV_REMOVE (which works with memfd
        // objects). According to the manpage, madvise(MADV_REMOVE) is equivalent to
        // fallocate(FALLOC_FL_PUNCH_HOLE).
        if (-1 == ::madvise(prev_page_start_address, pages_to_decommit_count * c_page_size_in_bytes, MADV_REMOVE))
        {
            throw_system_error("madvise(MADV_REMOVE) failed!");
        }

        return true;
    }

    return false;
}

char* client_t::get_txn_metadata_page_address_from_ts(gaia_txn_id_t ts)
{
    char* txn_metadata_map_base_address = reinterpret_cast<char*>(get_txn_metadata());
    size_t ts_entry_byte_offset = ts * sizeof(txn_metadata_entry_t);
    size_t ts_entry_page_byte_offset = (ts_entry_byte_offset / c_page_size_in_bytes) * c_page_size_in_bytes;
    char* ts_entry_page_address = txn_metadata_map_base_address + ts_entry_page_byte_offset;
    return ts_entry_page_address;
}

size_t client_t::get_txn_metadata_page_count_from_ts_range(gaia_txn_id_t start_ts, gaia_txn_id_t end_ts)
{
    ASSERT_PRECONDITION(start_ts <= end_ts, "Start timestamp must be at least as large as end timestamp!");

    char* start_page_start_address = get_txn_metadata_page_address_from_ts(start_ts);
    char* end_page_start_address = get_txn_metadata_page_address_from_ts(end_ts);

    // Check for overflow, just in case.
    ASSERT_INVARIANT(
        start_page_start_address <= end_page_start_address,
        "The end timestamp entry must reside on the same or later page as the start timestamp entry!");

    return (end_page_start_address - start_page_start_address) / c_page_size_in_bytes;
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

    // Verify that the logs are sorted by locator/sequence.
    DEBUG_ASSERT_PRECONDITION(
        is_log_sorted(log1) && is_log_sorted(log2),
        "Logs must be sorted before conflict detection!");

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

// Record a transactional operation in the txn log.
void client_t::log_txn_operation(
    gaia_locator_t locator,
    gaia_offset_t old_offset,
    gaia_offset_t new_offset)
{
    txn_log_t* txn_log = client_t::txn_log();
    if (txn_log->record_count == c_max_log_records)
    {
        throw transaction_object_limit_exceeded_internal();
    }

    // Initialize the new record and increment the record count.
    auto& lr = txn_log->log_records[txn_log->record_count++];
    // The log record sequence should start at 0.
    lr.sequence = txn_log->record_count - 1;
    lr.locator = locator;
    lr.old_offset = old_offset;
    lr.new_offset = new_offset;
}

// We prevent txn metadata from having its memory reclaimed during a scan by
// observing the post-GC watermark and reserving the timestamp that we observed.
// Since the post-GC watermark lags all other watermarks used for scans (i.e.,
// all but the pre-truncate watermark), we can safely scan any txn metadata
// range beginning at or after any watermark. The pre-truncate watermark cannot
// be advanced past the timestamp that we reserved (and thus no txn metadata
// after that timestamp can be reclaimed) until we call
// unprotect_txn_metadata().
void client_t::protect_txn_metadata()
{
    ASSERT_PRECONDITION(safe_ts_entries_index() <= safe_ts_entries_t::c_max_safe_ts_index,
        "safe_ts entries index should be valid!");
    ASSERT_PRECONDITION(get_safe_ts_entries(), "Expected safe_ts_entries structure to be mapped!");
    ASSERT_PRECONDITION(get_watermarks(), "Expected watermarks structure to be mapped!");

    // Loop until we successfully reserve the timestamp of the post-GC watermark
    // that we observed. Technically, there is no bound on the number of
    // iterations until success, so this is not wait-free, but in practice
    // failures should be very rare.
    while (true)
    {
        // Get a snapshot of the post-GC watermark.
        auto post_gc_watermark = get_watermarks()->get_watermark(watermark_type_t::post_gc);

        // Try to reserve the post-GC watermark's timestamp.
        if (get_safe_ts_entries()->reserve_safe_ts(
            safe_ts_entries_index(), post_gc_watermark, get_watermarks()))
        {
            // We successfully reserved the timestamp that we observed.
            break;
        }
    }
}

// Release the timestamp reserved in protect_txn_metadata(), allowing the
// pre-truncate watermark to advance past that timestamp. No txn metadata can be
// safely scanned after this is called and before protect_txn_metadata() is
// called again.
void client_t::unprotect_txn_metadata()
{
    ASSERT_PRECONDITION(safe_ts_entries_index() <= safe_ts_entries_t::c_max_safe_ts_index,
        "safe_ts entries index should be valid!");
    ASSERT_PRECONDITION(get_safe_ts_entries(), "Expected safe_ts_entries structure to be mapped!");
    ASSERT_PRECONDITION(get_watermarks(), "Expected watermarks structure to be mapped!");

    get_safe_ts_entries()->release_safe_ts(safe_ts_entries_index(), get_watermarks());
}

gaia_txn_id_t client_t::get_safe_watermark(watermark_type_t watermark_type)
{
    // We should use this interface only for scanning a range of txn metadata,
    // which should never require reading the pre-truncate watermark.
    ASSERT_PRECONDITION(watermark_type != watermark_type_t::pre_truncate,
        "Cannot use get_safe_watermark() to retrieve pre-truncate watermark!");
    ASSERT_PRECONDITION(safe_ts_entries_index() <= safe_ts_entries_t::c_max_safe_ts_index,
        "safe_ts entries index should be valid!");
    ASSERT_PRECONDITION(get_safe_ts_entries(), "Expected safe_ts_entries structure to be mapped!");
    ASSERT_PRECONDITION(get_watermarks(), "Expected watermarks structure to be mapped!");

    auto reserved_ts = get_safe_ts_entries()->get_reserved_ts(safe_ts_entries_index());
    ASSERT_PRECONDITION(reserved_ts.is_valid(), "Expected valid safe_ts to be reserved!");

    auto watermark_ts = get_watermarks()->get_watermark(watermark_type);
    if (watermark_ts.is_valid())
    {
        ASSERT_INVARIANT(reserved_ts <= watermark_ts, "Expected reserved safe_ts to precede watermark!");
    }

    return watermark_ts;
}
