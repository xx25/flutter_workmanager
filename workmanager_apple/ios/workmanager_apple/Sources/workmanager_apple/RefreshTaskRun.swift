//
//  RefreshTaskRun.swift
//  workmanager
//
//  The life of one BGAppRefreshTask: the record of what happened to it, the
//  resubmission of the next request, and the single place its completion
//  is decided.
//

#if os(iOS)
import BackgroundTasks
import Foundation
import UIKit

/// A persisted ring of what the plugin did with its refresh tasks.
///
/// BGTaskScheduler gives an app no way to ask afterwards whether a task was
/// delivered, whether the next request was accepted, or how a run ended, and
/// the plugin's own log lines are gone with the process. An app whose
/// background refresh "stops firing" has nothing to look at. The ring is
/// that record: the last `capacity` events, each with a sequence number and
/// a wall-clock time, readable through `WorkmanagerPlugin.refreshTaskLog()`.
enum RefreshTaskLog {
    static let capacity = 200

    /// One serial queue owns the ring: an append is a read-modify-write of a
    /// capped array plus a sequence number, and the launch handler runs off
    /// the main queue.
    static let queue = DispatchQueue(label: "\(WorkmanagerPlugin.identifier).refreshTaskLog")

    private static let defaults = UserDefaults(suiteName: "\(WorkmanagerPlugin.identifier).userDefaults")!
    private static let entriesKey = "\(WorkmanagerPlugin.identifier).refreshTaskLog"
    private static let sequenceKey = "\(WorkmanagerPlugin.identifier).refreshTaskLog.seq"

    /// Appends an event. Safe from any queue; returns at once.
    static func record(_ event: String, identifier: String? = nil, detail: String? = nil) {
        let at = Date()
        queue.async { append(event, identifier: identifier, detail: detail, at: at) }
    }

    /// Appends an event from code already on `queue`.
    static func recordOnQueue(_ event: String, identifier: String? = nil, detail: String? = nil) {
        dispatchPrecondition(condition: .onQueue(queue))
        append(event, identifier: identifier, detail: detail, at: Date())
    }

    private static func append(_ event: String, identifier: String?, detail: String?, at: Date) {
        var entries = defaults.array(forKey: entriesKey) as? [[String: Any]] ?? []
        let seq = defaults.integer(forKey: sequenceKey) + 1
        var entry: [String: Any] = [
            "seq": seq,
            "at": Int64(at.timeIntervalSince1970 * 1000),
            "event": event,
        ]
        if let identifier = identifier { entry["id"] = identifier }
        if let detail = detail { entry["detail"] = detail }
        entries.append(entry)
        if entries.count > capacity {
            entries.removeFirst(entries.count - capacity)
        }
        defaults.set(entries, forKey: entriesKey)
        defaults.set(seq, forKey: sequenceKey)
    }

    /// The ring, oldest first. Each entry: `seq` (Int), `at` (ms since the
    /// epoch), `event`, and optionally `id` and `detail`.
    static func entries() -> [[String: Any]] {
        return queue.sync { defaults.array(forKey: entriesKey) as? [[String: Any]] ?? [] }
    }
}

/// When a refresh task is told to stop, counted from the launch-handler
/// entry. iOS gives a BGAppRefreshTask about 30 seconds and kills an app
/// that overruns; neither instant is set unless the app asks.
struct RefreshTaskStops {
    /// The Dart side is asked to wind down (`onTaskStopped`).
    let softAfter: TimeInterval?
    /// The engine is destroyed and the task completed, whatever Dart is doing.
    let hardAfter: TimeInterval?
}

/// One delivered BGAppRefreshTask.
///
/// Three things can end it — the Dart task returning, the app's hard stop,
/// iOS expiring it — on three different queues, and `setTaskCompleted` must
/// be called exactly once. Every decision is taken on
/// `RefreshTaskLog.queue`, which is NOT the main queue on purpose: the
/// engine start runs synchronously on main, and a stop that needed main
/// could not fire while main is the thing that hangs.
@available(iOS 13.0, *)
final class RefreshTaskRun {
    private let task: BGAppRefreshTask
    private let identifier: String
    private var queue: DispatchQueue { RefreshTaskLog.queue }

    /// Set on the main queue once the worker exists; read there only.
    var worker: BackgroundWorker?

    private var completed = false
    /// A resubmission is still being retried: an ordinary completion waits
    /// for it, because after `setTaskCompleted` the process may be suspended
    /// at once and the retry lost.
    private var submitPending = false
    private var heldCompletion: Bool?

    /// How long after a failed submit the next attempt is made.
    static let submitRetryDelays: [TimeInterval] = [1, 3, 8]

    init(task: BGAppRefreshTask, identifier: String) {
        self.task = task
        self.identifier = identifier
    }

    // MARK: The next request

    /// Submits the next request now and, when iOS refuses it, again after
    /// each of `submitRetryDelays`. Without a pending request the periodic
    /// chain is over until the app is next opened.
    func submitNext(_ submit: @escaping () -> Error?) {
        queue.async {
            self.submitPending = true
            self.trySubmit(submit, attempt: 0)
        }
    }

    private func trySubmit(_ submit: @escaping () -> Error?, attempt: Int) {
        dispatchPrecondition(condition: .onQueue(queue))
        if let error = submit() {
            RefreshTaskLog.recordOnQueue(
                "submit failed", identifier: identifier, detail: error.localizedDescription)
            if attempt < RefreshTaskRun.submitRetryDelays.count, !completed {
                queue.asyncAfter(deadline: .now() + RefreshTaskRun.submitRetryDelays[attempt]) {
                    // Completed meanwhile by a stop or an expiration: the
                    // process may be gone before this runs at all.
                    self.trySubmit(submit, attempt: attempt + 1)
                }
                return
            }
        } else {
            RefreshTaskLog.recordOnQueue("submit ok", identifier: identifier)
        }
        submitPending = false
        if let success = heldCompletion {
            heldCompletion = nil
            complete(success: success)
        }
    }

    // MARK: The three ends

    /// The Dart task returned (or could not start). Waits for a
    /// resubmission still in flight.
    func finish(success: Bool) {
        queue.async {
            guard !self.completed else { return }
            if self.submitPending {
                self.heldCompletion = success
                return
            }
            self.complete(success: success)
        }
    }

    /// Arms the app's own stops, counted from `launchedAt`.
    func arm(_ stops: RefreshTaskStops, launchedAt: Date) {
        if let soft = stops.softAfter {
            queue.asyncAfter(deadline: .now() + max(0, launchedAt.addingTimeInterval(soft).timeIntervalSinceNow)) {
                self.softStop()
            }
        }
        if let hard = stops.hardAfter {
            queue.asyncAfter(deadline: .now() + max(0, launchedAt.addingTimeInterval(hard).timeIntervalSinceNow)) {
                self.hardStop()
            }
        }
    }

    /// Asks Dart to wind down. Nothing is forced.
    private func softStop() {
        dispatchPrecondition(condition: .onQueue(queue))
        guard !completed else { return }
        RefreshTaskLog.recordOnQueue("stop signal", identifier: identifier)
        DispatchQueue.main.async { self.worker?.requestStop() }
    }

    /// The run is over whatever Dart is doing: destroy the engine when main
    /// can be had within half a second, then complete. Cleanup comes first
    /// when it can — after `setTaskCompleted` the process may be suspended
    /// at once — but completion does not depend on it.
    private func hardStop() {
        dispatchPrecondition(condition: .onQueue(queue))
        guard !completed else { return }
        RefreshTaskLog.recordOnQueue("forced stop", identifier: identifier)
        let destroyed = DispatchSemaphore(value: 0)
        DispatchQueue.main.async {
            self.worker?.destroyEngine()
            destroyed.signal()
        }
        let cleaned = destroyed.wait(timeout: .now() + 0.5) == .success
        RefreshTaskLog.recordOnQueue(
            cleaned ? "engine destroyed" : "engine left (main queue busy)", identifier: identifier)
        complete(success: false)
    }

    /// iOS is taking the task back. Apple promises no time after this, so
    /// the task is completed first and nothing is waited for; the engine is
    /// destroyed afterwards if the process is still running.
    func expire() {
        queue.async {
            guard !self.completed else { return }
            RefreshTaskLog.recordOnQueue("expired", identifier: self.identifier)
            self.complete(success: false)
            DispatchQueue.main.async {
                self.worker?.requestStop()
                self.worker?.destroyEngine()
            }
        }
    }

    private func complete(success: Bool) {
        dispatchPrecondition(condition: .onQueue(queue))
        guard !completed else { return }
        completed = true
        task.setTaskCompleted(success: success)
        RefreshTaskLog.recordOnQueue(
            "completed", identifier: identifier, detail: success ? "success" : "failure")
    }
}
#endif
