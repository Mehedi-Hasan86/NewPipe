/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkRequest
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.Message
import android.util.Log
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.preference.PreferenceManager
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.schedulers.Schedulers
import java.io.IOException
import java.util.UUID
import org.schabi.newpipe.R
import org.schabi.newpipe.extractor.MediaFormat
import org.schabi.newpipe.extractor.exceptions.ReCaptchaException
import org.schabi.newpipe.extractor.stream.StreamInfo
import org.schabi.newpipe.streams.io.StoredDirectoryHelper
import org.schabi.newpipe.streams.io.StoredFileHelper
import org.schabi.newpipe.util.FilenameUtils
import org.schabi.newpipe.util.ListHelper
import us.shandian.giga.get.DownloadMission
import us.shandian.giga.get.MissionRecoveryInfo
import us.shandian.giga.service.DownloadManager
import us.shandian.giga.service.DownloadManagerService
import us.shandian.giga.service.MissionState

/** Single-consumption event wrapper for LiveData messages. */
class BatchEvent<out T>(private val content: T) {
    private var consumed = false

    fun consume(): T? {
        if (consumed) {
            return null
        }
        consumed = true
        return content
    }
}

/** UI state of one queued video. */
data class BatchItemUiState(
    val url: String,
    val title: String,
    val position: Int,
    val status: BatchItemStatus,
    val fraction: Double,
    val speedBytesPerSec: Long,
    val etaSeconds: Long,
    val errorText: String?,
    val retryCount: Int,
    val languageNote: String?
)

/** UI state of the whole batch. */
data class BatchUiState(
    val batchId: String?,
    val playlistName: String,
    val batchStatus: BatchStatus?,
    val waitingWifi: Boolean,
    val needsFolder: Boolean,
    val items: List<BatchItemUiState>,
    val completedCount: Int,
    val failedCount: Int,
    val totalCount: Int,
    val overallFraction: Double
)

/** One row of the batch queue screen (batch list). */
data class BatchSummary(
    val batchId: String,
    val playlistName: String,
    val queueState: BatchQueueState,
    val batchStatus: BatchStatus,
    val totalCount: Int,
    val completedCount: Int,
    val failedCount: Int,
    val overallFraction: Double,
    val createdAt: Long
)

/**
 * Executes playlist batch downloads on top of the existing GigaGet pipeline
 * ([DownloadManagerService]/[DownloadManager]/[DownloadMission]).
 *
 * At most [PlaylistDownloadOptions.parallelVideos] items are active at once;
 * the next item starts only when one finishes or fails. State persists in
 * [BatchStore] (JSON, app-private) and is reconciled with existing missions
 * on restart instead of duplicating them. No new downloader, no FFmpeg.
 */
class BatchDownloadManager private constructor(private val appContext: Context) {

    companion object {
        @Volatile
        private var instance: BatchDownloadManager? = null

        fun getInstance(context: Context): BatchDownloadManager {
            return instance ?: synchronized(this) {
                instance ?: BatchDownloadManager(context.applicationContext).also {
                    instance = it
                }
            }
        }

        private const val POLL_INTERVAL_MS = 1000L
        private const val RESOLVE_STAGGER_MS = 1500L
        private const val COOLDOWN_MS = 60_000L
        private const val RETRY_BACKOFF_UNIT_MS = 5000L
        private const val LOW_SPACE_WARN_BYTES = 100L * 1024L * 1024L

        /** HTTP 403 (DownloadMission.ERROR_HTTP_FORBIDDEN is package-private). */
        private const val HTTP_FORBIDDEN = 403

        private const val TAG = "PlaylistBatch"
    }

    private val mainHandler = Handler(Looper.getMainLooper())
    private val disposables = CompositeDisposable()

    private val _uiState = MutableLiveData(
        BatchUiState(null, "", null, false, false, emptyList(), 0, 0, 0, 0.0)
    )
    val uiState: LiveData<BatchUiState> = _uiState

    private val _batchList = MutableLiveData<List<BatchSummary>>(emptyList())
    val batchList: LiveData<List<BatchSummary>> = _batchList

    private val _messageEvent = MutableLiveData<BatchEvent<String>>()
    val messageEvent: LiveData<BatchEvent<String>> = _messageEvent

    /** Fired once when a download folder must be picked (pref key as content). */
    private val _folderEvent = MutableLiveData<BatchEvent<String>>()
    val folderEvent: LiveData<BatchEvent<String>> = _folderEvent

    /** Fired once when ASK_AT_END items await a decision. */
    private val _askLanguageEvent = MutableLiveData<BatchEvent<List<String>>>()
    val askLanguageEvent: LiveData<BatchEvent<List<String>>> = _askLanguageEvent

    private var record: BatchRecord? = null

    /**
     * Every non-ACTIVE batch in queue order: QUEUED batches (FIFO) first, then DONE
     * batches. Together with [record] this is the whole persisted queue.
     */
    private var otherBatches: List<BatchRecord> = emptyList()
    private var binder: DownloadManagerService.DownloadManagerBinder? = null
    private var bound = false
    private var pendingAfterBind: (() -> Unit)? = null
    private var lastResolveAt = 0L
    private var cooldownUntil = 0L
    private var wifiCallbackRegistered = false
    private var pendingFolderKey: String? = null

    /** Storage URI string -> live mission, matched from service events. */
    private val liveMissions = mutableMapOf<String, DownloadMission>()
    private val speedSamples = mutableMapOf<String, Pair<Long, Long>>()

    private val serviceConnection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
            binder = service as? DownloadManagerService.DownloadManagerBinder
            bound = binder != null
            binder?.addMissionEventListener(missionListener)
            reconcileAfterRestart()
            pendingAfterBind?.invoke()
            pendingAfterBind = null
            pump()
            startPolling()
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            bound = false
            binder = null
        }
    }

    private val missionListener = Handler.Callback { msg: Message ->
        onMissionMessage(msg.what, msg.obj as? DownloadMission)
        true
    }

    private val pollRunnable = object : Runnable {
        override fun run() {
            pollActiveMissions()
            if (record != null && !isTerminal(record!!.status)) {
                mainHandler.postDelayed(this, POLL_INTERVAL_MS)
            }
        }
    }

    private val wifiCallback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
            mainHandler.post { pump() }
        }

        override fun onLost(network: Network) {
            mainHandler.post { publish() }
        }
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Enqueues a new batch. Each batch keeps the options captured here at creation time
     * (folder, subfolder, quality, audio language, naming, duplicate policy).
     * When no batch is ACTIVE the new batch starts immediately, otherwise it is QUEUED
     * behind the existing batches and runs automatically when its turn comes
     * (sequential FIFO, oldest first).
     *
     * @return 0 when the batch became ACTIVE at once, otherwise its 1-based position
     * among the QUEUED batches
     */
    fun startBatch(
        playlistName: String,
        playlistUrl: String,
        serviceId: Int,
        items: List<PlaylistBatchItem>,
        options: PlaylistDownloadOptions,
        overrides: Map<String, PerItemOverride>
    ): Int {
        Log.d(TAG, "startBatch: playlistName=$playlistName, items=${items.size}")
        val now = System.currentTimeMillis()
        val fresh = BatchRecord(
            id = UUID.randomUUID().toString(),
            playlistName = playlistName,
            playlistUrl = playlistUrl,
            serviceId = serviceId,
            createdAt = now,
            status = BatchStatus.RUNNING,
            queueState = BatchQueueState.QUEUED,
            options = options,
            items = items.map { item ->
                BatchItemRecord(
                    url = item.url,
                    title = item.title,
                    position = item.position,
                    serviceId = item.serviceId,
                    uploaderName = item.uploaderName,
                    uploadDateText = item.uploadDateText,
                    override = overrides[item.url]
                )
            }
        )
        Log.d(TAG, "startBatch: created BatchRecord id=${fresh.id} with ${fresh.items.size} items")
        if (record == null) {
            if (otherBatches.any { it.queueState == BatchQueueState.QUEUED }) {
                // Should not happen (finish/advance keeps the invariant), but a new
                // batch must never jump the line: enqueue it, then start the oldest.
                otherBatches = otherBatches + fresh
                Log.d(TAG, "batch added id=${fresh.id} state=QUEUED")
                persist()
                activateNextBatch()
                return queuedPosition(fresh.id)
            }
            record = fresh.copy(queueState = BatchQueueState.ACTIVE)
            liveMissions.clear()
            speedSamples.clear()
            cooldownUntil = 0L
            Log.d(TAG, "batch added id=${fresh.id} state=ACTIVE")
            persist()
            ensureBound { pump() }
            checkLowSpace()
            return 0
        }
        otherBatches = otherBatches + fresh
        val position = queuedPosition(fresh.id)
        Log.d(TAG, "batch added id=${fresh.id} state=QUEUED position=$position")
        persist()
        return position
    }

    /** 1-based position of [batchId] among the QUEUED batches (0 when not queued). */
    private fun queuedPosition(batchId: String): Int {
        return otherBatches.filter { it.queueState == BatchQueueState.QUEUED }
            .indexOfFirst { it.id == batchId } + 1
    }

    /** Pauses the ACTIVE batch only; queued batches are untouched. */
    fun pauseBatch() {
        val current = record ?: return
        record = current.copy(status = BatchStatus.PAUSED)
        persist()
        pauseActiveMissions()
        publish()
    }

    /** Resumes the ACTIVE batch only; queued batches are untouched. */
    fun resumeBatch() {
        val current = record ?: return
        if (isTerminal(current.status)) {
            return
        }
        record = current.copy(status = BatchStatus.RUNNING)
        persist()
        ensureBound {
            resumeActiveMissions()
            pump()
        }
    }

    /**
     * Cancels the ACTIVE batch only (queued batches are untouched): deletes its live
     * missions, marks busy items CANCELLED, marks the batch DONE and automatically
     * starts the next QUEUED batch, if any.
     */
    fun cancelBatch() {
        val current = record ?: return
        deleteActiveMissions()
        val items = current.items.map { item ->
            if (isActiveStatus(item.status)) {
                item.copy(status = BatchItemStatus.CANCELLED)
            } else {
                item
            }
        }
        finishActiveBatch(current.copy(status = BatchStatus.CANCELLED, items = items))
    }

    /**
     * Clears a stuck ACTIVE batch: deletes all active missions, marks all non-completed
     * items as FAILED (so the user can retry), marks the batch DONE and starts the
     * next QUEUED batch, if any. Use this when the batch is permanently stuck.
     */
    fun clearStuckBatch() {
        val current = record ?: return
        Log.w(TAG, "clearStuckBatch: clearing stuck batch id=${current.id}")
        deleteActiveMissions()
        val items = current.items.map { item ->
            if (!isTerminalItem(item.status)) {
                item.copy(status = BatchItemStatus.FAILED, failureReason = "Batch cleared by user")
            } else {
                item
            }
        }
        finishActiveBatch(current.copy(status = BatchStatus.CANCELLED, items = items))
    }

    fun retryItem(url: String) {
        val current = record ?: return
        val items = current.items.map { item ->
            if (item.url == url && !isActiveStatus(item.status)) {
                item.copy(
                    status = BatchItemStatus.PENDING,
                    retryCount = 0,
                    failureReason = null,
                    lastProgress = 0.0
                )
            } else {
                item
            }
        }
        val status = if (isTerminal(current.status)) BatchStatus.RUNNING else current.status
        record = current.copy(status = status, items = items)
        persist()
        ensureBound { pump() }
    }

    fun cancelItem(url: String) {
        val current = record ?: return
        findLiveMission(url)?.let { mission ->
            try {
                binder?.getDownloadManager()?.deleteMission(mission, true)
            } catch (e: Exception) {
                // already gone
            }
            liveMissions.remove(missionKey(mission))
        }
        record = current.copy(
            items = current.items.map { item ->
                if (item.url == url && !isTerminalItem(item.status)) {
                    item.copy(status = BatchItemStatus.CANCELLED)
                } else {
                    item
                }
            }
        )
        persist()
        publish()
        pump()
    }

    /** Moves a PENDING item within the queue order. */
    fun movePending(fromQueueIndex: Int, toQueueIndex: Int) {
        val current = record ?: return
        val pending = current.items.filter { it.status == BatchItemStatus.PENDING }.toMutableList()
        if (fromQueueIndex !in pending.indices || toQueueIndex !in pending.indices) {
            return
        }
        val moved = pending.removeAt(fromQueueIndex)
        pending.add(toQueueIndex, moved)
        val iterator = pending.iterator()
        record = current.copy(
            items = current.items.map { item ->
                if (item.status == BatchItemStatus.PENDING) iterator.next() else item
            }
        )
        persist()
        publish()
    }

    /** Re-queues ASK_AT_END language-skipped items with the Original track. */
    fun retrySkippedWithOriginal(urls: List<String>) {
        val current = record ?: return
        val urlSet = urls.toSet()
        val items = current.items.map { item ->
            if (item.url in urlSet && item.status == BatchItemStatus.SKIPPED) {
                item.copy(
                    status = BatchItemStatus.PENDING,
                    retryCount = 0,
                    failureReason = null,
                    override = item.override?.copy(audioTrackId = null, audioTrackLanguage = null)
                )
            } else {
                item
            }
        }
        record = current.copy(
            status = BatchStatus.RUNNING,
            items = items,
            askLanguageUrls = current.askLanguageUrls - urlSet
        )
        persist()
        ensureBound { pump() }
    }

    /** Folder picker intent (SAF tree or legacy dir picker, like single download). */
    fun folderPickerIntent(): Intent {
        return StoredDirectoryHelper.getPicker(appContext)
    }

    /**
     * Called after the user picks a download folder: stores it under [prefKey]
     * (same keys the service reads) and resumes the batch.
     */
    fun onFolderPicked(prefKey: String, folderUri: android.net.Uri) {
        Log.d(TAG, "onFolderPicked: key=$prefKey uri=$folderUri")
        try {
            appContext.contentResolver.takePersistableUriPermission(
                folderUri,
                Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_WRITE_URI_PERMISSION
            )
            Log.d(TAG, "onFolderPicked: persistable permission recorded for $folderUri")
        } catch (e: Exception) {
            Log.e(TAG, "onFolderPicked: takePersistableUriPermission failed for $folderUri", e)
        }
        PreferenceManager.getDefaultSharedPreferences(appContext).edit()
            .putString(prefKey, folderUri.toString())
            .apply()
        pendingFolderKey = null
        val current = record ?: return
        if (current.status == BatchStatus.PAUSED) {
            record = current.copy(status = BatchStatus.RUNNING)
            persist()
        }
        ensureBound { pump() }
    }

    private fun onFolderMissing(prefKey: String) {
        pendingFolderKey = prefKey
        val current = record ?: return
        record = current.copy(
            items = current.items.map {
                if (isActiveStatus(it.status)) {
                    it.copy(status = BatchItemStatus.PENDING)
                } else {
                    it
                }
            },
            status = BatchStatus.PAUSED
        )
        persist()
        _folderEvent.value = BatchEvent(prefKey)
        postMessage(appContext.getString(R.string.playlist_batch_pick_folder))
    }

    /**
     * Deletes a batch. Queued and done batches are simply dropped. Deleting the ACTIVE
     * batch first stops its running missions (downloaded files are always kept), drops
     * the record without a DONE entry, and starts the next QUEUED batch, if any.
     *
     * @param batchId the id of the batch to delete
     */
    fun deleteBatch(batchId: String) {
        val active = record
        if (active != null && active.id == batchId) {
            Log.d(TAG, "delete active batch id=$batchId, stopping missions (keeping files)")
            deleteActiveMissions(alsoDeleteFile = false)
            record = null
            pendingFolderKey = null
            stopWifiCallback()
            persist()
            activateNextBatch()
            record?.let { Log.d(TAG, "next batch started id=${it.id} name=${it.playlistName}") }
            return
        }
        val target = otherBatches.firstOrNull { it.id == batchId } ?: return
        Log.d(TAG, "delete batch id=$batchId name=${target.playlistName} state=${target.queueState}")
        otherBatches = otherBatches.filterNot { it.id == batchId }
        persist()
    }

    /**
     * Re-queues every FAILED item of the batch (status back to PENDING, retry counters
     * and failure reasons cleared). A DONE batch goes back to QUEUED — or becomes ACTIVE
     * at once when nothing is active. ACTIVE and QUEUED batches keep their state
     * (a PAUSED active batch stays paused until resumed).
     *
     * @param batchId the id of the batch whose failed items should run again
     * @return how many items were re-queued
     */
    fun retryFailed(batchId: String): Int {
        val active = record
        if (active != null && active.id == batchId) {
            val failed = active.items.count { it.status == BatchItemStatus.FAILED }
            if (failed == 0) {
                return 0
            }
            val status = if (active.status == BatchStatus.PAUSED) {
                BatchStatus.PAUSED
            } else {
                BatchStatus.RUNNING
            }
            Log.d(TAG, "retry failed items batch=$batchId count=$failed state=ACTIVE")
            record = active.copy(
                status = status,
                items = active.items.map { resetFailedItem(it) }
            )
            persist()
            ensureBound { pump() }
            return failed
        }
        val target = otherBatches.firstOrNull { it.id == batchId } ?: return 0
        val failed = target.items.count { it.status == BatchItemStatus.FAILED }
        if (failed == 0) {
            return 0
        }
        val requeued = target.copy(
            status = BatchStatus.RUNNING,
            queueState = BatchQueueState.QUEUED,
            items = target.items.map { resetFailedItem(it) }
        )
        otherBatches = otherBatches.map { if (it.id == batchId) requeued else it }
        Log.d(TAG, "retry failed items batch=$batchId count=$failed state=QUEUED")
        persist()
        if (record == null) {
            activateNextBatch()
        }
        return failed
    }

    private fun resetFailedItem(item: BatchItemRecord): BatchItemRecord {
        return if (item.status == BatchItemStatus.FAILED) {
            item.copy(
                status = BatchItemStatus.PENDING,
                retryCount = 0,
                failureReason = null,
                lastProgress = 0.0
            )
        } else {
            item
        }
    }

    /**
     * Removes all DONE batches from the queue (downloaded files are kept).
     *
     * @return how many batches were removed
     */
    fun clearFinished(): Int {
        val done = otherBatches.filter { it.queueState == BatchQueueState.DONE }
        if (done.isEmpty()) {
            return 0
        }
        Log.d(TAG, "clear finished batches count=${done.size}")
        otherBatches = otherBatches.filterNot { it.queueState == BatchQueueState.DONE }
        persist()
        return done.size
    }

    /**
     * Moves a QUEUED batch up ([delta] negative) or down ([delta] positive) within the
     * queued section. DONE batches and the ACTIVE batch never move.
     *
     * @param batchId the id of the queued batch to move
     * @param delta positions to move: negative moves up, positive moves down
     */
    fun moveQueuedBatch(batchId: String, delta: Int) {
        val queuedIds = otherBatches
            .filter { it.queueState == BatchQueueState.QUEUED }
            .map { it.id }
        val from = queuedIds.indexOf(batchId)
        if (from < 0 || delta == 0) {
            return
        }
        val to = (from + delta).coerceIn(queuedIds.indices)
        if (to == from) {
            return
        }
        val mutable = otherBatches.toMutableList()
        val fromPos = mutable.indexOfFirst { it.id == queuedIds[from] }
        val toPos = mutable.indexOfFirst { it.id == queuedIds[to] }
        if (fromPos < 0 || toPos < 0) {
            return
        }
        val moved = mutable.removeAt(fromPos)
        mutable.add(toPos, moved)
        otherBatches = mutable
        Log.d(TAG, "moved queued batch id=$batchId from=$from to=$to")
        persist()
    }

    /** Loads persisted state for the queue screen (no-op when already loaded). */
    fun restoreIfNeeded() {
        if (record != null) {
            Log.d(TAG, "restoreIfNeeded: record already in memory, publishing")
            publish()
            return
        }
        val loaded = BatchStore.loadQueue(appContext)
        Log.d(TAG, "restoreIfNeeded: loaded ${loaded.size} batches from store")
        if (loaded.isEmpty()) {
            publish()
            return
        }
        val actives = loaded.filter { it.queueState == BatchQueueState.ACTIVE }
        val firstActive = actives.firstOrNull()
        val demoted = actives.drop(1).map { it.copy(queueState = BatchQueueState.QUEUED) }
        if (demoted.isNotEmpty()) {
            Log.d(TAG, "restoreIfNeeded: demoted ${demoted.size} extra ACTIVE batches to QUEUED")
        }
        val queued = loaded.filter { it.queueState == BatchQueueState.QUEUED }
        val done = loaded.filter { it.queueState == BatchQueueState.DONE }
        otherBatches = demoted + queued + done
        if (firstActive != null && !isTerminal(firstActive.status)) {
            record = firstActive
        } else {
            if (firstActive != null) {
                Log.d(
                    TAG,
                    "restoreIfNeeded: persisted ACTIVE batch ${firstActive.id} " +
                        "is terminal, marking DONE"
                )
                otherBatches = otherBatches + firstActive.copy(queueState = BatchQueueState.DONE)
            }
            // No ACTIVE batch after a restart: start the oldest queued one.
            activateNextBatch()
        }
        val restored = record
        Log.d(
            TAG,
            "restoreIfNeeded: active=${restored?.id} " +
                "queued=${otherBatches.count { it.queueState == BatchQueueState.QUEUED }} " +
                "done=${otherBatches.count { it.queueState == BatchQueueState.DONE }}"
        )
        persist()
        if (restored != null && !isTerminal(restored.status)) {
            ensureBound { pump() }
        }
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    private fun ensureBound(after: () -> Unit) {
        if (bound) {
            after()
            return
        }
        pendingAfterBind = after
        try {
            appContext.bindService(
                Intent(appContext, DownloadManagerService::class.java),
                serviceConnection,
                Context.BIND_AUTO_CREATE
            )
        } catch (e: Exception) {
            postMessage(appContext.getString(R.string.playlist_batch_service_bind_failed))
        }
    }

    private fun manager(): DownloadManager? = binder?.getDownloadManager()

    private fun isTerminal(status: BatchStatus): Boolean {
        return status == BatchStatus.COMPLETED || status == BatchStatus.CANCELLED
    }

    private fun isTerminalItem(status: BatchItemStatus): Boolean {
        return status == BatchItemStatus.COMPLETED || status == BatchItemStatus.CANCELLED
    }

    private fun isActiveStatus(status: BatchItemStatus): Boolean {
        return status == BatchItemStatus.RESOLVING ||
            status == BatchItemStatus.DOWNLOADING ||
            status == BatchItemStatus.MERGING
    }

    private fun activeCount(): Int {
        return record?.items?.count { isActiveStatus(it.status) } ?: 0
    }

    private fun persist() {
        try {
            BatchStore.saveQueue(appContext, listOfNotNull(record) + otherBatches)
        } catch (e: Exception) {
            // store is best-effort; the batches continue in memory
        }
        publish()
    }

    // ---------------- loop ----------------

    private fun pump() {
        val current = record ?: return
        Log.d(TAG, "pump: status=${current.status}, activeCount=${activeCount()}, pendingCount=${current.items.count { it.status == BatchItemStatus.PENDING }}, parallel=${current.options.parallelVideos}")
        if (!bound || manager() == null) {
            Log.d(TAG, "pump: not bound or no manager, rebinding")
            ensureBound { pump() }
            return
        }
        if (current.status == BatchStatus.PAUSED || isTerminal(current.status)) {
            Log.d(TAG, "pump: batch paused or terminal, publishing")
            publish()
            return
        }
        if (System.currentTimeMillis() < cooldownUntil) {
            record = current.copy(status = BatchStatus.COOLDOWN)
            persist()
            mainHandler.postDelayed({ pump() }, cooldownUntil - System.currentTimeMillis())
            return
        } else if (current.status == BatchStatus.COOLDOWN) {
            record = current.copy(status = BatchStatus.RUNNING)
            persist()
        }
        val options = current.options
        if (options.wifiOnly && ListHelper.isMeteredNetwork(appContext)) {
            if (current.status != BatchStatus.WAITING_WIFI) {
                record = current.copy(status = BatchStatus.WAITING_WIFI)
                persist()
                postMessage(appContext.getString(R.string.playlist_batch_waiting_wifi))
            }
            startWifiCallback()
            publish()
            return
        }
        if (current.status == BatchStatus.WAITING_WIFI) {
            record = current.copy(status = BatchStatus.RUNNING)
            persist()
        }

        var started = false
        while (activeCount() < options.parallelVideos.coerceIn(1, 3)) {
            val next = record?.items?.firstOrNull { it.status == BatchItemStatus.PENDING }
                ?: break
            processItem(next)
            started = true
        }
        if (!started) {
            checkBatchDone()
        }
        publish()
    }

    private fun schedulePump(delayMs: Long) {
        mainHandler.removeCallbacks(pumpRunnable)
        mainHandler.postDelayed(pumpRunnable, delayMs)
    }

    private val pumpRunnable = Runnable { pump() }

    private fun processItem(item: BatchItemRecord) {
        setItemStatus(item.url, BatchItemStatus.RESOLVING, null)
        val wait = RESOLVE_STAGGER_MS - (System.currentTimeMillis() - lastResolveAt)
        val run: () -> Unit = {
            lastResolveAt = System.currentTimeMillis()
            val d = io.reactivex.rxjava3.core.Single.fromCallable {
                val info = org.schabi.newpipe.extractor.stream.StreamInfo.getInfo(
                    org.schabi.newpipe.extractor.NewPipe.getService(item.serviceId),
                    item.url
                )
                info
            }
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe({ info -> onStreamInfo(item.url, info) }, { error ->
                    onResolveError(item.url, error)
                })
            disposables.add(d)
        }
        if (wait > 0) {
            mainHandler.postDelayed(run, wait)
        } else {
            run()
        }
    }

    private fun onStreamInfo(url: String, info: StreamInfo) {
        val current = record ?: return
        val item = current.items.firstOrNull { it.url == url } ?: return
        if (item.status != BatchItemStatus.RESOLVING) {
            return
        }
        val options = current.options
        val override = item.override
        val targetResolution = override?.targetResolution ?: options.targetResolution
        val container = override?.videoContainer ?: options.videoContainer

        try {
            if (options.mode == PlaylistBatchMode.AUDIO_ONLY) {
                startAudioMission(item, info, options, override, container)
            } else {
                startVideoMission(item, info, options, override, targetResolution, container)
            }
        } catch (e: LanguageMissing) {
            applyLanguageFallback(item, e.availableLanguages)
        } catch (e: FolderMissingException) {
            onFolderMissing(e.prefKey)
        } catch (e: ResolveFailed) {
            Log.e(TAG, "onStreamInfo: item=${item.url} resolve failed: ${e.message}")
            failItem(item, e.message, canRetry = true)
        } catch (e: Exception) {
            Log.e(TAG, "onStreamInfo: item=${item.url} failed", e)
            failItem(item, "${e.javaClass.simpleName}: ${e.message}", canRetry = true)
        }
    }

    private class ResolveFailed(message: String?) : Exception(message)

    private class LanguageMissing(val availableLanguages: List<String>) : Exception()

    // ---------------- mission creation ----------------

    private fun startVideoMission(
        item: BatchItemRecord,
        info: StreamInfo,
        options: PlaylistDownloadOptions,
        override: PerItemOverride?,
        targetResolution: String,
        container: PlaylistVideoContainer
    ) {
        val resolvedVideo = PlaylistBatchResolver.resolveVideo(
            appContext,
            info,
            targetResolution,
            container,
            options.fallbackToBest
        ) ?: throw ResolveFailed(
            appContext.getString(R.string.playlist_batch_fail_no_video)
        )
        val audioContainer = if (container == PlaylistVideoContainer.MP4) {
            PlaylistAudioContainer.M4A
        } else {
            PlaylistAudioContainer.OPUS
        }
        val audioChoice = PlaylistBatchResolver.resolveAudio(
            appContext,
            info,
            audioContainer,
            options.preferHighestBitrate,
            override?.audioTrackLanguage ?: options.preferredAudioLanguage,
            override?.audioTrackId
        )
        val secondary = when (audioChoice) {
            is PlaylistBatchResolver.AudioResolution.Chosen -> audioChoice.stream

            is PlaylistBatchResolver.AudioResolution.MissingLanguage ->
                throw LanguageMissing(audioChoice.available.map { it.displayName })

            is PlaylistBatchResolver.AudioResolution.NoAudio -> null
        }
        val main = resolvedVideo.main
        val muxAudio = if (main.isVideoOnly) {
            secondary ?: resolvedVideo.secondaryAudio
                ?: throw ResolveFailed(appContext.getString(R.string.playlist_batch_fail_no_audio))
        } else {
            null
        }
        val urls: Array<String>
        val recovery: ArrayList<MissionRecoveryInfo>
        val psName: String?
        if (muxAudio != null) {
            urls = arrayOf(main.content, muxAudio.content)
            recovery = arrayListOf(MissionRecoveryInfo(main), MissionRecoveryInfo(muxAudio))
            psName = resolvedVideo.postprocessingName
        } else {
            urls = arrayOf(main.content)
            recovery = arrayListOf(MissionRecoveryInfo(main))
            psName = null
        }
        val lang = languageOf(override, options, secondary)
        val filename = PlaylistBatchResolver.buildFileName(
            appContext,
            options.filenameTemplate,
            toBatchItem(item),
            maxOf(record?.items?.size ?: 1, 1),
            options,
            override,
            lang
        )
        // DownloadDialog passes the concrete stream mime (e.g. video/mp4), never "video/*".
        val videoMime = if (container == PlaylistVideoContainer.MP4) "video/mp4" else "video/webm"
        launchMission(item, info, urls, 'v', psName, null, recovery, filename, videoMime)
        maybeStartSubtitle(item, info, options, filename)
    }

    private fun startAudioMission(
        item: BatchItemRecord,
        info: StreamInfo,
        options: PlaylistDownloadOptions,
        override: PerItemOverride?,
        container: PlaylistVideoContainer
    ) {
        val audioContainer = if (container == PlaylistVideoContainer.MP4) {
            PlaylistAudioContainer.M4A
        } else {
            PlaylistAudioContainer.OPUS
        }
        val audioChoice = PlaylistBatchResolver.resolveAudio(
            appContext,
            info,
            audioContainer,
            options.preferHighestBitrate,
            override?.audioTrackLanguage ?: options.preferredAudioLanguage,
            override?.audioTrackId
        )
        val stream = when (audioChoice) {
            is PlaylistBatchResolver.AudioResolution.Chosen -> audioChoice.stream

            is PlaylistBatchResolver.AudioResolution.MissingLanguage ->
                throw LanguageMissing(audioChoice.available.map { it.displayName })

            is PlaylistBatchResolver.AudioResolution.NoAudio ->
                throw ResolveFailed(appContext.getString(R.string.playlist_batch_fail_no_audio))
        }
        val lang = languageOf(override, options, stream)
        val filename = PlaylistBatchResolver.buildFileName(
            appContext,
            options.filenameTemplate,
            toBatchItem(item),
            maxOf(record?.items?.size ?: 1, 1),
            options,
            override,
            lang
        )
        val mime = if (stream.format == MediaFormat.M4A) "audio/mp4" else "audio/ogg"
        launchMission(
            item, info, arrayOf(stream.content), 'a',
            PlaylistBatchResolver.audioPostprocessing(stream), null,
            arrayListOf(MissionRecoveryInfo(stream)), filename, mime
        )
    }

    private fun languageOf(
        override: PerItemOverride?,
        options: PlaylistDownloadOptions,
        stream: org.schabi.newpipe.extractor.stream.AudioStream?
    ): String? {
        override?.audioTrackLanguage?.let { return it }
        if (!options.preferredAudioLanguage.equals(
                PlaylistDownloadOptions.AUDIO_LANGUAGE_ORIGINAL,
                ignoreCase = true
            )
        ) {
            return options.preferredAudioLanguage
        }
        return stream?.audioLocale?.language
    }

    private fun maybeStartSubtitle(
        item: BatchItemRecord,
        info: StreamInfo,
        options: PlaylistDownloadOptions,
        videoFilename: String
    ) {
        if (!options.subtitlesEnabled) {
            return
        }
        val sub = PlaylistBatchResolver.resolveSubtitle(
            info,
            options.subtitleLanguages,
            options.preferManualSubtitles,
            options.includeAutoGenerated
        ) ?: return
        val base = videoFilename.substringBeforeLast('.')
        val suffix = if (sub.format == MediaFormat.TTML) {
            ".srt"
        } else {
            sub.format?.getSuffix() ?: ".srt"
        }
        val filename = "$base-${sub.languageTag}$suffix"
        val (psName, psArgs) = PlaylistBatchResolver.subtitlePostprocessing(sub)
        try {
            launchMission(
                item, info, arrayOf(sub.content), 's', psName, psArgs,
                arrayListOf(MissionRecoveryInfo(sub)), filename, "text/plain",
                isSubtitle = true
            )
        } catch (e: Exception) {
            // Subtitles are best-effort and never fail the item.
        }
    }

    /**
     * Creates the target file (duplicate policy applied) and starts the mission
     * through [DownloadManagerService.startMission]. Throws on storage problems.
     */
    @Throws(IOException::class)
    private fun launchMission(
        item: BatchItemRecord,
        info: StreamInfo,
        urls: Array<String>,
        kind: Char,
        psName: String?,
        psArgs: Array<String>?,
        recovery: ArrayList<MissionRecoveryInfo>,
        filename: String,
        mime: String,
        isSubtitle: Boolean = false
    ) {
        val manager = manager()
            ?: throw IOException(appContext.getString(R.string.playlist_batch_service_bind_failed))
        val audioOnly = record?.options?.mode == PlaylistBatchMode.AUDIO_ONLY || kind == 's'
        val dir = targetDirectory(audioOnly, item)
        Log.d(TAG, "launchMission: item=${item.url} dir=${dir.uri} name=$filename mime=$mime")
        val existingUri = try {
            dir.findFile(filename)
        } catch (e: Exception) {
            Log.e(TAG, "launchMission: findFile failed for $filename in ${dir.uri}", e)
            throw IOException(
                appContext.getString(R.string.playlist_batch_fail_storage) +
                    " ${e.javaClass.simpleName}: ${e.message}",
                e
            )
        }

        val storage: StoredFileHelper
        if (existingUri == null) {
            storage = createBatchFile(dir, filename, mime, unique = false)
        } else {
            when (record?.options?.duplicateAction ?: DuplicateAction.OVERWRITE) {
                DuplicateAction.SKIP -> {
                    if (!isSubtitle) {
                        skipItem(item, appContext.getString(R.string.playlist_batch_skipped_exists))
                    }
                    return
                }

                DuplicateAction.RENAME -> {
                    storage = createBatchFile(dir, filename, mime, unique = true)
                }

                DuplicateAction.OVERWRITE -> {
                    storage = StoredFileHelper(appContext, dir.uri, existingUri, dir.tag)
                    val state = manager.checkForExistingMission(storage)
                    if (state == MissionState.Finished || state == MissionState.Pending) {
                        manager.forgetMission(storage)
                    }
                    if (storage.length() > 0) {
                        storage.truncate()
                    }
                }
            }
        }

        val threads = PreferenceManager.getDefaultSharedPreferences(appContext)
            .getInt(appContext.getString(R.string.default_download_threads), 3)
        Log.d(
            TAG,
            "launchMission: batch=${record?.id} kind=$kind file=${storage.uri} " +
                "parent=${storage.parentUri} tag=${storage.tag} urls=${urls.size} " +
                "ps=$psName item=${item.url}"
        )
        DownloadManagerService.startMission(
            appContext, urls, storage, kind, threads, info, psName, psArgs, 0L, recovery
        )
        if (!isSubtitle) {
            setItemStorage(
                item.url,
                storage.uri.toString(),
                storage.tag,
                kind.toString(),
                BatchItemStatus.DOWNLOADING
            )
        }
    }

    /**
     * Creates the target file exactly like DownloadDialog does, but keeps the real cause:
     * the helper records it (see StoredDirectoryHelper.consumeCreateError) and it is
     * logged with a stack trace and appended to the failure reason.
     *
     * @param dir the directory helper from [targetDirectory]
     * @param filename the sanitized file name
     * @param mime the concrete stream mime type
     * @param unique true for createUniqueFile, false for createFile
     * @return the created file helper, never null
     * @throws IOException with the real failure cause in its message when creation fails
     */
    @Throws(IOException::class)
    private fun createBatchFile(
        dir: StoredDirectoryHelper,
        filename: String,
        mime: String,
        unique: Boolean
    ): StoredFileHelper {
        val created = if (unique) {
            dir.createUniqueFile(filename, mime)
        } else {
            dir.createFile(filename, mime)
        }
        if (created != null) {
            Log.d(TAG, "createBatchFile: created ${created.uri} parent=${created.parentUri}")
            return created
        }
        val cause = dir.consumeCreateError()
        Log.e(
            TAG,
            "createBatchFile: returned null dir=${dir.uri} name=$filename " +
                "mime=$mime unique=$unique",
            cause
        )
        val detail = if (cause == null) {
            ""
        } else {
            " ${cause.javaClass.simpleName}: ${cause.message}"
        }
        throw IOException(
            appContext.getString(R.string.playlist_batch_fail_storage) + detail,
            cause
        )
    }

    /** Main video/audio folder, or the playlist subfolder inside it. */
    @Throws(IOException::class)
    private fun targetDirectory(audioOnly: Boolean, item: BatchItemRecord): StoredDirectoryHelper {
        val prefs = PreferenceManager.getDefaultSharedPreferences(appContext)
        val key = if (audioOnly) {
            appContext.getString(R.string.download_path_audio_key)
        } else {
            appContext.getString(R.string.download_path_video_key)
        }
        val path = prefs.getString(key, null)
        if (path.isNullOrBlank()) {
            throw FolderMissingException(key)
        }
        val main: StoredDirectoryHelper
        try {
            main = StoredDirectoryHelper(
                appContext,
                android.net.Uri.parse(path),
                if (audioOnly) DownloadManager.TAG_AUDIO else DownloadManager.TAG_VIDEO
            )
        } catch (e: Exception) {
            throw FolderMissingException(key)
        }
        val options = record?.options
        if (options?.usePlaylistSubfolder != true) {
            Log.d(TAG, "targetDirectory: using root folder $path")
            return main
        }
        // FilenameUtils applies the user's charset; additionally strip '!' and trailing
        // dots/spaces, which SAF providers and Windows-style names reject
        // (e.g. "Switching to Linux Challenge!").
        val subName = FilenameUtils.createFilename(
            appContext,
            record?.playlistName.orEmpty().ifBlank { item.title }
        ).replace("!", "").trim().trimEnd('.', ' ').ifBlank { "playlist" }
        Log.d(TAG, "targetDirectory: root=$path subfolder=$subName")
        val sub = main.getSubfolder(
            appContext,
            subName,
            if (audioOnly) DownloadManager.TAG_AUDIO else DownloadManager.TAG_VIDEO
        )
        Log.d(TAG, "targetDirectory: dirUri=${sub.uri} tag=${sub.tag}")
        return sub
    }

    class FolderMissingException(val prefKey: String) : IOException()

    // ---------------- failures / retry ----------------

    private fun onResolveError(url: String, error: Throwable) {
        if (isRateLimited(error)) {
            startCooldown()
            setItemStatus(url, BatchItemStatus.PENDING, null)
            return
        }
        Log.e(TAG, "onResolveError: url=$url", error)
        failItem(url, "${error.javaClass.simpleName}: ${error.message}", canRetry = true)
    }

    private fun failItem(item: BatchItemRecord, reason: String?, canRetry: Boolean) {
        failItem(item.url, reason, canRetry)
    }

    private fun failItem(url: String, reason: String?, canRetry: Boolean) {
        val current = record ?: return
        val item = current.items.firstOrNull { it.url == url } ?: return
        Log.w(TAG, "failItem: url=$url reason=$reason canRetry=$canRetry retry=${item.retryCount}")
        val options = current.options
        if (canRetry && item.retryCount < options.maxRetries) {
            val items = current.items.map {
                if (it.url == url) {
                    it.copy(
                        status = BatchItemStatus.PENDING,
                        retryCount = it.retryCount + 1,
                        failureReason = reason
                    )
                } else {
                    it
                }
            }
            record = current.copy(items = items)
            persist()
            schedulePump(item.retryCount * RETRY_BACKOFF_UNIT_MS)
            return
        }
        setItemStatus(
            url,
            BatchItemStatus.FAILED,
            reason ?: appContext.getString(R.string.playlist_batch_fail_unknown)
        )
        pump()
    }

    private fun skipItem(item: BatchItemRecord, reason: String) {
        setItemStatus(item.url, BatchItemStatus.SKIPPED, reason)
        pump()
    }

    private fun applyLanguageFallback(
        item: BatchItemRecord,
        availableLanguages: List<String>
    ) {
        val options = record?.options ?: return
        when (options.missingLanguageAction) {
            MissingLanguageAction.USE_ORIGINAL -> {
                // Retry this item forced to the Original track.
                val cleared = item.override?.copy(
                    audioTrackId = null,
                    audioTrackLanguage = PlaylistDownloadOptions.AUDIO_LANGUAGE_ORIGINAL
                ) ?: PerItemOverride(
                    audioTrackLanguage = PlaylistDownloadOptions.AUDIO_LANGUAGE_ORIGINAL
                )
                val current = record ?: return
                record = current.copy(
                    items = current.items.map {
                        if (it.url == item.url) {
                            it.copy(
                                status = BatchItemStatus.PENDING,
                                override = cleared
                            )
                        } else {
                            it
                        }
                    }
                )
                persist()
                pump()
            }

            MissingLanguageAction.SKIP_ITEM -> {
                skipItem(
                    item,
                    appContext.getString(R.string.playlist_batch_fail_language)
                )
            }

            MissingLanguageAction.ASK_AT_END -> {
                val current = record ?: return
                record = current.copy(
                    items = current.items.map {
                        if (it.url == item.url) {
                            it.copy(
                                status = BatchItemStatus.SKIPPED,
                                failureReason = appContext.getString(
                                    R.string.playlist_batch_fail_language
                                )
                            )
                        } else {
                            it
                        }
                    },
                    askLanguageUrls = (current.askLanguageUrls + item.url).distinct()
                )
                persist()
                pump()
            }
        }
    }

    private fun isRateLimited(error: Throwable): Boolean {
        var cause: Throwable? = error
        while (cause != null) {
            if (cause is ReCaptchaException) {
                return true
            }
            cause = cause.cause
        }
        val message = (error.message ?: "").lowercase()
        return message.contains("captcha") || message.contains("429")
    }

    private fun startCooldown() {
        cooldownUntil = System.currentTimeMillis() + COOLDOWN_MS
        postMessage(appContext.getString(R.string.playlist_batch_cooldown))
        pump()
    }

    // ---------------- mission events ----------------

    private fun missionKey(mission: DownloadMission): String {
        return try {
            mission.storage.uri.toString()
        } catch (e: Exception) {
            ""
        }
    }

    private fun findLiveMission(url: String): DownloadMission? {
        val current = record ?: return null
        val storageUri = current.items.firstOrNull { it.url == url }?.storageUri
            ?: return liveMissions.values.firstOrNull { it.source == url }
        return liveMissions[storageUri]
    }

    private fun onMissionMessage(what: Int, mission: DownloadMission?): Boolean {
        if (mission == null) {
            return false
        }
        val key = missionKey(mission)
        if (key.isNotBlank()) {
            if (what == DownloadManagerService.MESSAGE_DELETED) {
                liveMissions.remove(key)
                return true
            }
            liveMissions[key] = mission
        }
        val current = record ?: return false
        val item = current.items.firstOrNull { batchItem ->
            batchItem.storageUri != null && batchItem.storageUri == key
        } ?: return false
        if (!isActiveStatus(item.status)) {
            return false
        }
        when (what) {
            DownloadManagerService.MESSAGE_RUNNING -> {
                if (item.status != BatchItemStatus.DOWNLOADING) {
                    setItemStatus(item.url, BatchItemStatus.DOWNLOADING, null)
                }
            }

            DownloadManagerService.MESSAGE_FINISHED -> {
                if (mission.isFinished) {
                    Log.d(TAG, "mission finished: batch=${current.id} url=${item.url} file=$key")
                    setItemStatus(item.url, BatchItemStatus.COMPLETED, null)
                    liveMissions.remove(key)
                    pump()
                }
            }

            DownloadManagerService.MESSAGE_ERROR -> {
                if (mission.isRecovering) {
                    return true
                }
                val code = mission.errCode
                val error = mission.errObject
                if (error != null && isRateLimited(error)) {
                    startCooldown()
                    setItemStatus(item.url, BatchItemStatus.PENDING, null)
                } else if (code == HTTP_FORBIDDEN ||
                    code == DownloadMission.ERROR_RESOURCE_GONE
                ) {
                    // The mission's own recover mechanism already re-resolved once;
                    // resolve fresh with the same choices.
                    failItem(item, error?.message, canRetry = true)
                } else {
                    failItem(item, error?.message ?: missionErrorText(code), canRetry = true)
                }
            }

            DownloadManagerService.MESSAGE_PAUSED -> publish()
        }
        return true
    }

    private fun missionErrorText(code: Int): String {
        return appContext.getString(R.string.playlist_batch_fail_mission, code)
    }

    // ---------------- polling / progress ----------------

    private fun startPolling() {
        mainHandler.removeCallbacks(pollRunnable)
        mainHandler.post(pollRunnable)
    }

    private fun pollActiveMissions() {
        val current = record ?: return
        if (isTerminal(current.status)) {
            return
        }
        var changed = false
        val items = current.items.map { item ->
            if (!isActiveStatus(item.status)) {
                return@map item
            }
            val mission = findLiveMission(item.url) ?: return@map item
            val length = mission.getLength().coerceAtLeast(0L)
            val done = mission.done.coerceAtLeast(0L)
            val fraction = if (length > 0) (done.toDouble() / length).coerceIn(0.0, 1.0) else 0.0
            var updated = item.copy(lastProgress = fraction)
            if (mission.isPsRunning && item.status != BatchItemStatus.MERGING) {
                updated = updated.copy(status = BatchItemStatus.MERGING)
            }
            if (updated.status != item.status || updated.lastProgress != item.lastProgress) {
                changed = true
            }
            updated
        }
        if (changed) {
            record = current.copy(items = items)
            persist()
        } else {
            publish()
        }
    }

    private fun speedFor(url: String, done: Long): Pair<Long, Long> {
        val now = System.currentTimeMillis()
        val last = speedSamples[url]
        speedSamples[url] = Pair(now, done)
        if (last == null) {
            return Pair(0L, -1L)
        }
        val dt = (now - last.first).coerceAtLeast(1L)
        val speed = (done - last.second).coerceAtLeast(0L) * 1000L / dt
        val mission = findLiveMission(url)
        val length = mission?.getLength()?.coerceAtLeast(0L) ?: 0L
        val eta = if (speed > 0 && length > done) (length - done) / speed else -1L
        return Pair(speed, eta)
    }

    // ---------------- restart reconciliation ----------------

    private fun reconcileAfterRestart() {
        val current = record ?: return
        if (isTerminal(current.status)) {
            Log.d(TAG, "reconcileAfterRestart: batch already terminal, skipping")
            publish()
            return
        }
        Log.d(TAG, "reconcileAfterRestart: reconciling batch id=${current.id}, status=${current.status}, items=${current.items.size}")
        val manager = manager() ?: return
        val iterator = manager.iterator
        val liveByStorage = mutableMapOf<String, DownloadMission>()
        for (i in 0 until iterator.oldListSize) {
            val mission = iterator.getItem(i).mission as? DownloadMission ?: continue
            try {
                liveByStorage[mission.storage.uri.toString()] = mission
            } catch (e: Exception) {
                // ignore missions without readable storage
            }
        }
        val items = current.items.map { item ->
            if (!isActiveStatus(item.status)) {
                return@map item
            }
            val storageUri = item.storageUri
            val mission = storageUri?.let { liveByStorage[it] }
            if (mission == null) {
                Log.d(TAG, "reconcileAfterRestart: item ${item.url} has no live mission, marking PENDING for retry")
                item.copy(status = BatchItemStatus.PENDING, failureReason = null)
            } else {
                // A mission is only found through a non-null storage URI.
                liveMissions[requireNotNull(storageUri)] = mission
                when {
                    mission.isFinished -> {
                        Log.d(TAG, "reconcileAfterRestart: mission finished for ${item.url}")
                        item.copy(status = BatchItemStatus.COMPLETED, lastProgress = 1.0)
                    }

                    mission.running || mission.enqueued -> {
                        Log.d(TAG, "reconcileAfterRestart: mission running/enqueued for ${item.url}")
                        item.copy(status = BatchItemStatus.DOWNLOADING)
                    }

                    else -> {
                        Log.d(TAG, "reconcileAfterRestart: mission not running for ${item.url}, marking PENDING")
                        item.copy(status = BatchItemStatus.PENDING, failureReason = null)
                    }
                }
            }
        }
        val paused = current.status == BatchStatus.PAUSED
        val newStatus = when {
            paused -> BatchStatus.PAUSED
            else -> BatchStatus.RUNNING
        }
        Log.d(TAG, "reconcileAfterRestart: new status=$newStatus")
        record = current.copy(status = newStatus, items = items)
        persist()
    }

    // ---------------- helpers ----------------

    private fun setItemStatus(url: String, status: BatchItemStatus, reason: String?) {
        val current = record ?: return
        record = current.copy(
            items = current.items.map {
                if (it.url == url) {
                    it.copy(
                        status = status,
                        failureReason = reason ?: it.failureReason,
                        lastProgress = if (status == BatchItemStatus.COMPLETED) {
                            1.0
                        } else {
                            it.lastProgress
                        }
                    )
                } else {
                    it
                }
            }
        )
        persist()
        if (status == BatchItemStatus.COMPLETED || status == BatchItemStatus.FAILED ||
            status == BatchItemStatus.SKIPPED
        ) {
            checkBatchDone()
        }
    }

    private fun setItemStorage(
        url: String,
        storageUri: String,
        tag: String?,
        kind: String,
        status: BatchItemStatus
    ) {
        val current = record ?: return
        record = current.copy(
            items = current.items.map {
                if (it.url == url) {
                    it.copy(
                        storageUri = storageUri,
                        storageTag = tag,
                        missionKind = kind,
                        status = status
                    )
                } else {
                    it
                }
            }
        )
        persist()
    }

    private fun checkBatchDone() {
        val current = record ?: return
        if (isTerminal(current.status)) {
            return
        }
        val busy = current.items.any { isActiveStatus(it.status) || it.status == BatchItemStatus.PENDING }
        if (busy) {
            return
        }
        val askUrls = current.askLanguageUrls
        finishActiveBatch(current.copy(status = BatchStatus.COMPLETED))
        if (askUrls.isNotEmpty()) {
            _askLanguageEvent.value = BatchEvent(askUrls)
        }
    }

    /**
     * Marks the ACTIVE batch DONE and automatically starts the next QUEUED batch
     * (oldest first). Failed items never block the next batch: a batch is done once
     * none of its items is busy (completed/failed/skipped/cancelled all count as done).
     *
     * @param finished the ACTIVE batch in its final state
     */
    private fun finishActiveBatch(finished: BatchRecord) {
        val completed = finished.items.count { it.status == BatchItemStatus.COMPLETED }
        val failed = finished.items.count { it.status == BatchItemStatus.FAILED }
        Log.d(
            TAG,
            "batch done id=${finished.id} status=${finished.status} " +
                "completed=$completed failed=$failed total=${finished.items.size}"
        )
        otherBatches = otherBatches + finished.copy(queueState = BatchQueueState.DONE)
        record = null
        pendingFolderKey = null
        stopWifiCallback()
        persist()
        activateNextBatch()
        record?.let { Log.d(TAG, "next batch started id=${it.id} name=${it.playlistName}") }
    }

    /**
     * Starts the oldest QUEUED batch (FIFO). No-op when the queue is empty.
     * Only the ACTIVE batch runs items; queued batches wait untouched.
     */
    private fun activateNextBatch() {
        val index = otherBatches.indexOfFirst { it.queueState == BatchQueueState.QUEUED }
        if (index < 0) {
            record = null
            persist()
            return
        }
        val next = otherBatches[index]
        otherBatches = otherBatches.filterNot { it.id == next.id }
        if (isTerminal(next.status)) {
            Log.d(TAG, "activateNextBatch: queued batch ${next.id} is terminal, marking DONE")
            otherBatches = otherBatches + next.copy(queueState = BatchQueueState.DONE)
            persist()
            activateNextBatch()
            return
        }
        record = next.copy(queueState = BatchQueueState.ACTIVE)
        liveMissions.clear()
        speedSamples.clear()
        cooldownUntil = 0L
        Log.d(TAG, "batch activated id=${next.id} name=${next.playlistName}")
        persist()
        ensureBound { pump() }
        checkLowSpace()
    }

    /**
     * Static item list for a non-ACTIVE batch (queued or done), for the item screen.
     * Returns null for the ACTIVE batch (use the live [uiState]) and unknown ids.
     *
     * @param batchId the id of the batch to snapshot
     * @return the item screen state, or null
     */
    fun itemsSnapshot(batchId: String): BatchUiState? {
        if (record?.id == batchId) {
            return null
        }
        val batch = otherBatches.firstOrNull { it.id == batchId } ?: return null
        val items = batch.items.map { item ->
            BatchItemUiState(
                url = item.url,
                title = item.title,
                position = item.position,
                status = item.status,
                fraction = if (item.status == BatchItemStatus.COMPLETED) {
                    1.0
                } else {
                    item.lastProgress
                },
                speedBytesPerSec = 0L,
                etaSeconds = -1L,
                errorText = item.failureReason,
                retryCount = item.retryCount,
                languageNote = if (item.status == BatchItemStatus.SKIPPED) {
                    item.failureReason
                } else {
                    null
                }
            )
        }
        val completed = batch.items.count { it.status == BatchItemStatus.COMPLETED }
        val failed = batch.items.count { it.status == BatchItemStatus.FAILED }
        val overall = if (items.isEmpty()) {
            0.0
        } else {
            items.sumOf { it.fraction } / items.size
        }
        return BatchUiState(
            batchId = batch.id,
            playlistName = batch.playlistName,
            batchStatus = batch.status,
            waitingWifi = false,
            needsFolder = false,
            items = items,
            completedCount = completed,
            failedCount = failed,
            totalCount = items.size,
            overallFraction = overall
        )
    }

    private fun checkLowSpace() {
        try {
            val prefs = PreferenceManager.getDefaultSharedPreferences(appContext)
            val path = prefs.getString(appContext.getString(R.string.download_path_video_key), null)
            if (path.isNullOrBlank()) {
                return
            }
            val dir = StoredDirectoryHelper(
                appContext,
                android.net.Uri.parse(path),
                DownloadManager.TAG_VIDEO
            )
            if (dir.freeStorageSpace < LOW_SPACE_WARN_BYTES) {
                postMessage(appContext.getString(R.string.playlist_batch_low_space))
            }
        } catch (e: Exception) {
            // best-effort only
        }
    }

    private fun pauseActiveMissions() {
        val manager = manager() ?: return
        record?.items?.forEach { item ->
            findLiveMission(item.url)?.let { mission ->
                try {
                    manager.pauseMission(mission)
                } catch (e: Exception) {
                    // ignore
                }
            }
        }
    }

    private fun resumeActiveMissions() {
        val manager = manager() ?: return
        record?.items?.forEach { item ->
            if (isActiveStatus(item.status)) {
                findLiveMission(item.url)?.let { mission ->
                    try {
                        manager.resumeMission(mission)
                    } catch (e: Exception) {
                        // ignore
                    }
                }
            }
        }
    }

    private fun deleteActiveMissions(alsoDeleteFile: Boolean = true) {
        val manager = manager() ?: return
        record?.items?.forEach { item ->
            findLiveMission(item.url)?.let { mission ->
                try {
                    if (!alsoDeleteFile) {
                        manager.pauseMission(mission)
                    }
                    manager.deleteMission(mission, alsoDeleteFile)
                } catch (e: Exception) {
                    // ignore
                }
            }
        }
        liveMissions.clear()
    }

    private fun startWifiCallback() {
        if (wifiCallbackRegistered) {
            return
        }
        try {
            val connectivity = appContext.getSystemService(ConnectivityManager::class.java)
                ?: return
            connectivity.registerNetworkCallback(NetworkRequest.Builder().build(), wifiCallback)
            wifiCallbackRegistered = true
        } catch (e: Exception) {
            // best-effort only
        }
    }

    private fun stopWifiCallback() {
        if (!wifiCallbackRegistered) {
            return
        }
        try {
            appContext.getSystemService(ConnectivityManager::class.java)
                ?.unregisterNetworkCallback(wifiCallback)
        } catch (e: Exception) {
            // ignore
        }
        wifiCallbackRegistered = false
    }

    /** One summary per batch (ACTIVE first, then QUEUED, then DONE) for the batch list. */
    private fun batchSummaries(): List<BatchSummary> {
        return (listOfNotNull(record) + otherBatches).map { batch ->
            val completed = batch.items.count { it.status == BatchItemStatus.COMPLETED }
            val failed = batch.items.count { it.status == BatchItemStatus.FAILED }
            val total = batch.items.size
            val overall = if (total == 0) {
                0.0
            } else {
                batch.items.sumOf { item ->
                    if (item.status == BatchItemStatus.COMPLETED) 1.0 else item.lastProgress
                } / total
            }
            BatchSummary(
                batchId = batch.id,
                playlistName = batch.playlistName,
                queueState = batch.queueState,
                batchStatus = batch.status,
                totalCount = total,
                completedCount = completed,
                failedCount = failed,
                overallFraction = overall,
                createdAt = batch.createdAt
            )
        }
    }

    private fun postMessage(text: String) {
        _messageEvent.value = BatchEvent(text)
    }

    private fun toBatchItem(item: BatchItemRecord): PlaylistBatchItem {
        return PlaylistBatchItem(
            serviceId = item.serviceId,
            url = item.url,
            title = item.title,
            durationSecs = 0L,
            uploaderName = item.uploaderName,
            thumbnails = emptyList(),
            streamType = null,
            position = item.position,
            selected = true,
            uploadDateText = item.uploadDateText
        )
    }

    private fun publish() {
        _batchList.value = batchSummaries()
        val current = record
        if (current == null) {
            _uiState.value = BatchUiState(
                null, "", null, false, false,
                emptyList(), 0, 0, 0, 0.0
            )
            return
        }
        val items = current.items.map { item ->
            val mission = findLiveMission(item.url)
            val done = mission?.done?.coerceAtLeast(0L) ?: 0L
            val (speed, eta) = if (isActiveStatus(item.status) && mission != null) {
                speedFor(item.url, done)
            } else {
                Pair(0L, -1L)
            }
            val fraction = when (item.status) {
                BatchItemStatus.COMPLETED -> 1.0
                BatchItemStatus.DOWNLOADING, BatchItemStatus.MERGING -> item.lastProgress
                else -> 0.0
            }
            BatchItemUiState(
                url = item.url,
                title = item.title,
                position = item.position,
                status = item.status,
                fraction = fraction,
                speedBytesPerSec = speed,
                etaSeconds = eta,
                errorText = item.failureReason,
                retryCount = item.retryCount,
                languageNote = if (item.status == BatchItemStatus.SKIPPED) {
                    item.failureReason
                } else {
                    null
                }
            )
        }
        val completed = current.items.count { it.status == BatchItemStatus.COMPLETED }
        val failed = current.items.count { it.status == BatchItemStatus.FAILED }
        val overall = if (items.isEmpty()) {
            0.0
        } else {
            items.sumOf { it.fraction } / items.size
        }
        _uiState.value = BatchUiState(
            batchId = current.id,
            playlistName = current.playlistName,
            batchStatus = current.status,
            waitingWifi = current.status == BatchStatus.WAITING_WIFI,
            needsFolder = pendingFolderKey != null,
            items = items,
            completedCount = completed,
            failedCount = failed,
            totalCount = items.size,
            overallFraction = overall
        )
    }
}
