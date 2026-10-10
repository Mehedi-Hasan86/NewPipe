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

/** One pending language decision: which batch asked, and which item urls. */
data class AskLanguageRequest(
    val batchId: String,
    val urls: List<String>
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
    private val _askLanguageEvent = MutableLiveData<BatchEvent<AskLanguageRequest>>()
    val askLanguageEvent: LiveData<BatchEvent<AskLanguageRequest>> = _askLanguageEvent

    private var record: BatchRecord? = null

    /**
     * Every non-ACTIVE batch in queue order: QUEUED batches (FIFO) first, then DONE
     * batches. Together with [record] this is the whole persisted queue.
     */
    private var otherBatches: List<BatchRecord> = emptyList()

    /** Stable subfolder name per playlist key (persisted with the queue). */
    private var folderNames: MutableMap<String, String> = mutableMapOf()

    /**
     * True once the persisted queue has been read into memory. Every mutating entry
     * point calls [ensureLoaded] first, so memory can never overwrite the store
     * before it has been read (which silently dropped batches).
     */
    private var storeLoaded = false

    /** Transient queue-screen selection requested from elsewhere (not persisted). */
    var pendingSelectBatchId: String? = null
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
     * Outcome of [startBatch]: a new batch, or a duplicate of an ACTIVE/QUEUED one.
     *
     * @param batchId the new or existing batch id
     * @param position 0 when running now, otherwise the 1-based position among QUEUED
     * @param isNew true when a batch was created, false for duplicates
     */
    data class StartBatchResult(
        val batchId: String,
        val position: Int,
        val isNew: Boolean
    )

    /**
     * Enqueues a new batch. Each batch keeps the options captured here at creation time
     * (folder, subfolder, quality, audio language, naming, duplicate policy).
     * When no batch is ACTIVE the new batch starts immediately, otherwise it is QUEUED
     * behind the existing batches and runs automatically when its turn comes
     * (sequential FIFO, oldest first).
     * A playlist that is already ACTIVE or QUEUED is not added twice: the existing
     * batch is returned so the UI can offer Open / Add-missing-only instead.
     *
     * @return what happened (new ACTIVE/QUEUED batch, or the duplicate)
     */
    fun startBatch(
        playlistName: String,
        playlistUrl: String,
        serviceId: Int,
        items: List<PlaylistBatchItem>,
        options: PlaylistDownloadOptions,
        overrides: Map<String, PerItemOverride>
    ): StartBatchResult {
        ensureLoaded()
        Log.d(TAG, "startBatch: playlistName=$playlistName, items=${items.size}")
        val key = PlaylistBatchUrlHelper.playlistKey(serviceId, playlistUrl)
        Log.d(TAG, "startBatch: playlistKey=$key")
        activeOrQueuedByKey(key)?.let { existing ->
            val position = if (existing.id == record?.id) {
                0
            } else {
                queuedPosition(existing.id)
            }
            Log.d(TAG, "startBatch: duplicate playlistKey=$key batch=${existing.id}")
            return StartBatchResult(existing.id, position, false)
        }
        var batchOptions = options
        if (key.isNotBlank() &&
            otherBatches.any { it.playlistKey == key && it.queueState == BatchQueueState.DONE }
        ) {
            // Re-downloading a finished playlist: reuse its folder (via the map) and
            // default to SKIP so existing files are kept instead of re-downloaded.
            batchOptions = batchOptions.copy(duplicateAction = DuplicateAction.SKIP)
            Log.d(TAG, "startBatch: DONE batch with playlistKey=$key exists, defaulting to SKIP")
        }
        batchOptions = captureRoots(batchOptions)
        val now = System.currentTimeMillis()
        val fresh = BatchRecord(
            id = UUID.randomUUID().toString(),
            playlistName = playlistName,
            playlistUrl = playlistUrl,
            serviceId = serviceId,
            createdAt = now,
            status = BatchStatus.RUNNING,
            queueState = BatchQueueState.QUEUED,
            playlistKey = key,
            options = batchOptions,
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
        val folderName = folderNameFor(fresh, fresh.items.firstOrNull()?.title.orEmpty())
        Log.d(TAG, "startBatch: playlistKey=$key folder=$folderName")
        if (record == null) {
            if (otherBatches.any { it.queueState == BatchQueueState.QUEUED }) {
                // Should not happen (finish/advance keeps the invariant), but a new
                // batch must never jump the line: enqueue it, then start the oldest.
                otherBatches = otherBatches + fresh
                Log.d(TAG, "batch added id=${fresh.id} state=QUEUED")
                persist()
                activateNextBatch()
                return StartBatchResult(fresh.id, queuedPosition(fresh.id), true)
            }
            record = fresh.copy(queueState = BatchQueueState.ACTIVE)
            liveMissions.clear()
            speedSamples.clear()
            cooldownUntil = 0L
            Log.d(TAG, "batch added id=${fresh.id} state=ACTIVE")
            persist()
            ensureBound { pump() }
            checkLowSpace()
            return StartBatchResult(fresh.id, 0, true)
        }
        otherBatches = otherBatches + fresh
        val position = queuedPosition(fresh.id)
        Log.d(TAG, "batch added id=${fresh.id} state=QUEUED position=$position")
        persist()
        return StartBatchResult(fresh.id, position, true)
    }

    /** The ACTIVE or QUEUED batch with [key] (blank keys never match), or null. */
    private fun activeOrQueuedByKey(key: String): BatchRecord? {
        if (key.isBlank()) {
            return null
        }
        record?.takeIf { it.playlistKey == key }?.let { return it }
        return otherBatches.firstOrNull {
            it.playlistKey == key && it.queueState == BatchQueueState.QUEUED
        }
    }

    /**
     * Captures the current download root folders into the options, so later
     * preference changes never move already-queued batches.
     *
     * @param options the options chosen in the intake screen
     * @return the options with roots filled in
     */
    private fun captureRoots(options: PlaylistDownloadOptions): PlaylistDownloadOptions {
        val prefs = PreferenceManager.getDefaultSharedPreferences(appContext)
        val videoKey = appContext.getString(R.string.download_path_video_key)
        val audioKey = appContext.getString(R.string.download_path_audio_key)
        return options.copy(
            videoRootUri = options.videoRootUri
                ?: prefs.getString(videoKey, null)?.takeIf { it.isNotBlank() },
            audioRootUri = options.audioRootUri
                ?: prefs.getString(audioKey, null)?.takeIf { it.isNotBlank() }
        )
    }

    /**
     * Stable subfolder name for [batch]: the mapped name for its playlist key, assigning
     * `base`, `base (2)`, ... on first use. Batches without a key get a unique folder.
     *
     * @param batch the batch needing a folder
     * @param fallbackTitle used when the batch has no playlist name
     * @return the subfolder name to use
     */
    private fun folderNameFor(batch: BatchRecord, fallbackTitle: String): String {
        val key = batch.playlistKey.ifBlank { "batch:${batch.id}" }
        folderNames[key]?.let { return it }
        val base = sanitizeSubfolderName(batch.playlistName.ifBlank { fallbackTitle })
        var name = base
        var suffix = 2
        val taken = folderNames.values.toSet()
        while (name in taken) {
            name = "$base ($suffix)"
            suffix++
        }
        folderNames[key] = name
        Log.d(TAG, "folderNameFor: playlistKey=$key folder=$name")
        return name
    }

    /**
     * Sanitizes a playlist title into a provider-safe folder name.
     *
     * @param title the raw playlist (or video) title
     * @return the sanitized name, or "playlist" when blank
     */
    private fun sanitizeSubfolderName(title: String): String {
        return FilenameUtils.createFilename(appContext, title)
            .replace("!", "").trim().trimEnd('.', ' ').ifBlank { "playlist" }
    }

    /** 1-based position of [batchId] among the QUEUED batches (0 when not queued). */
    private fun queuedPosition(batchId: String): Int {
        return otherBatches.filter { it.queueState == BatchQueueState.QUEUED }
            .indexOfFirst { it.id == batchId } + 1
    }

    /** Pauses the ACTIVE batch only; queued batches are untouched. */
    fun pauseBatch() {
        ensureLoaded()
        val current = record ?: return
        record = current.copy(status = BatchStatus.PAUSED)
        persist()
        pauseActiveMissions()
        publish()
    }

    /** Resumes the ACTIVE batch only; queued batches are untouched. */
    fun resumeBatch() {
        ensureLoaded()
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
        ensureLoaded()
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
        ensureLoaded()
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

    /**
     * Adds items missing from an ACTIVE or QUEUED batch: urls not present at all, plus
     * FAILED ones (reset to PENDING with cleared counters). Anything else is left alone.
     *
     * @param batchId the ACTIVE or QUEUED batch to merge into
     * @param items candidate items (only missing/failed urls are used)
     * @param overrides per-item overrides for the merged items
     * @return how many items were added or reset
     */
    fun mergeMissing(
        batchId: String,
        items: List<PlaylistBatchItem>,
        overrides: Map<String, PerItemOverride>
    ): Int {
        ensureLoaded()
        val active = record
        if (active != null && active.id == batchId) {
            val (merged, updated) = mergeInto(active, items, overrides)
            if (merged == 0) {
                return 0
            }
            Log.d(TAG, "mergeMissing: batch=$batchId added=$merged state=ACTIVE")
            record = updated
            persist()
            ensureBound { pump() }
            return merged
        }
        val target = otherBatches.firstOrNull {
            it.id == batchId && it.queueState == BatchQueueState.QUEUED
        } ?: return 0
        val (merged, updated) = mergeInto(target, items, overrides)
        if (merged == 0) {
            return 0
        }
        Log.d(TAG, "mergeMissing: batch=$batchId added=$merged state=QUEUED")
        otherBatches = otherBatches.map { if (it.id == batchId) updated else it }
        persist()
        return merged
    }

    private fun mergeInto(
        batch: BatchRecord,
        items: List<PlaylistBatchItem>,
        overrides: Map<String, PerItemOverride>
    ): Pair<Int, BatchRecord> {
        val urls = items.map { it.url }.toSet()
        val existing = batch.items.map { it.url }.toSet()
        var position = batch.items.maxOfOrNull { it.position } ?: 0
        var merged = 0
        val kept = batch.items.map { item ->
            if (item.status == BatchItemStatus.FAILED && item.url in urls) {
                merged++
                item.copy(
                    status = BatchItemStatus.PENDING,
                    retryCount = 0,
                    failureReason = null,
                    lastProgress = 0.0,
                    override = overrides[item.url] ?: item.override
                )
            } else {
                item
            }
        }
        val added = items.filter { it.url !in existing }.map { item ->
            merged++
            position++
            BatchItemRecord(
                url = item.url,
                title = item.title,
                position = position,
                serviceId = item.serviceId,
                uploaderName = item.uploaderName,
                uploadDateText = item.uploadDateText,
                override = overrides[item.url]
            )
        }
        return merged to batch.copy(items = kept + added)
    }

    /**
     * Retries one item of the ACTIVE batch only; requests for other batches are ignored.
     *
     * @param batchId the batch that must currently be ACTIVE
     * @param url the item url to re-queue
     */
    fun retryItem(batchId: String, url: String) {
        ensureLoaded()
        val current = record
        if (current == null || current.id != batchId) {
            Log.d(TAG, "retryItem: ignoring url=$url for non-active batch $batchId")
            return
        }
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

    /**
     * Cancels one item of the ACTIVE batch only; requests for other batches are ignored.
     *
     * @param batchId the batch that must currently be ACTIVE
     * @param url the item url to cancel
     */
    fun cancelItem(batchId: String, url: String) {
        ensureLoaded()
        val current = record
        if (current == null || current.id != batchId) {
            Log.d(TAG, "cancelItem: ignoring url=$url for non-active batch $batchId")
            return
        }
        findLiveMission(url, current.id)?.let { mission ->
            try {
                binder?.getDownloadManager()?.deleteMission(mission, true)
            } catch (e: Exception) {
                // already gone
            }
            liveMissions.remove(missionMapKey(current.id, missionKey(mission)))
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

    /**
     * Moves a PENDING item of the ACTIVE batch within the queue order.
     *
     * @param batchId the batch that must currently be ACTIVE
     * @param fromQueueIndex index among PENDING items to move from
     * @param toQueueIndex index among PENDING items to move to
     */
    fun movePending(batchId: String, fromQueueIndex: Int, toQueueIndex: Int) {
        ensureLoaded()
        val current = record
        if (current == null || current.id != batchId) {
            return
        }
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

    /**
     * Re-queues ASK_AT_END language-skipped items with the Original track.
     * Only applies when [batchId] is still the ACTIVE batch; late answers for
     * finished batches are ignored instead of leaking into the next batch.
     *
     * @param batchId the batch that asked (must currently be ACTIVE)
     * @param urls the skipped item urls to re-queue
     */
    fun retrySkippedWithOriginal(batchId: String, urls: List<String>) {
        ensureLoaded()
        val current = record
        if (current == null || current.id != batchId) {
            Log.d(TAG, "retrySkippedWithOriginal: ignoring non-active batch $batchId")
            return
        }
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
        ensureLoaded()
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
        ensureLoaded()
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
        ensureLoaded()
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
        ensureLoaded()
        val done = otherBatches.filter { it.queueState == BatchQueueState.DONE }
        if (done.isEmpty()) {
            Log.d(TAG, "clearFinished: no DONE batches to remove")
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
        ensureLoaded()
        if (record == null &&
            otherBatches.any { it.queueState == BatchQueueState.QUEUED }
        ) {
            // No ACTIVE batch (e.g. after a restart): start the oldest queued one.
            activateNextBatch()
        } else {
            publish()
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

    /**
     * Loads the persisted queue into memory exactly once (idempotent). Every mutating
     * entry point calls this first, so in-memory state can never overwrite the store
     * before it has been read. Loading alone starts nothing: no activation, no
     * reconcile, no pump, so a later [restoreIfNeeded] never duplicates side effects.
     */
    private fun ensureLoaded() {
        if (storeLoaded) {
            return
        }
        storeLoaded = true
        val stored = BatchStore.loadQueue(appContext)
        folderNames = stored.folderNames.toMutableMap()
        val loaded = stored.batches
        Log.d(TAG, "ensureLoaded: loaded ${loaded.size} batches from store")
        if (loaded.isEmpty()) {
            return
        }
        // Seed stable folder names for every known batch (migrated ones included).
        for (batch in loaded) {
            folderNameFor(batch, batch.items.firstOrNull()?.title.orEmpty())
        }
        adoptLoaded(loaded)
    }

    /**
     * Splits loaded batches into the ACTIVE record and the rest, demoting extra
     * ACTIVES to QUEUED and marking terminal ACTIVES as DONE.
     *
     * @param loaded the batches in stored order
     */
    private fun adoptLoaded(loaded: List<BatchRecord>) {
        val actives = loaded.filter { it.queueState == BatchQueueState.ACTIVE }
        val firstActive = actives.firstOrNull()
        val demoted = actives.drop(1).map { it.copy(queueState = BatchQueueState.QUEUED) }
        if (demoted.isNotEmpty()) {
            Log.d(TAG, "adoptLoaded: demoted ${demoted.size} extra ACTIVE batches to QUEUED")
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
                    "adoptLoaded: persisted ACTIVE batch ${firstActive.id} " +
                        "is terminal, marking DONE"
                )
                otherBatches = otherBatches + firstActive.copy(queueState = BatchQueueState.DONE)
            }
            record = null
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
            BatchStore.saveQueue(
                appContext,
                BatchQueueFile(
                    batches = listOfNotNull(record) + otherBatches,
                    folderNames = folderNames.toMap()
                )
            )
        } catch (e: Exception) {
            // store is best-effort; the batches continue in memory
            Log.e(TAG, "persist failed", e)
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
        val current = record
        val prefs = PreferenceManager.getDefaultSharedPreferences(appContext)
        val key = if (audioOnly) {
            appContext.getString(R.string.download_path_audio_key)
        } else {
            appContext.getString(R.string.download_path_video_key)
        }
        // Roots were captured into the batch options at creation time; preferences
        // are only a legacy fallback for batches created before that.
        val path = (if (audioOnly) current?.options?.audioRootUri else current?.options?.videoRootUri)
            ?: prefs.getString(key, null)
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
        if (current == null || !current.options.usePlaylistSubfolder) {
            Log.d(TAG, "targetDirectory: using root folder $path")
            return main
        }
        // Stable per-playlist folder from the persisted map: the same playlistKey
        // always reuses its folder, title collisions get "Name (2)", ... instead of
        // title-only naming that mixed different playlists together.
        val folderName = folderNameFor(current, item.title)
        Log.d(TAG, "targetDirectory: root=$path subfolder=$folderName key=${current.playlistKey}")
        val sub = main.getSubfolder(
            appContext,
            folderName,
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

    /**
     * Finds the batch and item owning [storageUri] (active batch first).
     *
     * @param storageUri the target file URI of a mission
     * @return the owning batch and item, or null when unknown
     */
    private fun findBatchItemByStorageUri(storageUri: String): Pair<BatchRecord, BatchItemRecord>? {
        val current = record
        if (current != null) {
            current.items.firstOrNull { it.storageUri == storageUri }?.let {
                return current to it
            }
        }
        for (batch in otherBatches) {
            batch.items.firstOrNull { it.storageUri == storageUri }?.let {
                return batch to it
            }
        }
        return null
    }

    private fun missionKey(mission: DownloadMission): String {
        return try {
            mission.storage.uri.toString()
        } catch (e: Exception) {
            ""
        }
    }

    /** Map key scoping a mission to its batch: batch id + storage URI. */
    private fun missionMapKey(batchId: String, storageUri: String): String {
        return "$batchId\n$storageUri"
    }

    private fun batchById(batchId: String): BatchRecord? {
        record?.takeIf { it.id == batchId }?.let { return it }
        return otherBatches.firstOrNull { it.id == batchId }
    }

    /**
     * Live mission for [url] inside [batchId] only. The url/`source` fallback never
     * leaves the batch, so another batch's mission is never returned.
     *
     * @param url the item url to look up
     * @param batchId the owning batch, or null to look up nothing
     * @return the live mission, or null
     */
    private fun findLiveMission(url: String, batchId: String?): DownloadMission? {
        if (batchId == null) {
            return null
        }
        val batch = batchById(batchId) ?: return null
        val storageUri = batch.items.firstOrNull { it.url == url }?.storageUri
            ?: return liveMissions.entries
                .firstOrNull { (key, mission) ->
                    key.startsWith("$batchId\n") && mission.source == url
                }?.value
        return liveMissions[missionMapKey(batchId, storageUri)]
    }

    private fun onMissionMessage(what: Int, mission: DownloadMission?): Boolean {
        if (mission == null) {
            return false
        }
        val storageUri = missionKey(mission)
        if (what == DownloadManagerService.MESSAGE_DELETED) {
            if (storageUri.isNotBlank()) {
                liveMissions.keys
                    .filter { it == storageUri || it.endsWith("\n$storageUri") }
                    .forEach { liveMissions.remove(it) }
            }
            return true
        }
        if (storageUri.isBlank()) {
            return false
        }
        // Attribute the event to the owning batch; events for missions of
        // non-ACTIVE batches are ignored instead of leaking into the active one.
        val owner = findBatchItemByStorageUri(storageUri) ?: return false
        if (owner.first.id != record?.id) {
            Log.d(TAG, "onMissionMessage: ignoring event for non-active batch ${owner.first.id}")
            return false
        }
        liveMissions[missionMapKey(owner.first.id, storageUri)] = mission
        val current = record ?: return false
        val item = owner.second
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
                    Log.d(TAG, "mission finished: batch=${current.id} url=${item.url} file=$storageUri")
                    setItemStatus(item.url, BatchItemStatus.COMPLETED, null)
                    liveMissions.remove(missionMapKey(current.id, storageUri))
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
            val mission = findLiveMission(item.url, current.id) ?: return@map item
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

    private fun speedFor(batchId: String, url: String, done: Long): Pair<Long, Long> {
        val now = System.currentTimeMillis()
        val sampleKey = "$batchId\n$url"
        val last = speedSamples[sampleKey]
        speedSamples[sampleKey] = Pair(now, done)
        if (last == null) {
            return Pair(0L, -1L)
        }
        val dt = (now - last.first).coerceAtLeast(1L)
        val speed = (done - last.second).coerceAtLeast(0L) * 1000L / dt
        val mission = findLiveMission(url, batchId)
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
                liveMissions[missionMapKey(current.id, requireNotNull(storageUri))] = mission
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
        val finishedId = current.id
        finishActiveBatch(current.copy(status = BatchStatus.COMPLETED))
        if (askUrls.isNotEmpty()) {
            _askLanguageEvent.value = BatchEvent(AskLanguageRequest(finishedId, askUrls))
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
        val current = record ?: return
        current.items.forEach { item ->
            findLiveMission(item.url, current.id)?.let { mission ->
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
        val current = record ?: return
        current.items.forEach { item ->
            if (isActiveStatus(item.status)) {
                findLiveMission(item.url, current.id)?.let { mission ->
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
        val current = record ?: return
        current.items.forEach { item ->
            findLiveMission(item.url, current.id)?.let { mission ->
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
            val mission = findLiveMission(item.url, current.id)
            val done = mission?.done?.coerceAtLeast(0L) ?: 0L
            val (speed, eta) = if (isActiveStatus(item.status) && mission != null) {
                speedFor(current.id, item.url, done)
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
