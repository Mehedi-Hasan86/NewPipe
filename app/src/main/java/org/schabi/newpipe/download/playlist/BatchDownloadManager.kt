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
    }

    private val mainHandler = Handler(Looper.getMainLooper())
    private val disposables = CompositeDisposable()

    private val _uiState = MutableLiveData(
        BatchUiState(null, "", null, false, false, emptyList(), 0, 0, 0, 0.0)
    )
    val uiState: LiveData<BatchUiState> = _uiState

    private val _messageEvent = MutableLiveData<BatchEvent<String>>()
    val messageEvent: LiveData<BatchEvent<String>> = _messageEvent

    /** Fired once when a download folder must be picked (pref key as content). */
    private val _folderEvent = MutableLiveData<BatchEvent<String>>()
    val folderEvent: LiveData<BatchEvent<String>> = _folderEvent

    /** Fired once when ASK_AT_END items await a decision. */
    private val _askLanguageEvent = MutableLiveData<BatchEvent<List<String>>>()
    val askLanguageEvent: LiveData<BatchEvent<List<String>>> = _askLanguageEvent

    private var record: BatchRecord? = null
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

    /** True when a non-terminal batch is persisted (caller asks to replace it). */
    private companion object {
        private const val TAG = "PlaylistBatch"
    }

    fun hasUnfinishedBatch(): Boolean {
        val current = record ?: BatchStore.load(appContext)
        if (current == null || isTerminal(current.status)) {
            return false
        }
        // Only consider the batch "unfinished" if there's at least one
        // non-terminal item (PENDING/RESOLVING/DOWNLOADING/MERGING/PAUSED).
        return current.items.any { !isTerminalItem(it.status) }
    }

    /**
     * Starts a new batch, replacing any previous one when [replace] is true.
     * Returns false when another batch is active and [replace] is false.
     */
    fun startBatch(
        playlistName: String,
        playlistUrl: String,
        serviceId: Int,
        items: List<PlaylistBatchItem>,
        options: PlaylistDownloadOptions,
        overrides: Map<String, PerItemOverride>,
        replace: Boolean = false
    ): Boolean {
        if (hasUnfinishedBatch() && !replace) {
            Log.d(TAG, "startBatch: batch already active, replace=$replace, returning false")
            return false
        }
        Log.d(TAG, "startBatch: playlistName=$playlistName, items=${items.size}, replace=$replace")
        val now = System.currentTimeMillis()
        record = BatchRecord(
            id = UUID.randomUUID().toString(),
            playlistName = playlistName,
            playlistUrl = playlistUrl,
            serviceId = serviceId,
            createdAt = now,
            status = BatchStatus.RUNNING,
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
        Log.d(TAG, "startBatch: created BatchRecord id=${record.id} with ${record.items.size} items")
        liveMissions.clear()
        speedSamples.clear()
        cooldownUntil = 0L
        persist()
        ensureBound { pump() }
        checkLowSpace()
        return true
    }

    fun pauseBatch() {
        val current = record ?: return
        record = current.copy(status = BatchStatus.PAUSED)
        persist()
        pauseActiveMissions()
        publish()
    }

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
        record = current.copy(status = BatchStatus.CANCELLED, items = items)
        persist()
        stopWifiCallback()
        publish()
    }

    /**
     * Clears a stuck batch: deletes all active missions, marks all non-completed
     * items as FAILED (so user can retry), and clears the batch record.
     * Use this when the batch is permanently stuck.
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
        record = current.copy(status = BatchStatus.CANCELLED, items = items)
        persist()
        stopWifiCallback()
        publish()
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

    /** Loads persisted state for the queue screen (no-op when already loaded). */
    fun restoreIfNeeded() {
        if (record != null) {
            Log.d(TAG, "restoreIfNeeded: record already in memory, publishing")
            publish()
            return
        }
        record = BatchStore.load(appContext)
        Log.d(TAG, "restoreIfNeeded: loaded from store: ${if (record != null) "id=${record.id}, items=${record.items.size}, status=${record.status}" else "null"}")
        publish()
        if (record != null && !isTerminal(record!!.status)) {
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
        val current = record ?: return
        try {
            BatchStore.save(appContext, current)
        } catch (e: Exception) {
            // store is best-effort; the batch continues in memory
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
            failItem(item, e.message, canRetry = true)
        } catch (e: Exception) {
            failItem(item, e.message, canRetry = true)
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
        launchMission(item, info, urls, 'v', psName, null, recovery, filename, "video/*")
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
        val existingUri = dir.findFile(filename)

        val storage: StoredFileHelper
        if (existingUri == null) {
            storage = dir.createFile(filename, mime)
                ?: throw IOException(appContext.getString(R.string.playlist_batch_fail_storage))
        } else {
            when (record?.options?.duplicateAction ?: DuplicateAction.OVERWRITE) {
                DuplicateAction.SKIP -> {
                    if (!isSubtitle) {
                        skipItem(item, appContext.getString(R.string.playlist_batch_skipped_exists))
                    }
                    return
                }

                DuplicateAction.RENAME -> {
                    storage = dir.createUniqueFile(filename, mime)
                        ?: throw IOException(
                            appContext.getString(R.string.playlist_batch_fail_storage)
                        )
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
            return main
        }
        val subName = FilenameUtils.createFilename(
            appContext,
            record?.playlistName.orEmpty().ifBlank { item.title }
        ).ifBlank { "playlist" }
        return main.getSubfolder(
            appContext,
            subName,
            if (audioOnly) DownloadManager.TAG_AUDIO else DownloadManager.TAG_VIDEO
        )
    }

    class FolderMissingException(val prefKey: String) : IOException()

    // ---------------- failures / retry ----------------

    private fun onResolveError(url: String, error: Throwable) {
        if (isRateLimited(error)) {
            startCooldown()
            setItemStatus(url, BatchItemStatus.PENDING, null)
            return
        }
        failItem(url, error.message, canRetry = true)
    }

    private fun failItem(item: BatchItemRecord, reason: String?, canRetry: Boolean) {
        failItem(item.url, reason, canRetry)
    }

    private fun failItem(url: String, reason: String?, canRetry: Boolean) {
        val current = record ?: return
        val item = current.items.firstOrNull { it.url == url } ?: return
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
            val mission = item.storageUri?.let { liveByStorage[it] }
            if (mission == null) {
                Log.d(TAG, "reconcileAfterRestart: item ${item.url} has no live mission, marking PENDING for retry")
                item.copy(status = BatchItemStatus.PENDING, failureReason = null)
            } else {
                liveMissions[item.storageUri!!] = mission
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
        record = current.copy(status = BatchStatus.COMPLETED)
        persist()
        stopWifiCallback()
        if (current.askLanguageUrls.isNotEmpty()) {
            _askLanguageEvent.value = BatchEvent(current.askLanguageUrls)
        }
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

    private fun deleteActiveMissions() {
        val manager = manager() ?: return
        record?.items?.forEach { item ->
            findLiveMission(item.url)?.let { mission ->
                try {
                    manager.deleteMission(mission, true)
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
