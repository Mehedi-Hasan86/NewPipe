/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.schedulers.Schedulers
import java.io.IOException
import org.schabi.newpipe.extractor.ListExtractor
import org.schabi.newpipe.extractor.Page
import org.schabi.newpipe.extractor.exceptions.ReCaptchaException
import org.schabi.newpipe.extractor.playlist.PlaylistInfo
import org.schabi.newpipe.extractor.stream.StreamInfo
import org.schabi.newpipe.extractor.stream.StreamInfoItem
import org.schabi.newpipe.util.ExtractorHelper
import org.schabi.newpipe.util.NO_SERVICE_ID

/**
 * Playlist batch intake + selection (STEP 1) with lazy per-item stream info
 * and download options (STEP 2).
 *
 * Survives configuration changes via ViewModel. No downloading here.
 */
class PlaylistBatchViewModel(application: Application) : AndroidViewModel(application) {

    /** Lazy per-item [StreamInfo] state, keyed by stream URL. */
    sealed class StreamInfoState {
        data object Loading : StreamInfoState()
        data class Loaded(val info: StreamInfo) : StreamInfoState()
        data class Error(val kind: ErrorKind) : StreamInfoState()
    }

    companion object {
        /** Max cached [StreamInfo] results (small limit, evicted LRU). */
        private const val MAX_STREAM_CACHE_SIZE = 12
    }

    enum class ErrorKind {
        NONE,
        INVALID_URL,
        NO_INTERNET,
        RATE_LIMITED,
        NOT_AVAILABLE,
        UNKNOWN
    }

    data class HeaderState(
        val name: String = "",
        val uploaderName: String = "",
        val totalCount: Long = ListExtractor.ITEM_COUNT_UNKNOWN,
        val loadedCount: Int = 0,
        val totalDurationSecs: Long = 0L,
        val durationComplete: Boolean = false,
        val skippedCount: Int = 0,
        val hasMore: Boolean = false
    )

    private val disposables = CompositeDisposable()

    private val _header = MutableLiveData(HeaderState())
    val header: LiveData<HeaderState> = _header

    private val _items = MutableLiveData<List<PlaylistBatchItem>>(emptyList())
    val items: LiveData<List<PlaylistBatchItem>> = _items

    private val _selectedCount = MutableLiveData(0)
    val selectedCount: LiveData<Int> = _selectedCount

    private val _isLoading = MutableLiveData(false)
    val isLoading: LiveData<Boolean> = _isLoading

    private val _isLoadingMore = MutableLiveData(false)
    val isLoadingMore: LiveData<Boolean> = _isLoadingMore

    private val _errorKind = MutableLiveData(ErrorKind.NONE)
    val errorKind: LiveData<ErrorKind> = _errorKind

    private val _errorMessage = MutableLiveData<String?>(null)
    val errorMessage: LiveData<String?> = _errorMessage

    private val _options = MutableLiveData<PlaylistDownloadOptions>()
    val options: LiveData<PlaylistDownloadOptions> = _options

    private val _streamStates =
        MutableLiveData<Map<String, StreamInfoState>>(emptyMap())
    val streamStates: LiveData<Map<String, StreamInfoState>> = _streamStates

    private val _audioGroupCounts = MutableLiveData<Map<String, Int>>(emptyMap())
    val audioGroupCounts: LiveData<Map<String, Int>> = _audioGroupCounts

    private val _overridesVersion = MutableLiveData(0)
    val overridesVersion: LiveData<Int> = _overridesVersion

    private var serviceId: Int = NO_SERVICE_ID
    private var playlistUrl: String = ""
    private var nextPage: Page? = null
    private var hasMore: Boolean = false
    private var totalCount: Long = ListExtractor.ITEM_COUNT_UNKNOWN
    private var skippedCount: Int = 0
    private var totalDurationSecs: Long = 0L
    private var loadedOnce: Boolean = false

    private val internalItems = mutableListOf<PlaylistBatchItem>()

    /** Per-item overrides keyed by stream URL (in memory for STEP 2). */
    private val overrides = mutableMapOf<String, PerItemOverride>()

    private val streamCache = object : LinkedHashMap<String, StreamInfo>(
        MAX_STREAM_CACHE_SIZE,
        0.75f,
        true
    ) {
        override fun removeEldestEntry(eldest: Map.Entry<String, StreamInfo>): Boolean {
            return size > MAX_STREAM_CACHE_SIZE
        }
    }
    private val streamFetchInFlight = mutableSetOf<String>()

    fun currentServiceId(): Int = serviceId
    fun currentUrl(): String = playlistUrl
    fun hasMorePages(): Boolean = hasMore

    /** First selected item, used for the filename preview. Null when nothing selected. */
    fun firstSelectedItem(): PlaylistBatchItem? = internalItems.firstOrNull { it.selected }

    /** Total selected count, used for the filename padding width. */
    fun selectedItems(): List<PlaylistBatchItem> = internalItems.filter { it.selected }

    fun getOverride(url: String): PerItemOverride? = overrides[url]

    fun setOverride(url: String, override: PerItemOverride?) {
        if (override == null || override.isEmpty()) {
            overrides.remove(url)
        } else {
            overrides[url] = override
        }
        _overridesVersion.value = (_overridesVersion.value ?: 0) + 1
    }

    /** Loads last-used globals once (no-op afterwards unless [force]). */
    fun loadOptions(force: Boolean = false) {
        if (!force && _options.value != null) {
            return
        }
        _options.value = PlaylistBatchPrefs.load(getApplication())
    }

    /** Updates globals in memory and persists them as last-used. */
    fun updateOptions(newOptions: PlaylistDownloadOptions) {
        _options.value = newOptions
        PlaylistBatchPrefs.save(getApplication(), newOptions)
    }

    /**
     * Returns cached [StreamInfo] for [url] or fetches it lazily via
     * [ExtractorHelper.getStreamInfo]. Results are published on [streamStates];
     * [audioGroupCounts] gets the audio-track group count for the row chip.
     */
    fun fetchStreamInfo(itemServiceId: Int, url: String) {
        streamCache[url]?.let { cached ->
            publishStreamState(url, StreamInfoState.Loaded(cached))
            return
        }
        if (!streamFetchInFlight.add(url)) {
            return
        }
        publishStreamState(url, StreamInfoState.Loading)
        val d = ExtractorHelper.getStreamInfo(itemServiceId, url, false)
            .subscribeOn(Schedulers.io())
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe({ info ->
                streamFetchInFlight.remove(url)
                streamCache[url] = info
                publishStreamState(url, StreamInfoState.Loaded(info))
                val groups = PlaylistBatchResolver.audioTrackChoices(getApplication(), info)
                val counts = _audioGroupCounts.value.orEmpty().toMutableMap()
                counts[url] = groups.size
                _audioGroupCounts.value = counts
            }, { error ->
                streamFetchInFlight.remove(url)
                publishStreamState(url, StreamInfoState.Error(mapError(error)))
            })
        disposables.add(d)
    }

    private fun publishStreamState(url: String, state: StreamInfoState) {
        val states = _streamStates.value.orEmpty().toMutableMap()
        states[url] = state
        _streamStates.value = states
    }

    /**
     * Start loading (or no-op when already loaded for the same URL).
     */
    fun load(serviceId: Int, url: String, force: Boolean = false) {
        val resolved = try {
            PlaylistBatchUrlHelper.resolve(serviceId, url)
        } catch (e: Exception) {
            showError(ErrorKind.INVALID_URL, e.message)
            return
        }

        if (!force && loadedOnce && this.serviceId == resolved.serviceId &&
            playlistUrl == resolved.playlistUrl && internalItems.isNotEmpty()
        ) {
            return
        }
        if (!force && _isLoading.value == true) {
            return
        }

        this.serviceId = resolved.serviceId
        this.playlistUrl = resolved.playlistUrl

        if (resolved.linkType != org.schabi.newpipe.extractor.StreamingService.LinkType.PLAYLIST) {
            showError(ErrorKind.INVALID_URL, resolved.playlistUrl)
            return
        }

        internalItems.clear()
        overrides.clear()
        streamCache.clear()
        streamFetchInFlight.clear()
        _streamStates.value = emptyMap()
        _audioGroupCounts.value = emptyMap()
        nextPage = null
        hasMore = false
        totalCount = ListExtractor.ITEM_COUNT_UNKNOWN
        skippedCount = 0
        totalDurationSecs = 0L
        loadedOnce = false
        publishItems()
        publishHeader()
        _errorKind.value = ErrorKind.NONE
        _errorMessage.value = null
        _isLoading.value = true

        val d = ExtractorHelper.getPlaylistInfo(this.serviceId, playlistUrl, force)
            .subscribeOn(Schedulers.io())
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe({ info -> onInitialInfo(info) }, { e -> onLoadError(e) })
        disposables.add(d)
    }

    fun retry() {
        if (playlistUrl.isBlank()) {
            return
        }
        load(serviceId, playlistUrl, true)
    }

    fun loadMore() {
        if (_isLoading.value == true || _isLoadingMore.value == true || !hasMore) {
            return
        }
        val page = nextPage
        if (!Page.isValid(page)) {
            hasMore = false
            publishHeader()
            return
        }
        _isLoadingMore.value = true

        val d = ExtractorHelper.getMorePlaylistItems(serviceId, playlistUrl, page)
            .subscribeOn(Schedulers.io())
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe({ result -> onMoreItems(result) }, { e ->
                _isLoadingMore.value = false
                // Keep already loaded items; surface pagination errors via skipped report.
                skippedCount += 1
                publishHeader()
                if (internalItems.isEmpty()) {
                    onLoadError(e)
                }
            })
        disposables.add(d)
    }

    // ------------------------------------------------------------------
    // Selection (kept in memory; survives rotation via ViewModel)
    // ------------------------------------------------------------------

    fun toggleSelection(position: Int) {
        val item = internalItems.find { it.position == position } ?: return
        item.selected = !item.selected
        publishItems()
    }

    fun selectAll() {
        if (internalItems.isEmpty()) {
            return
        }
        internalItems.forEach { it.selected = true }
        publishItems()
    }

    fun deselectAll() {
        if (internalItems.isEmpty()) {
            return
        }
        internalItems.forEach { it.selected = false }
        publishItems()
    }

    /**
     * Select inclusive 1-based serial range, e.g. from=5 to=20.
     * Out-of-bounds values are clamped; reversed inputs are swapped.
     */
    fun selectRange(fromSerial: Int, toSerial: Int) {
        if (internalItems.isEmpty()) {
            return
        }
        var from = fromSerial
        var to = toSerial
        if (from > to) {
            val tmp = from
            from = to
            to = tmp
        }
        val max = internalItems.size
        from = from.coerceIn(1, max)
        to = to.coerceIn(1, max)
        for (item in internalItems) {
            if (item.position in from..to) {
                item.selected = true
            }
        }
        publishItems()
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    private fun onInitialInfo(info: PlaylistInfo) {
        _isLoading.value = false
        loadedOnce = true
        totalCount = info.streamCount
        nextPage = info.nextPage
        hasMore = Page.isValid(nextPage)

        val skippedBefore = countSkipped(info.errors.size, 0)
        appendStreamItems(info.relatedItems)
        skippedCount = skippedBefore + countSkipped(0, 0)

        _header.value = HeaderState(
            name = info.name.orEmpty(),
            uploaderName = info.uploaderName.orEmpty(),
            totalCount = totalCount,
            loadedCount = internalItems.size,
            totalDurationSecs = totalDurationSecs,
            durationComplete = !hasMore,
            skippedCount = skippedCount,
            hasMore = hasMore
        )
        publishItems()
        publishHeader()
    }

    private fun onMoreItems(result: ListExtractor.InfoItemsPage<StreamInfoItem>) {
        _isLoadingMore.value = false
        nextPage = result.nextPage
        hasMore = Page.isValid(nextPage) && result.hasNextPage()
        skippedCount += result.errors.size
        appendStreamItems(result.items)
        publishItems()
        publishHeader()
    }

    private fun appendStreamItems(raw: List<StreamInfoItem>?) {
        if (raw.isNullOrEmpty()) {
            return
        }
        for (entry in raw) {
            if (!isAvailable(entry)) {
                skippedCount += 1
                continue
            }
            val position = internalItems.size + 1
            totalDurationSecs += maxOf(entry.duration, 0L)
            internalItems.add(
                PlaylistBatchItem(
                    serviceId = entry.serviceId,
                    url = entry.url,
                    title = entry.name.orEmpty(),
                    durationSecs = maxOf(entry.duration, 0L),
                    uploaderName = entry.uploaderName,
                    thumbnails = entry.thumbnails,
                    streamType = entry.streamType,
                    position = position,
                    selected = true,
                    uploadDateText = entry.textualUploadDate
                )
            )
        }
    }

    private fun isAvailable(entry: StreamInfoItem?): Boolean {
        if (entry == null) {
            return false
        }
        // Private / deleted / unavailable entries arrive with empty url or name.
        if (entry.url.isNullOrBlank() || entry.name.isNullOrBlank()) {
            return false
        }
        return true
    }

    private fun countSkipped(errorCount: Int, unused: Int): Int {
        return errorCount + unused
    }

    private fun publishItems() {
        _items.value = internalItems.toList()
        _selectedCount.value = internalItems.count { it.selected }
    }

    private fun publishHeader() {
        val current = _header.value ?: HeaderState()
        _header.value = current.copy(
            loadedCount = internalItems.size,
            totalCount = totalCount,
            totalDurationSecs = totalDurationSecs,
            durationComplete = !hasMore,
            skippedCount = skippedCount,
            hasMore = hasMore
        )
    }

    private fun onLoadError(e: Throwable) {
        _isLoading.value = false
        _isLoadingMore.value = false
        showError(mapError(e), e.message)
    }

    private fun showError(kind: ErrorKind, message: String?) {
        _errorKind.value = kind
        _errorMessage.value = message
        if (kind != ErrorKind.NONE && internalItems.isEmpty()) {
            publishHeader()
        }
    }

    private fun mapError(e: Throwable): ErrorKind {
        var cause: Throwable? = e
        while (cause != null) {
            if (cause is ReCaptchaException) {
                return ErrorKind.RATE_LIMITED
            }
            if (cause is IOException || cause is java.net.UnknownHostException ||
                cause is java.net.SocketTimeoutException
            ) {
                return ErrorKind.NO_INTERNET
            }
            val name = cause.javaClass.simpleName
            if (name.contains("NotFound") || name.contains("NotAvailable") ||
                name.contains("ContentNotAvailable") || name.contains("PlaylistNotFound")
            ) {
                return ErrorKind.NOT_AVAILABLE
            }
            if (cause is IllegalArgumentException) {
                return ErrorKind.INVALID_URL
            }
            cause = cause.cause
        }
        val msg = (e.message ?: "").lowercase()
        if (msg.contains("captcha")) {
            return ErrorKind.RATE_LIMITED
        }
        if (msg.contains("unable to resolve host") || msg.contains("network") ||
            msg.contains("timeout") || msg.contains("no internet")
        ) {
            return ErrorKind.NO_INTERNET
        }
        if (msg.contains("404") || msg.contains("not found") || msg.contains("not available") ||
            msg.contains("private") || msg.contains("deleted")
        ) {
            return ErrorKind.NOT_AVAILABLE
        }
        return ErrorKind.UNKNOWN
    }

    override fun onCleared() {
        disposables.clear()
        super.onCleared()
    }
}
