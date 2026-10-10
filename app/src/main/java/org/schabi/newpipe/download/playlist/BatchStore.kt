/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.content.Context
import java.io.File
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * Per-item state inside a batch. Persisted as JSON (separate from newpipe.db,
 * so no Room schema change or migration is needed).
 */
@Serializable
enum class BatchItemStatus {
    PENDING,
    RESOLVING,
    DOWNLOADING,
    MERGING,
    COMPLETED,
    FAILED,
    SKIPPED,
    CANCELLED
}

/** Batch-level state. */
@Serializable
enum class BatchStatus {
    RUNNING,
    PAUSED,
    WAITING_WIFI,
    COOLDOWN,
    COMPLETED,
    CANCELLED
}

/** One queued video. [storageUri] is the target file URI once created. */
@Serializable
data class BatchItemRecord(
    val url: String,
    val title: String,
    val position: Int,
    val serviceId: Int,
    val uploaderName: String? = null,
    val uploadDateText: String? = null,
    val status: BatchItemStatus = BatchItemStatus.PENDING,
    val retryCount: Int = 0,
    val failureReason: String? = null,
    val storageUri: String? = null,
    val storageTag: String? = null,
    val missionKind: String = "?",
    val override: PerItemOverride? = null,
    val lastProgress: Double = 0.0
)

/** One batch (single active batch at a time, replaced on new start). */
@Serializable
data class BatchRecord(
    val id: String,
    val playlistName: String,
    val playlistUrl: String,
    val serviceId: Int,
    val createdAt: Long = 0L,
    val status: BatchStatus = BatchStatus.RUNNING,
    val options: PlaylistDownloadOptions = PlaylistDownloadOptions(),
    val items: List<BatchItemRecord> = emptyList(),
    /** Items skipped for missing language under ASK_AT_END, awaiting the end dialog. */
    val askLanguageUrls: List<String> = emptyList()
)

/**
 * Persists the active batch as JSON in app-private storage
 * (`filesDir/playlist_batch/current_batch.json`), independent of newpipe.db.
 */
object BatchStore {
    private const val DIR_NAME = "playlist_batch"
    private const val FILE_NAME = "current_batch.json"

    private val json = Json {
        prettyPrint = true
        ignoreUnknownKeys = true
    }

    private fun storeFile(context: Context): File {
        val dir = File(context.filesDir, DIR_NAME)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        return File(dir, FILE_NAME)
    }

    /** Loads the persisted batch, or null when none exists. */
    fun load(context: Context): BatchRecord? {
        val file = storeFile(context)
        if (!file.exists()) {
            return null
        }
        return try {
            json.decodeFromString(BatchRecord.serializer(), file.readText())
        } catch (e: Exception) {
            null
        }
    }

    /** Atomically writes [record] (temp file + rename). */
    fun save(context: Context, record: BatchRecord) {
        val file = storeFile(context)
        val tmp = File(file.parent, "$FILE_NAME.tmp")
        tmp.writeText(json.encodeToString(BatchRecord.serializer(), record))
        if (!tmp.renameTo(file)) {
            file.delete()
            tmp.renameTo(file)
        }
    }

    /** Deletes the persisted batch, if any. */
    fun clear(context: Context) {
        storeFile(context).delete()
    }
}
