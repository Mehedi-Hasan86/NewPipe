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

/** Batch-level execution state (only meaningful while the batch is ACTIVE). */
@Serializable
enum class BatchStatus {
    RUNNING,
    PAUSED,
    WAITING_WIFI,
    COOLDOWN,
    COMPLETED,
    CANCELLED
}

/** Queue position of a batch. At most one batch is ACTIVE at a time. */
@Serializable
enum class BatchQueueState {
    ACTIVE,
    QUEUED,
    DONE
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

/** One batch. Each batch keeps the options captured at creation time. */
@Serializable
data class BatchRecord(
    val id: String,
    val playlistName: String,
    val playlistUrl: String,
    val serviceId: Int,
    val createdAt: Long = 0L,
    val status: BatchStatus = BatchStatus.RUNNING,
    val queueState: BatchQueueState = BatchQueueState.ACTIVE,
    /**
     * Internal playlist identity (`"<serviceId>:<playlistId>"`, blank when unknown).
     * Used for duplicate detection and subfolder reuse. Never shown in the UI.
     */
    val playlistKey: String = "",
    val options: PlaylistDownloadOptions = PlaylistDownloadOptions(),
    val items: List<BatchItemRecord> = emptyList(),
    /** Items skipped for missing language under ASK_AT_END, awaiting the end dialog. */
    val askLanguageUrls: List<String> = emptyList()
)

/** Versioned on-disk shape: a sequential FIFO queue of batches (schema version 2). */
@Serializable
data class BatchQueueFile(
    val version: Int = CURRENT_QUEUE_VERSION,
    val batches: List<BatchRecord> = emptyList(),
    /** Stable subfolder name per playlist key, so one playlist always reuses its folder. */
    val folderNames: Map<String, String> = emptyMap()
) {
    companion object {
        const val CURRENT_QUEUE_VERSION = 2
    }
}

/**
 * Persists the sequential FIFO queue of batches as JSON in app-private storage
 * (`filesDir/playlist_batch/batch_queue.json`), independent of newpipe.db.
 */
object BatchStore {
    private const val DIR_NAME = "playlist_batch"
    private const val FILE_NAME = "batch_queue.json"
    private const val LEGACY_FILE_NAME = "current_batch.json"

    private val json = Json {
        prettyPrint = true
        ignoreUnknownKeys = true
    }

    private fun storeDir(context: Context): File {
        val dir = File(context.filesDir, DIR_NAME)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        return dir
    }

    /**
     * Loads the persisted batch queue (schema version 2). A legacy version 1
     * `current_batch.json` (single batch) is migrated into the list as the first
     * batch on first read, keeping every item and its state: terminal batches
     * become DONE, anything else becomes ACTIVE. Batches without a playlist key
     * get one derived from their stored URL (blank when that fails).
     *
     * @param context the Android context
     * @return the persisted queue file (empty when nothing exists or it is corrupt)
     */
    fun loadQueue(context: Context): BatchQueueFile {
        val dir = storeDir(context)
        val file = File(dir, FILE_NAME)
        if (file.exists()) {
            return try {
                val stored = json.decodeFromString(
                    BatchQueueFile.serializer(),
                    file.readText()
                )
                stored.copy(batches = stored.batches.map { it.withDerivedKey() })
            } catch (e: Exception) {
                BatchQueueFile()
            }
        }
        val legacy = File(dir, LEGACY_FILE_NAME)
        if (!legacy.exists()) {
            return BatchQueueFile()
        }
        return try {
            val single = json.decodeFromString(BatchRecord.serializer(), legacy.readText())
            val queueState = if (single.status == BatchStatus.COMPLETED ||
                single.status == BatchStatus.CANCELLED
            ) {
                BatchQueueState.DONE
            } else {
                BatchQueueState.ACTIVE
            }
            val migrated = single.copy(queueState = queueState).withDerivedKey()
            BatchQueueFile(batches = listOf(migrated))
        } catch (e: Exception) {
            BatchQueueFile()
        }
    }

    /**
     * Atomically writes [queue] (temp file + rename). A leftover legacy version 1
     * file is deleted after a successful write so it is never migrated twice.
     *
     * @param context the Android context
     * @param queue the queue file to persist
     */
    fun saveQueue(context: Context, queue: BatchQueueFile) {
        val dir = storeDir(context)
        val file = File(dir, FILE_NAME)
        val tmp = File(dir, "$FILE_NAME.tmp")
        tmp.writeText(json.encodeToString(BatchQueueFile.serializer(), queue))
        if (!tmp.renameTo(file)) {
            file.delete()
            tmp.renameTo(file)
        }
        File(dir, LEGACY_FILE_NAME).delete()
    }

    /** Deletes both the version 2 queue file and any legacy version 1 file, if any. */
    fun clearQueue(context: Context) {
        val dir = storeDir(context)
        File(dir, FILE_NAME).delete()
        File(dir, LEGACY_FILE_NAME).delete()
    }

    private fun BatchRecord.withDerivedKey(): BatchRecord {
        if (playlistKey.isNotBlank()) {
            return this
        }
        return copy(playlistKey = PlaylistBatchUrlHelper.playlistKey(serviceId, playlistUrl))
    }
}
